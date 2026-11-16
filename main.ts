// free-model-aggregator-02 — one server, two client wires, live free models.
//
// What it does
//   Serves an OpenAI-compatible API (POST /v1/chat/completions) and an
//   Anthropic-compatible API (POST /v1/messages), in JSON and in streaming,
//   plus GET /v1/models in the shape of the auth header it got. Every
//   completion is a real call to a free OpenRouter model, dialled through a
//   gateway on Chat Completions. It registers with the trial env and exits 0
//   when the env says the trial is done.
//
// How it picks a model (the improvements over the kit's baseline)
//   1. Quality: candidates that meet the prompt's requirements are ranked by
//      the brief's published rating_milli (highest first), ties broken by
//      larger context length, then by brief order. Deterministic: no clock,
//      no randomness, only the brief, the requests and the gateway's answers.
//   2. Failover: a gateway 4xx/5xx or a network error is retried on the
//      next-best candidate that meets the requirements (at most 4 attempts
//      per prompt, and never past the trial's 60-call cap). Refusals that a
//      retry then served are reported in the `x-makefun-upstream-failures`
//      response header (at most 8 rows), so the failover column can see them.
//   3. Cooldowns: a model that was just refused is skipped for the next
//      prompt (counted in prompts, never in wall-clock seconds, so a replay
//      reproduces the same choices), then becomes eligible again. If every
//      candidate is cooling down, the best of them is tried anyway.
//
// The key
//   The OpenRouter key is NEVER in this repository. In a make.fun trial the
//   sandbox provides OPENAI_BASE_URL (the gateway) and OPENAI_API_KEY (a
//   dummy the gateway swaps for the key pushed to the competition). Outside
//   a trial, set OPENAI_BASE_URL to a gateway (or a local mock gateway) with
//   OPENAI_API_KEY, or set OPENROUTER_API_KEY to dial OpenRouter directly
//   at https://openrouter.ai/api/v1. With neither set the server still
//   starts and answers /v1/models and --list, but every completion is a
//   graceful 502 naming the missing configuration — it never crashes and
//   never invents an answer of its own.
//
// Usage
//   node main.ts --list     print the catalog document and exit 0
//   node main.ts            trial mode when MAKEFUN_ENV_URL is set
//                           (register + poll /work, exit 0 when done);
//                           standalone mode otherwise (PORT or a free port,
//                           serve until SIGTERM/SIGINT)
//   Node 22, standard library only; nothing is installed.

import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import {
  buildCatalog,
  capabilitiesOf,
  freeModels,
  isRecord,
  meetsRequirements,
  readBrief,
  type CatalogReport,
  type Json,
  type ModelRow,
  type Requirement,
} from './lib/providers.ts';
import {
  anthropicError,
  anthropicEventsFromOpenAI,
  anthropicModelList,
  anthropicToOpenAI,
  openAIError,
  openAIJsonWithModel,
  openAIModelList,
  openAIStreamWithModel,
  openAIToAnthropic,
  type ServedModel,
} from './lib/wire.ts';

/** The wire protocol's version, in the register body. */
const PROTOCOL_VERSION = 1;
/** How often GET /work is polled, in milliseconds. */
const POLL_INTERVAL_MS = 100;
/** The most bytes of one client request body this server reads. */
const MAX_BODY_BYTES = 8 * 1024 * 1024;
/** The most gateway attempts spent on one prompt (failover budget). */
const MAX_ATTEMPTS_PER_PROMPT = 4;
/** The trial gateway's per-trial call cap; never attempt past it. */
const TRIAL_CALL_CAP = 60;
/** The output cap given to a gateway call that names none. */
const DEFAULT_MAX_TOKENS = 1024;
/** The gateway refuses an output cap above this. */
const MAX_OUTPUT_TOKENS = 8192;
/** The most refusal rows carried in x-makefun-upstream-failures. */
const MAX_TRAIL_ROWS = 8;

const ENV_URL = (process.env.MAKEFUN_ENV_URL ?? '').replace(/\/+$/, '');
const PROMPT_FILE = process.env.MAKEFUN_PROMPT_FILE ?? '';
const GATEWAY_URL = (process.env.OPENAI_BASE_URL ?? '').replace(/\/+$/, '');
/** The trial's dummy key (the gateway swaps it for the pushed key), else a local key. */
const GATEWAY_KEY = process.env.OPENAI_API_KEY ?? process.env.OPENROUTER_API_KEY ?? '';
/** Direct OpenRouter endpoint, used only when OPENROUTER_API_KEY is set and no gateway is given. */
const DIRECT_OPENROUTER_URL = 'https://openrouter.ai/api/v1';

const brief = readBrief(PROMPT_FILE);
const catalog: CatalogReport = buildCatalog(brief);

/** Arrival order of the completion requests: the brief's prompt row for each. */
let promptCounter = 0;
/** Gateway calls made so far in this trial (the 60-call cap). */
let gatewayCalls = 0;
/** Per-model cooldown: the last prompt index during which the model is skipped. */
const coolUntil = new Map<string, number>();
/** Brief order, for the deterministic final tie-break. */
const briefIndex = new Map<string, number>(brief.models.map((m, i) => [m.id, i]));

function log(message: string): void {
  process.stderr.write(`aggregator: ${message}\n`);
}

/**
 * The gateway's base URL. The trial (or a local mock) gives OPENAI_BASE_URL;
 * otherwise a locally set OPENROUTER_API_KEY dials OpenRouter directly.
 * Empty means no gateway is configured: completions answer a graceful 502.
 */
function gatewayBase(): string {
  if (GATEWAY_URL !== '') return GATEWAY_URL;
  if (process.env.OPENROUTER_API_KEY !== undefined && process.env.OPENROUTER_API_KEY !== '') return DIRECT_OPENROUTER_URL;
  return '';
}

/** The gateway's Chat Completions route; '' when no gateway is configured. */
function chatCompletionsUrl(): string {
  const base = gatewayBase();
  if (base === '') return '';
  return base.endsWith('/v1') ? `${base}/chat/completions` : `${base}/v1/chat/completions`;
}

/** The free models the brief lists, as GET /v1/models names them. */
function servedModels(): ServedModel[] {
  return freeModels(brief).map((model) => ({ id: model.id, provider: model.provider, display_name: model.display_name }));
}

/** Rank candidates: rating desc, then context length desc, then brief order. */
function compareModels(a: ModelRow, b: ModelRow): number {
  if (b.rating_milli !== a.rating_milli) return b.rating_milli - a.rating_milli;
  const aCtx = a.context_length ?? -1;
  const bCtx = b.context_length ?? -1;
  if (bCtx !== aCtx) return bCtx - aCtx;
  return (briefIndex.get(a.id) ?? 0) - (briefIndex.get(b.id) ?? 0);
}

/**
 * Candidates for a prompt, best first. Models cooling down after a refusal
 * sort out; if every candidate is cooling down they are tried anyway,
 * best first, so a prompt is never lost for want of a fresh model.
 */
function rankedCandidates(requires: Requirement, promptIndex: number): ModelRow[] {
  const eligible = freeModels(brief)
    .filter((m) => meetsRequirements(requires, capabilitiesOf(m)))
    .sort(compareModels);
  const fresh = eligible.filter((m) => promptIndex > (coolUntil.get(m.id) ?? -1));
  return fresh.length > 0 ? fresh : eligible;
}

/** Mark a model cooling down: skipped for the next prompt, eligible after it. */
function markCool(modelId: string, promptIndex: number): void {
  coolUntil.set(modelId, promptIndex + 1);
}

/** One row of the x-makefun-upstream-failures trail. */
interface FailureRow {
  status: number;
  code: string;
  model: string | null;
}

function trailHeader(trail: FailureRow[]): Record<string, string> {
  if (trail.length === 0) return {};
  return { 'x-makefun-upstream-failures': JSON.stringify(trail.slice(0, MAX_TRAIL_ROWS)) };
}

/** A gateway error body's code/type, or a fallback when it is not JSON. */
function codeOf(text: string): string {
  try {
    const parsed: unknown = JSON.parse(text);
    if (isRecord(parsed) && isRecord(parsed.error)) {
      if (typeof parsed.error.code === 'string' && parsed.error.code !== '') return parsed.error.code.slice(0, 64);
      if (typeof parsed.error.type === 'string' && parsed.error.type !== '') return parsed.error.type.slice(0, 64);
    }
  } catch {
    // Not JSON: the fallback below.
  }
  return 'upstream_error';
}

/** The requirements of the prompt now arriving: the brief's row by arrival order, else the body's own. */
function requirementsFor(body: Json, promptIndex: number): Requirement {
  const entry = brief.prompts[promptIndex];
  if (entry !== undefined) return entry.requires;
  return requirementsFromBody(body);
}

/** Requirements read off a request body, when the brief has no row for it. */
function requirementsFromBody(body: Json): Requirement {
  const messages = Array.isArray(body.messages) ? body.messages : [];
  let vision = false;
  for (const message of messages) {
    if (!isRecord(message) || !Array.isArray(message.content)) continue;
    for (const block of message.content) {
      if (isRecord(block) && (block.type === 'image' || block.type === 'image_url')) vision = true;
    }
  }
  const format = isRecord(body.response_format) ? body.response_format.type : undefined;
  return { tools: Array.isArray(body.tools) && body.tools.length > 0, vision, json: format === 'json_object' || format === 'json_schema' };
}

/** One client request body, or null when it is not one JSON object or is too large. */
async function readJson(req: IncomingMessage): Promise<Json | null> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk));
    size += buffer.length;
    if (size > MAX_BODY_BYTES) return null;
    chunks.push(buffer);
  }
  if (chunks.length === 0) return null;
  try {
    const value: unknown = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    return isRecord(value) ? value : null;
  } catch {
    return null;
  }
}

function sendJson(res: ServerResponse, status: number, body: unknown, extraHeaders: Record<string, string> = {}): void {
  const text = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(text), ...extraHeaders });
  res.end(text);
}

function sendError(res: ServerResponse, wire: 'openai' | 'anthropic', status: number, type: string, message: string, extraHeaders: Record<string, string> = {}): void {
  if (wire === 'anthropic') sendJson(res, status, anthropicError(type, message), extraHeaders);
  else sendJson(res, status, openAIError(message, type, null), extraHeaders);
}

/** GET /v1/models, in the shape of the auth header the client sent. */
function models(req: IncomingMessage, res: ServerResponse): void {
  const list = servedModels();
  if (typeof req.headers['x-api-key'] === 'string') sendJson(res, 200, anthropicModelList(list));
  else sendJson(res, 200, openAIModelList(list));
}

/** The gateway refuses a call with no output cap, or one over 8192: guarantee a legal one. */
function ensureMaxTokens(body: Json): Json {
  const out: Json = { ...body };
  const maxTokens = out.max_tokens;
  const maxCompletion = out.max_completion_tokens;
  if (typeof maxTokens !== 'number' && typeof maxCompletion !== 'number') out.max_tokens = DEFAULT_MAX_TOKENS;
  if (typeof out.max_tokens === 'number') {
    if (out.max_tokens > MAX_OUTPUT_TOKENS) out.max_tokens = MAX_OUTPUT_TOKENS;
    if (out.max_tokens < 1) out.max_tokens = 1;
  }
  if (typeof out.max_completion_tokens === 'number') {
    if (out.max_completion_tokens > MAX_OUTPUT_TOKENS) out.max_completion_tokens = MAX_OUTPUT_TOKENS;
    if (out.max_completion_tokens < 1) out.max_completion_tokens = 1;
  }
  return out;
}

/** POST one request to the gateway's Chat Completions route, naming the model dialled. */
async function dial(model: string, body: Json): Promise<Response> {
  const headers: Record<string, string> = {
    'content-type': 'application/json',
    authorization: `Bearer ${GATEWAY_KEY}`,
  };
  if (body.stream === true) headers.accept = 'text/event-stream';
  return fetch(chatCompletionsUrl(), { method: 'POST', headers, body: JSON.stringify({ ...body, model }) });
}

interface DialOutcome {
  upstream: Response | null;
  model: string | null;
  trail: FailureRow[];
}

/**
 * Dial the gateway with failover: try the ranked candidates in order until
 * one answers, recording every refusal in the trail. Never retries the same
 * model for one prompt, never spends more than the per-prompt attempt budget,
 * and never attempts past the trial's call cap. Deterministic: the order and
 * the cooldowns are functions of the brief, the request order and the
 * gateway's answers only.
 */
async function dialWithFailover(
  buildBody: (model: string) => Json,
  requires: Requirement,
  promptIndex: number,
): Promise<DialOutcome> {
  const trail: FailureRow[] = [];
  if (chatCompletionsUrl() === '') {
    trail.push({ status: 502, code: 'no_gateway_configured', model: null });
    return { upstream: null, model: null, trail };
  }
  const candidates = rankedCandidates(requires, promptIndex);
  if (candidates.length === 0) return { upstream: null, model: null, trail };
  let attempts = 0;
  for (const candidate of candidates) {
    if (attempts >= MAX_ATTEMPTS_PER_PROMPT) break;
    if (gatewayCalls >= TRIAL_CALL_CAP) break;
    attempts += 1;
    gatewayCalls += 1;
    let upstream: Response;
    try {
      upstream = await dial(candidate.id, buildBody(candidate.id));
    } catch (err) {
      trail.push({ status: 502, code: 'upstream_unreachable', model: candidate.id });
      log(`prompt ${promptIndex}: ${candidate.id} unreachable (${(err as Error).message}); failing over`);
      markCool(candidate.id, promptIndex);
      continue;
    }
    if (upstream.ok && upstream.body !== null) return { upstream, model: candidate.id, trail };
    const text = await upstream.text().catch(() => '');
    trail.push({ status: upstream.status, code: codeOf(text), model: candidate.id });
    log(`prompt ${promptIndex}: ${candidate.id} refused HTTP ${upstream.status}; failing over`);
    markCool(candidate.id, promptIndex);
  }
  return { upstream: null, model: null, trail };
}

/** The 502 for a prompt no candidate served, naming the refusals behind it. */
function giveUp(res: ServerResponse, wire: 'openai' | 'anthropic', trail: FailureRow[], promptIndex: number): void {
  const detail =
    trail.length === 0
      ? 'no free model in the brief meets this request'
      : `the gateway refused every model tried (${trail.map((r) => `${r.model ?? 'gateway'}:${r.status}`).join(', ')})`;
  log(`prompt ${promptIndex}: giving up (${detail})`);
  if (wire === 'anthropic') {
    sendError(res, wire, 502, 'api_error', detail, trailHeader(trail));
  } else {
    sendError(res, wire, 502, 'upstream_error', detail, trailHeader(trail));
  }
}

/** POST /v1/chat/completions: an OpenAI-shaped client, answered by the best free model that serves it. */
async function chat(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const body = await readJson(req);
  if (body === null) return sendError(res, 'openai', 400, 'invalid_request_error', 'the request body is not one JSON object');
  const promptIndex = promptCounter;
  promptCounter += 1;
  const requires = requirementsFor(body, promptIndex);
  const { upstream, model, trail } = await dialWithFailover(
    (id) => ensureMaxTokens({ ...body, model: id }),
    requires,
    promptIndex,
  );
  if (upstream === null || model === null) return giveUp(res, 'openai', trail, promptIndex);
  const headers = trailHeader(trail);
  if (body.stream === true) {
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive', ...headers });
    try {
      for await (const frame of openAIStreamWithModel(upstream.body as ReadableStream<Uint8Array>, model)) res.write(frame);
    } finally {
      res.end();
    }
    return;
  }
  const answer: unknown = await upstream.json();
  sendJson(res, 200, openAIJsonWithModel(isRecord(answer) ? answer : {}, model), headers);
}

/** POST /v1/messages: an Anthropic-shaped client, rewritten to Chat Completions and answered back. */
async function messages(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const body = await readJson(req);
  if (body === null) return sendError(res, 'anthropic', 400, 'invalid_request_error', 'the request body is not one JSON object');
  const promptIndex = promptCounter;
  promptCounter += 1;
  const requires = requirementsFor(body, promptIndex);
  const { upstream, model, trail } = await dialWithFailover(
    (id) => ensureMaxTokens(anthropicToOpenAI(body, id)),
    requires,
    promptIndex,
  );
  if (upstream === null || model === null) return giveUp(res, 'anthropic', trail, promptIndex);
  const headers = trailHeader(trail);
  if (body.stream === true) {
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive', ...headers });
    try {
      for await (const event of anthropicEventsFromOpenAI(upstream.body as ReadableStream<Uint8Array>, model)) res.write(event);
    } finally {
      res.end();
    }
    return;
  }
  const answer: unknown = await upstream.json();
  sendJson(res, 200, openAIToAnthropic(isRecord(answer) ? answer : {}, model), headers);
}

async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const url = new URL(req.url ?? '/', 'http://entry');
  if (req.method === 'GET' && url.pathname === '/v1/models') {
    models(req, res);
    return;
  }
  if (req.method === 'POST' && url.pathname === '/v1/chat/completions') {
    await chat(req, res);
    return;
  }
  if (req.method === 'POST' && url.pathname === '/v1/messages') {
    await messages(req, res);
    return;
  }
  sendError(res, url.pathname === '/v1/messages' ? 'anthropic' : 'openai', 404, 'not_found', `no route ${req.method ?? ''} ${url.pathname}`);
}

function listen(server: Server, port: number): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '0.0.0.0', () => resolve());
  });
}

/** POST /register with the port and the one catalog; the env answers with the trial's prompt count. */
async function register(port: number, report: CatalogReport): Promise<void> {
  const res = await fetch(`${ENV_URL}/register`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ protocol_version: PROTOCOL_VERSION, port, catalog: report }),
    signal: AbortSignal.timeout(5000),
  });
  if (!res.ok) throw new Error(`POST ${ENV_URL}/register answered HTTP ${res.status}: ${(await res.text()).slice(0, 500)}`);
  const reply: unknown = await res.json();
  if (isRecord(reply) && typeof reply.requests === 'number') log(`registered: the trial holds ${reply.requests} prompts`);
}

/** Poll GET /work every 100 ms; exit 0 the moment it says done. */
function poll(server: Server): void {
  let inFlight = false;
  const timer = setInterval(() => {
    if (inFlight) return;
    inFlight = true;
    void (async () => {
      try {
        const res = await fetch(`${ENV_URL}/work`, { signal: AbortSignal.timeout(2000) });
        if (res.status !== 200) return;
        const work: unknown = await res.json();
        if (isRecord(work) && work.done === true) {
          clearInterval(timer);
          server.close();
          process.exit(0);
        }
      } catch {
        // The env is not reachable yet: try again on the next tick.
      } finally {
        inFlight = false;
      }
    })();
  }, POLL_INTERVAL_MS);
}

function makeServer(): Server {
  return createServer((req, res) => {
    void handle(req, res).catch((err: unknown) => {
      log((err as Error).stack ?? String(err));
      if (!res.headersSent) sendError(res, req.url?.startsWith('/v1/messages') === true ? 'anthropic' : 'openai', 500, 'internal_error', 'the server failed to answer');
    });
  });
}

async function start(): Promise<void> {
  if (brief.error !== null) log(brief.error);
  if (process.argv.includes('--list')) {
    // The same document the register body carries, byte for byte: print the one built object and stop
    // before any server starts. A process whose stdout is not flushed loses the tail, so wait for it.
    const text = `${JSON.stringify(catalog)}\n`;
    await new Promise<void>((resolve) => process.stdout.write(text, () => resolve()));
    process.exit(0);
  }
  const server = makeServer();
  const port = ENV_URL === '' ? Number(process.env.PORT ?? 0) || 0 : 0;
  await listen(server, port);
  const address = server.address();
  if (address === null || typeof address === 'string') throw new Error('the server has no port');
  const bound = address.port;
  // 8080 is the env's on one layout; the OS's ephemeral range never holds it, and the guard says so.
  if (bound === 8080) throw new Error('the OS picked 8080, which the env owns on one layout: restart the trial');
  log(`listening on 0.0.0.0:${bound} with ${servedModels().length} free models`);
  if (chatCompletionsUrl() === '') {
    log('no model gateway configured: set OPENAI_BASE_URL (trial or mock gateway) or OPENROUTER_API_KEY; completions will answer 502 until one is set');
  }
  if (ENV_URL === '') {
    // Standalone mode (local use): serve until SIGTERM/SIGINT, no register, no poll.
    process.on('SIGTERM', () => process.exit(0));
    process.on('SIGINT', () => process.exit(0));
    return;
  }
  await register(bound, catalog);
  process.on('SIGTERM', () => process.exit(0));
  process.on('SIGINT', () => process.exit(0));
  poll(server);
}

try {
  await start();
} catch (err) {
  log((err as Error).stack ?? String(err));
  process.exit(1);
}
