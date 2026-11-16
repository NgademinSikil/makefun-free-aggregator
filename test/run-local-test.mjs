// Local end-to-end tests for the aggregator — mock gateway + mock env, NO key.
//
// Scenarios (each starts fresh mock services and a fresh server):
//   A happy path:   all 6 practice prompts served, tokens echoed, best model
//                   per prompt, /v1/models exact on both auth headers, --list
//                   byte-identical to the registered catalog, exit 0 on done.
//   B failover:     the best model 429s once; the prompt is still served by
//                   the next-best model, the 429 is reported in the
//                   x-makefun-upstream-failures header, and the refused model
//                   is skipped for the next prompt (cooldown) then retried.
//   C no key (401): every gateway call is refused 401; every prompt answers
//                   a well-formed 502, the server stays up and exits 0.
//   D --list:       with no env at all, prints {"providers":[]} and exits 0.
//   E no gateway:   standalone server with no gateway configured answers a
//                   graceful 502 naming the missing configuration.
//
// Usage: node test/run-local-test.mjs   (exit 0 = all checks pass)

import { spawn } from 'node:child_process';
import { mkdtempSync, readFileSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
const KIT = join(ROOT, 'free-model-aggregator-02-kit');
const PROMPT_FILE = join(KIT, 'practice/trial-0/input/prompt.txt');
const WORKLOAD = JSON.parse(readFileSync(join(KIT, 'practice/trial-0/input/env/workload.json'), 'utf8'));
const RATINGS = JSON.parse(readFileSync(join(KIT, 'practice/trial-0/reference/ratings.json'), 'utf8'));

const EXPECTED_MODELS = new Set(RATINGS.ratings.map((r) => r.model));
// Best model per requirement class, from the practice ratings table.
const BEST_PLAIN = 'openrouter/thinkingmachines/inkling-2-small:free'; // 1475
const BEST_JSON = 'openrouter/nvidia/nemotron-3.5-super-vl:free'; // 1303, json:true
const EXPECTED_MODEL = { p01: BEST_JSON, p02: BEST_PLAIN, p03: BEST_PLAIN, p04: BEST_JSON, p05: BEST_PLAIN, p06: BEST_PLAIN };

let passed = 0;
let failed = 0;
const failures = [];
function check(name, ok, detail = '') {
  if (ok) {
    passed += 1;
    process.stdout.write(`  PASS ${name}\n`);
  } else {
    failed += 1;
    failures.push(name);
    process.stdout.write(`  FAIL ${name}${detail === '' ? '' : ` — ${detail}`}\n`);
  }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitFor(label, fn, timeoutMs = 10000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    let value = null;
    try {
      value = await fn();
    } catch {
      value = null;
    }
    if (value !== null && value !== undefined && value !== false) return value;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
    await sleep(100);
  }
}
function fetchJson(url, options = {}) {
  return fetch(url, { ...options, signal: AbortSignal.timeout(5000) });
}

/** Spawn a node script; resolves with {proc, port} once it prints PORT=<n> on stdout. */
function spawnService(script, env, readyKey) {
  const proc = spawn(process.execPath, [script], { env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
  let stdout = '';
  let stderr = '';
  proc.stdout.on('data', (d) => {
    stdout += d;
  });
  proc.stderr.on('data', (d) => {
    stderr += d;
  });
  return {
    proc,
    ready: waitFor(`${script} ready`, () => {
      const m = stdout.match(new RegExp(`${readyKey}=(\\d+)`));
      return m === null ? null : Number(m[1]);
    }),
    stderr: () => stderr,
  };
}

function spawnServer(env) {
  const proc = spawn(process.execPath, [join(ROOT, 'main.ts')], {
    env: { ...process.env, ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stderr = '';
  proc.stderr.on('data', (d) => {
    stderr += d;
  });
  return {
    proc,
    port: waitFor('server listening', () => {
      const m = stderr.match(/listening on 0\.0\.0\.0:(\d+)/);
      return m === null ? null : Number(m[1]);
    }),
    stderr: () => stderr,
  };
}

function waitExit(proc, timeoutMs = 15000) {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(null), timeoutMs);
    proc.once('exit', (code) => {
      clearTimeout(timer);
      resolve(code);
    });
  });
}

function stop(proc) {
  if (proc.exitCode === null) proc.kill('SIGTERM');
}

/** Send one workload prompt to the server; returns {text, model, status, trail}. */
async function sendPrompt(serverPort, id) {
  const manifest = WORKLOAD.manifest.find((p) => p.id === id);
  const body = WORKLOAD.bodies[id];
  if (manifest.api === 'openai') {
    const res = await fetchJson(`http://127.0.0.1:${serverPort}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', authorization: 'Bearer test' },
      body: JSON.stringify(body),
    });
    const trail = res.headers.get('x-makefun-upstream-failures');
    if (res.status !== 200) return { status: res.status, text: await res.text(), model: null, trail };
    if (body.stream === true) {
      const sse = await res.text();
      let text = '';
      let model = null;
      for (const line of sse.split('\n')) {
        if (!line.startsWith('data:')) continue;
        const payload = line.slice(5).trim();
        if (payload === '[DONE]' || payload === '') continue;
        const chunk = JSON.parse(payload);
        model = chunk.model ?? model;
        const delta = chunk.choices?.[0]?.delta?.content;
        if (typeof delta === 'string') text += delta;
      }
      return { status: 200, text, model, trail };
    }
    const json = await res.json();
    return { status: 200, text: json.choices?.[0]?.message?.content ?? '', model: json.model ?? null, trail };
  }
  const res = await fetchJson(`http://127.0.0.1:${serverPort}/v1/messages`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-api-key': 'test', 'anthropic-version': '2023-06-01' },
    body: JSON.stringify(body),
  });
  const trail = res.headers.get('x-makefun-upstream-failures');
  if (res.status !== 200) return { status: res.status, text: await res.text(), model: null, trail };
  if (body.stream === true) {
    const sse = await res.text();
    let text = '';
    let model = null;
    let sawStart = false;
    let sawStop = false;
    for (const line of sse.split('\n')) {
      if (!line.startsWith('data:')) continue;
      const payload = line.slice(5).trim();
      if (payload === '') continue;
      const event = JSON.parse(payload);
      if (event.type === 'message_start') {
        sawStart = true;
        model = event.message?.model ?? model;
      }
      if (event.type === 'message_stop') sawStop = true;
      if (event.type === 'content_block_delta' && event.delta?.type === 'text_delta') text += event.delta.text;
    }
    return { status: 200, text, model, trail, sawStart, sawStop };
  }
  const json = await res.json();
  const text = (json.content ?? []).filter((b) => b.type === 'text').map((b) => b.text).join('');
  return { status: 200, text, model: json.model ?? null, trail };
}

async function scenarioHappy(tmp) {
  process.stdout.write('Scenario A — happy path (mock gateway, no key)\n');
  const gwLog = join(tmp, 'gw-a.jsonl');
  const regFile = join(tmp, 'register-a.json');
  const gw = spawnService(join(ROOT, 'test/mock-gateway.mjs'), { MOCK_GATEWAY_LOG: gwLog }, 'MOCK_GATEWAY_PORT');
  const env = spawnService(join(ROOT, 'test/mock-env.mjs'), { MOCK_ENV_REGISTER_FILE: regFile }, 'MOCK_ENV_PORT');
  const gwPort = await gw.ready;
  const envPort = await env.ready;
  const server = spawnServer({
    MAKEFUN_PROMPT_FILE: PROMPT_FILE,
    MAKEFUN_ENV_URL: `http://127.0.0.1:${envPort}`,
    OPENAI_BASE_URL: `http://127.0.0.1:${gwPort}/v1`,
    OPENAI_API_KEY: 'mock-dummy-key',
  });
  const port = await server.port;
  try {
    await waitFor('register', async () => {
      try {
        return JSON.parse(readFileSync(regFile, 'utf8'));
      } catch {
        return null;
      }
    });
    const registered = JSON.parse(readFileSync(regFile, 'utf8'));
    check('A register: protocol_version 1', registered.protocol_version === 1);
    check('A register: catalog holds exactly the 14 free ids', (() => {
      const ids = (registered.catalog.providers ?? []).flatMap((p) => (p.models ?? []).map((m) => m.id));
      return ids.length === 14 && new Set(ids).size === 14 && ids.every((id) => EXPECTED_MODELS.has(id));
    })());

    // --list prints the same catalog document, byte for byte.
    const listProc = spawn(process.execPath, [join(ROOT, 'main.ts'), '--list'], {
      env: { ...process.env, MAKEFUN_PROMPT_FILE: PROMPT_FILE },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let listOut = '';
    listProc.stdout.on('data', (d) => {
      listOut += d;
    });
    const listCode = await waitExit(listProc);
    check('A --list exits 0', listCode === 0, `exit=${listCode}`);
    check('A --list equals registered catalog byte for byte', listOut === `${JSON.stringify(registered.catalog)}\n`);

    // GET /v1/models on both auth headers.
    const openaiList = await (await fetchJson(`http://127.0.0.1:${port}/v1/models`, { headers: { authorization: 'Bearer test' } })).json();
    const anthropicList = await (await fetchJson(`http://127.0.0.1:${port}/v1/models`, { headers: { 'x-api-key': 'test' } })).json();
    const openaiIds = (openaiList.data ?? []).map((m) => m.id);
    const anthropicIds = (anthropicList.data ?? []).map((m) => m.id);
    check('A GET /v1/models (OpenAI) lists exactly the 14 ids', openaiIds.length === 14 && new Set(openaiIds).size === 14 && openaiIds.every((id) => EXPECTED_MODELS.has(id)));
    check('A GET /v1/models (Anthropic) lists exactly the 14 ids', anthropicIds.length === 14 && new Set(anthropicIds).size === 14 && anthropicIds.every((id) => EXPECTED_MODELS.has(id)));

    // The six prompts, in manifest order.
    for (const prompt of WORKLOAD.manifest) {
      const answer = await sendPrompt(port, prompt.id);
      const token = prompt.token;
      check(`A ${prompt.id} served (HTTP 200, token ${token} in answer)`, answer.status === 200 && answer.text.includes(token), `status=${answer.status} text=${JSON.stringify(answer.text.slice(0, 80))}`);
      check(`A ${prompt.id} model is the best for its requirements`, answer.model === EXPECTED_MODEL[prompt.id], `model=${answer.model}`);
      if (prompt.stream) {
        check(`A ${prompt.id} streaming events well-formed`, prompt.api === 'openai' ? answer.text !== '' : answer.sawStart === true && answer.sawStop === true);
      }
    }
    const calls = readFileSync(gwLog, 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l));
    check('A gateway saw exactly 6 calls (no wasted retries)', calls.length === 6, `calls=${calls.length}`);

    await fetchJson(`http://127.0.0.1:${envPort}/__finish`, { method: 'POST' });
    const code = await waitExit(server.proc);
    check('A server exits 0 when the env says done', code === 0, `exit=${code}`);
  } finally {
    stop(server.proc);
    stop(gw.proc);
    stop(env.proc);
  }
}

async function scenarioFailover(tmp) {
  process.stdout.write('Scenario B — failover + cooldown (best model 429s once)\n');
  const gwLog = join(tmp, 'gw-b.jsonl');
  const regFile = join(tmp, 'register-b.json');
  const gw = spawnService(join(ROOT, 'test/mock-gateway.mjs'), { MOCK_GATEWAY_LOG: gwLog, MOCK_FAIL_ONCE_MODEL: BEST_PLAIN }, 'MOCK_GATEWAY_PORT');
  const env = spawnService(join(ROOT, 'test/mock-env.mjs'), { MOCK_ENV_REGISTER_FILE: regFile }, 'MOCK_ENV_PORT');
  const gwPort = await gw.ready;
  const envPort = await env.ready;
  const server = spawnServer({
    MAKEFUN_PROMPT_FILE: PROMPT_FILE,
    MAKEFUN_ENV_URL: `http://127.0.0.1:${envPort}`,
    OPENAI_BASE_URL: `http://127.0.0.1:${gwPort}/v1`,
    OPENAI_API_KEY: 'mock-dummy-key',
  });
  const port = await server.port;
  try {
    await waitFor('register', async () => {
      try {
        return JSON.parse(readFileSync(regFile, 'utf8'));
      } catch {
        return null;
      }
    });
    const results = {};
    for (const prompt of WORKLOAD.manifest) results[prompt.id] = await sendPrompt(port, prompt.id);
    check('B p01 (JSON) served by best JSON model', results.p01.status === 200 && results.p01.model === BEST_JSON && results.p01.text.includes('SPARROW-7435'));
    check('B p02 still served after its model 429s, by the next-best model', results.p02.status === 200 && results.p02.model === BEST_JSON && results.p02.text.includes('COMET-3884'), `model=${results.p02.model} status=${results.p02.status}`);
    const trail = results.p02.trail === null ? null : JSON.parse(results.p02.trail);
    check('B p02 reports the 429 in x-makefun-upstream-failures', Array.isArray(trail) && trail.length === 1 && trail[0].status === 429 && trail[0].model === BEST_PLAIN, `trail=${results.p02.trail}`);
    check('B p03 skips the cooling-down model (next prompt)', results.p03.status === 200 && results.p03.model === BEST_JSON, `model=${results.p03.model}`);
    check('B p05 retries the recovered model after its cooldown', results.p05.status === 200 && results.p05.model === BEST_PLAIN, `model=${results.p05.model}`);
    check('B p06 served by the best model', results.p06.status === 200 && results.p06.model === BEST_PLAIN);
    const calls = readFileSync(gwLog, 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l));
    check('B gateway saw 7 calls (6 + 1 failed retry)', calls.length === 7, `calls=${calls.length}`);
    check('B exactly one call was refused (the injected 429)', calls.filter((c) => c.status !== 200).length === 1);
    await fetchJson(`http://127.0.0.1:${envPort}/__finish`, { method: 'POST' });
    const code = await waitExit(server.proc);
    check('B server exits 0 when the env says done', code === 0, `exit=${code}`);
  } finally {
    stop(server.proc);
    stop(gw.proc);
    stop(env.proc);
  }
}

async function scenarioNoKey(tmp) {
  process.stdout.write('Scenario C — no key: gateway answers 401 to every call\n');
  const gwLog = join(tmp, 'gw-c.jsonl');
  const gw = spawnService(join(ROOT, 'test/mock-gateway.mjs'), { MOCK_GATEWAY_LOG: gwLog, MOCK_ALWAYS_401: '1' }, 'MOCK_GATEWAY_PORT');
  const env = spawnService(join(ROOT, 'test/mock-env.mjs'), {}, 'MOCK_ENV_PORT');
  const gwPort = await gw.ready;
  const envPort = await env.ready;
  const server = spawnServer({
    MAKEFUN_PROMPT_FILE: PROMPT_FILE,
    MAKEFUN_ENV_URL: `http://127.0.0.1:${envPort}`,
    OPENAI_BASE_URL: `http://127.0.0.1:${gwPort}/v1`,
    OPENAI_API_KEY: 'mock-dummy-key',
  });
  const port = await server.port;
  try {
    await waitFor('register', async () => {
      try {
        const res = await fetchJson(`http://127.0.0.1:${envPort}/__register`);
        return res.status === 200 ? await res.json() : null;
      } catch {
        return null;
      }
    });
    let all502 = true;
    let wellFormed = true;
    for (const prompt of WORKLOAD.manifest) {
      const answer = await sendPrompt(port, prompt.id);
      if (answer.status !== 502) all502 = false;
      try {
        const parsed = JSON.parse(answer.text);
        if (prompt.api === 'openai' && !parsed.error) wellFormed = false;
        if (prompt.api === 'anthropic' && parsed.type !== 'error') wellFormed = false;
      } catch {
        wellFormed = false;
      }
      if (answer.trail !== null) {
        const trail = JSON.parse(answer.trail);
        if (!Array.isArray(trail) || trail.some((r) => r.status !== 401)) wellFormed = false;
      }
    }
    check('C every prompt answers a graceful 502 (never a crash, never a fake answer)', all502);
    check('C 502 bodies are well-formed errors in each wire, with the 401 trail', wellFormed);
    const modelsRes = await fetchJson(`http://127.0.0.1:${port}/v1/models`, { headers: { authorization: 'Bearer test' } });
    check('C server stays up: GET /v1/models still 200', modelsRes.status === 200);
    await fetchJson(`http://127.0.0.1:${envPort}/__finish`, { method: 'POST' });
    const code = await waitExit(server.proc);
    check('C server exits 0 when the env says done', code === 0, `exit=${code}`);
  } finally {
    stop(server.proc);
    stop(gw.proc);
    stop(env.proc);
  }
}

async function scenarioList() {
  process.stdout.write('Scenario D — --list with no env at all\n');
  const proc = spawn(process.execPath, [join(ROOT, 'main.ts'), '--list'], {
    env: { PATH: process.env.PATH },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let out = '';
  proc.stdout.on('data', (d) => {
    out += d;
  });
  const code = await waitExit(proc);
  check('D --list exits 0 with an empty catalog and no env', code === 0 && out === '{"providers":[]}\n', `exit=${code} out=${JSON.stringify(out)}`);
}

async function scenarioNoGateway() {
  process.stdout.write('Scenario E — standalone server with no gateway configured\n');
  const server = spawnServer({ PORT: '0' });
  const port = await server.port;
  try {
    const res = await fetchJson(`http://127.0.0.1:${port}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', authorization: 'Bearer test' },
      body: JSON.stringify({ model: 'auto', messages: [{ role: 'user', content: 'Reply with exactly the word TEST-0001 and nothing else.' }], max_tokens: 100 }),
    });
    const text = await res.text();
    check('E completion answers a graceful 502 naming the missing gateway', res.status === 502 && text.includes('gateway'), `status=${res.status} body=${text.slice(0, 120)}`);
    const modelsRes = await fetchJson(`http://127.0.0.1:${port}/v1/models`, { headers: { authorization: 'Bearer test' } });
    check('E GET /v1/models still answers (empty catalog, no brief)', modelsRes.status === 200);
    server.proc.kill('SIGTERM');
    const code = await waitExit(server.proc);
    check('E server exits 0 on SIGTERM', code === 0, `exit=${code}`);
  } finally {
    stop(server.proc);
  }
}

const tmp = mkdtempSync(join(tmpdir(), 'aggregator-test-'));
try {
  await scenarioHappy(tmp);
  await scenarioFailover(tmp);
  await scenarioNoKey(tmp);
  await scenarioList();
  await scenarioNoGateway();
} catch (err) {
  failed += 1;
  failures.push(`harness error: ${err.message}`);
  process.stdout.write(`  FAIL harness error: ${err.stack ?? err.message}\n`);
} finally {
  rmSync(tmp, { recursive: true, force: true });
}
process.stdout.write(`\n${passed} passed, ${failed} failed${failed > 0 ? ` — failed: ${failures.join('; ')}` : ''}\n`);
process.exit(failed === 0 ? 0 : 1);
