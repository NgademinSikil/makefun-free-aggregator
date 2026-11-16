// Mock OpenRouter gateway for local testing — NO real key, NO network.
//
// Mimics the make.fun trial gateway's Chat Completions route closely enough
// to test the aggregator end to end:
//   - reads the prompt's unique token (e.g. ZEBRA-4721) out of the messages
//     and answers with it, as a real free model instructed to echo it would;
//   - JSON mode (response_format json_object) answers {"token": "<token>"};
//   - stream=true answers SSE chunks with a final usage chunk, then [DONE];
//   - the `model` in answers spells the id WITHOUT the `openrouter/` prefix,
//     as the real provider does (the aggregator puts its dialled id back).
// Failure injection (env):
//   MOCK_FAIL_ONCE_MODEL=<id>  the first call to that model answers 429 once
//   MOCK_ALWAYS_401=1          every call answers 401 (no-key behaviour)
// Every call is appended as one JSON line to MOCK_GATEWAY_LOG (if set):
//   {"model": "<id>", "stream": true|false, "status": <n>}
//
// Usage: node test/mock-gateway.mjs   (prints MOCK_GATEWAY_PORT=<n>)

import { createServer } from 'node:http';
import { appendFileSync } from 'node:fs';

const FAIL_ONCE_MODEL = process.env.MOCK_FAIL_ONCE_MODEL ?? '';
const ALWAYS_401 = process.env.MOCK_ALWAYS_401 === '1';
const LOG_FILE = process.env.MOCK_GATEWAY_LOG ?? '';
const failedOnce = new Set();

function logCall(model, stream, status) {
  if (LOG_FILE === '') return;
  try {
    appendFileSync(LOG_FILE, `${JSON.stringify({ model, stream: stream === true, status })}\n`);
  } catch {
    // Logging must never break the mock.
  }
}

function textOfMessages(messages) {
  const parts = [];
  for (const m of Array.isArray(messages) ? messages : []) {
    if (typeof m?.content === 'string') parts.push(m.content);
    else if (Array.isArray(m?.content)) {
      for (const p of m.content) if (p?.type === 'text' && typeof p.text === 'string') parts.push(p.text);
    }
  }
  return parts.join('\n');
}

function stripPrefix(id) {
  return id.startsWith('openrouter/') ? id.slice('openrouter/'.length) : id;
}

function readJson(req) {
  return new Promise((resolve) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch {
        resolve(null);
      }
    });
  });
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url ?? '/', 'http://mock');
  if (req.method === 'GET' && url.pathname === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end('{"ok":true}');
    return;
  }
  if (req.method !== 'POST' || url.pathname !== '/v1/chat/completions') {
    res.writeHead(404, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: { message: 'not found', type: 'not_found', code: 404 } }));
    return;
  }
  const body = await readJson(req);
  if (body === null || typeof body !== 'object') {
    res.writeHead(400, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: { message: 'invalid_json', type: 'invalid_request_error', code: 400 } }));
    return;
  }
  const model = typeof body.model === 'string' ? body.model : '';
  const stream = body.stream === true;

  if (ALWAYS_401) {
    logCall(model, stream, 401);
    res.writeHead(401, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: { message: 'No auth credentials found', type: 'auth_error', code: 401 } }));
    return;
  }
  if (FAIL_ONCE_MODEL !== '' && model === FAIL_ONCE_MODEL && !failedOnce.has(model)) {
    failedOnce.add(model);
    logCall(model, stream, 429);
    res.writeHead(429, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: { message: 'Rate limit exceeded', type: 'rate_limit_error', code: 429 } }));
    return;
  }
  if (typeof body.max_tokens !== 'number' && typeof body.max_completion_tokens !== 'number') {
    logCall(model, stream, 400);
    res.writeHead(400, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: { message: 'max_tokens_required', type: 'invalid_request_error', code: 400 } }));
    return;
  }

  const tokenMatch = textOfMessages(body.messages).match(/[A-Z]{3,}-\d{4}/);
  const token = tokenMatch !== null ? tokenMatch[0] : 'MOCK-0000';
  const jsonMode = body.response_format?.type === 'json_object';
  const content = jsonMode ? JSON.stringify({ token }) : token;
  const upstreamModel = stripPrefix(model);
  const usage = { prompt_tokens: 42, completion_tokens: content.length, total_tokens: 42 + content.length };

  if (!stream) {
    logCall(model, false, 200);
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(
      JSON.stringify({
        id: 'chatcmpl-mock',
        object: 'chat.completion',
        created: 1767225600,
        model: upstreamModel,
        choices: [{ index: 0, message: { role: 'assistant', content }, finish_reason: 'stop' }],
        usage,
      }),
    );
    return;
  }

  logCall(model, true, 200);
  res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive' });
  const half = Math.ceil(content.length / 2);
  const chunk = (delta, finish) =>
    `data: ${JSON.stringify({ id: 'chatcmpl-mock', object: 'chat.completion.chunk', created: 1767225600, model: upstreamModel, choices: [{ index: 0, delta, finish_reason: finish }] })}\n\n`;
  res.write(chunk({ role: 'assistant', content: content.slice(0, half) }, null));
  res.write(chunk({ content: content.slice(half) }, null));
  res.write(
    `data: ${JSON.stringify({ id: 'chatcmpl-mock', object: 'chat.completion.chunk', created: 1767225600, model: upstreamModel, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }], usage })}\n\n`,
  );
  res.write('data: [DONE]\n\n');
  res.end();
});

server.listen(0, '127.0.0.1', () => {
  const address = server.address();
  process.stdout.write(`MOCK_GATEWAY_PORT=${address.port}\n`);
});
