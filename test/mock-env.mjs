// Mock trial env for local testing — mimics the two routes the aggregator uses:
//   POST /register  {protocol_version, port, catalog} -> {ok: true, requests: 6}
//   GET  /work      -> {done: false, ...} until POST /__finish, then {done: true}
// The register body is written to MOCK_ENV_REGISTER_FILE (if set) and served
// back at GET /__register so the test driver can compare it with --list.
//
// Usage: node test/mock-env.mjs   (prints MOCK_ENV_PORT=<n>)

import { createServer } from 'node:http';
import { writeFileSync } from 'node:fs';

const REGISTER_FILE = process.env.MOCK_ENV_REGISTER_FILE ?? '';
let registered = null;
let done = false;

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
  const send = (status, obj) => {
    const text = JSON.stringify(obj);
    res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(text) });
    res.end(text);
  };
  if (req.method === 'POST' && url.pathname === '/register') {
    const body = await readJson(req);
    if (body === null || typeof body !== 'object' || body.protocol_version !== 1 || typeof body.port !== 'number') {
      send(400, { error: 'bad_register', message: 'the body is not a valid register' });
      return;
    }
    registered = body;
    if (REGISTER_FILE !== '') {
      try {
        writeFileSync(REGISTER_FILE, `${JSON.stringify(body)}\n`);
      } catch {
        // Never break the mock on a logging failure.
      }
    }
    send(200, { ok: true, requests: 6 });
    return;
  }
  if (req.method === 'GET' && url.pathname === '/work') {
    send(200, done ? { done: true } : { done: false, next: null, pending: 6 });
    return;
  }
  if (req.method === 'POST' && url.pathname === '/__finish') {
    done = true;
    send(200, { ok: true });
    return;
  }
  if (req.method === 'GET' && url.pathname === '/__register') {
    if (registered === null) {
      send(404, { error: 'not_registered' });
      return;
    }
    send(200, registered);
    return;
  }
  send(404, { error: 'not_found' });
});

server.listen(0, '127.0.0.1', () => {
  const address = server.address();
  process.stdout.write(`MOCK_ENV_PORT=${address.port}\n`);
});
