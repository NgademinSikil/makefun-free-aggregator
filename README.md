# free-model-aggregator-02 — best live free-model aggregator

One server, two client wires, live free models behind every answer.
Entry for the make.fun bounty
[`free-model-aggregator-02`](https://make.fun/bounty/free-model-aggregator-02)
($500, closes Tue 17 Nov 2026 11:30 WITA).

## What it is

- `POST /v1/chat/completions` — OpenAI-compatible, JSON and streaming (SSE)
- `POST /v1/messages` — Anthropic-compatible, JSON and streaming
- `GET /v1/models` — OpenAI shape for `Authorization: Bearer`, Anthropic
  shape for `x-api-key`
- `node main.ts --list` — prints the catalog document, byte for byte the
  same document the server registers with the trial env
- Trial mode (`MAKEFUN_ENV_URL` set): listens on a free port, registers
  `{protocol_version: 1, port, catalog}`, polls `GET /work` every 100 ms,
  exits 0 when the env says done
- Standalone mode (no `MAKEFUN_ENV_URL`): serves on `$PORT` (or a free
  port) until SIGTERM/SIGINT — for local use

Routing, per prompt (deterministic — no wall clock, no randomness, only
the brief, the request order and the gateway's answers):

1. **Quality** — candidates meeting the prompt's requirements are ranked
   by the brief's `rating_milli` (highest first), ties by larger context
   length, then brief order. On the practice brief that means the
   1475-rated `openrouter/thinkingmachines/inkling-2-small:free` for plain
   and tool prompts, and the 1303-rated
   `openrouter/nvidia/nemotron-3.5-super-vl:free` for JSON-mode prompts.
2. **Failover** — a gateway 4xx/5xx or network error is retried on the
   next-best candidate (max 4 attempts per prompt, never past the trial's
   60-call cap). Refusals recovered from are reported in the
   `x-makefun-upstream-failures` response header (max 8 rows).
3. **Cooldowns** — a model just refused is skipped for the next prompt
   (counted in prompts, never seconds, so a replay reproduces it), then
   eligible again; if every candidate is cooling down, the best is tried
   anyway.

## The key — NOT included

No OpenRouter key is in this repository, and none is needed to build or
test it. The key is read from the environment only:

- **In a make.fun trial** the sandbox sets `OPENAI_BASE_URL` (the gateway)
  and `OPENAI_API_KEY` (a dummy the gateway swaps for the key pushed to
  the competition). The real key never enters the sandbox.
- **Outside a trial** set `OPENAI_BASE_URL` to a gateway (or a local mock
  gateway) with `OPENAI_API_KEY`, or set `OPENROUTER_API_KEY` to dial
  OpenRouter directly at `https://openrouter.ai/api/v1`.
- **With neither set** the server still starts and answers `/v1/models`
  and `--list`, but every completion is a graceful 502 naming the missing
  configuration. It never crashes and never invents an answer.

To submit, the key must come from an OpenRouter account with **at least
10 credits purchased** (free models still cost $0; the purchase raises the
free daily ceiling from 50 to 1,000 requests), pushed to the competition
with `makefun secrets push free-model-aggregator-02 --env-file .env`
— see the bounty page. **Do not commit a key: `.env` is git-ignored and
any committed key gets the entry refused.**

## Files

| File | What it is |
|---|---|
| `main.ts` | The server (Node 22, standard library only) |
| `lib/providers.ts` | Brief parsing, free-model list, catalog (from the kit starter) |
| `lib/wire.ts` | Anthropic ↔ Chat Completions translation, SSE writers (from the kit starter) |
| `LICENSE` | MIT (OSI-approved) |
| `test/mock-gateway.mjs` | Fake OpenRouter gateway: echoes each prompt's token, supports streams, can inject a one-time 429 or a permanent 401 |
| `test/mock-env.mjs` | Fake trial env: `/register` and `/work` |
| `test/run-local-test.mjs` | Local end-to-end tests, no key, no network (5 scenarios, 39 checks) |
| `free-model-aggregator-02-kit/` | The downloaded make.fun kit (reference only — **not** part of the submission repo) |
| `kit.tar.gz` | The downloaded kit archive (reference only — **not** part of the submission repo) |

The submission repository holds only `main.ts`, `lib/`, `LICENSE` (and
optionally this README): well under the 8 MiB single-file / 16 MiB total
caps. The kit directory and archive are git-ignored so they can never be
committed by accident.

## Local tests (no key, no network)

```sh
node test/run-local-test.mjs
```

Five scenarios, all against the mock gateway and mock env using practice
trial-0's brief and workload:

- **A happy path** — all 6 prompts served, tokens echoed, best model per
  prompt, `/v1/models` exact on both auth headers, `--list` byte-identical
  to the registered catalog, exactly 6 gateway calls, exit 0 on done.
- **B failover** — the best model 429s once: the prompt is still served by
  the next-best model, the 429 is reported in
  `x-makefun-upstream-failures`, the refused model is skipped for the next
  prompt and retried after its cooldown (7 gateway calls total).
- **C no key (401)** — every gateway call refused: every prompt answers a
  well-formed 502, the server stays up and exits 0.
- **D `--list`** — with no env at all, prints `{"providers":[]}` and exits 0.
- **E no gateway** — standalone server with no gateway configured answers a
  graceful 502 naming the missing configuration.

Result on 16 Nov 2026: **39 passed, 0 failed** (Node v24.22.1; the trial
runs Node 22 — the code uses only stable APIs available in Node 22 and
erasable TypeScript syntax only).

## Still missing for submission (NOT done here)

1. An OpenRouter account with ≥ 10 credits purchased + its API key, pushed
   to the competition (`makefun secrets push … --env-file .env`).
2. A public GitHub repository owned by the entrant's GitHub account,
   created after the competition opened, with this server at `main.ts` in
   the root — plus the agent session transcripts that built it.
3. A Solana wallet (keypair) linked to the make.fun account, for signing
   the entry and claiming any prize in USDC.
