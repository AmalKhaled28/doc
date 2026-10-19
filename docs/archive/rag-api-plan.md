# RAG Quality and API Implementation Plan

## Goal

Turn the current single-question CLI agent into a measurable, reusable service
without weakening grounding or prematurely replacing exact cosine retrieval.
The first API version must support Arabic, English, and French questions, expose
the evidence already returned by `RagResult`, and remain bounded to three queries
per retrieval round and two retrieval rounds.

Work is split into four ordered milestones. Evaluation comes first so retrieval
and API changes can be compared against a recorded baseline.

## Milestone 1: Retrieval evaluation baseline

Add a small versioned evaluation dataset and a command that runs retrieval without
calling Groq.

- Store JSONL cases under `evaluation/`, with question, language, expected stable
  chunk IDs, and optional notes. Start with at least 20 questions per supported
  language, including simple, multipart, colloquial, unanswerable, and
  cross-language-query cases.
- Add `scripts/evaluate_retrieval.py`. It must use the production query planner
  only when explicitly requested; its deterministic mode accepts the case's
  search queries so retrieval changes can be measured independently from LLM
  variability.
- Report Recall@1, Recall@5, MRR, irrelevant-result rate, per-language results,
  query embedding time, database search time, and total retrieval time.
- Save timestamped JSON reports outside the committed fixture directory. Do not
  tune against anecdotal questions alone.
- Record the current exact-cosine implementation as the baseline before changing
  filtering or ranking.

Acceptance criteria:

- The evaluator produces identical rankings for repeated runs against an unchanged
  database and embedding version.
- Metrics are broken down by language and include the exact chunking, embedding,
  and retrieval configuration used for the run.
- CI unit tests use a tiny synthetic fixture; evaluation against PostgreSQL remains
  an explicit integration command.

## Milestone 2: Evidence-quality controls

Improve what reaches Qwen while keeping exact cosine as the only ranking signal.

- Add a configurable `MIN_COSINE_SIMILARITY`, disabled by default until the
  evaluation dataset establishes a threshold. Apply it after cosine search and
  before adjacent merging. Never silently substitute a guessed production value.
- Keep per-query result groups until coverage is checked. Deduplicate physical
  chunks, merge only independently retrieved adjacent chunks, remove their exact
  overlap, and retain all query/source provenance as today.
- Add ingestion or pre-embedding rejection rules for known error pages and obvious
  boilerplate. Evaluate each rule against false removals before rematerializing
  chunks and embeddings under a new version.
- Add context limits by total characters and evidence-block count. Select blocks
  round-robin across subqueries before filling remaining capacity by similarity,
  so one easy subquestion cannot consume all context.
- Preserve the grounded-finalizer behavior: a directly supported partial answer
  is returned with an explicit limitation; `insufficient` is reserved for no
  directly responsive evidence.

Acceptance criteria:

- No regression in Recall@5 for any supported language relative to Milestone 1.
- The irrelevant-result rate improves on the labeled set.
- Existing citation validation, query limits, retry limits, deduplication, and
  adjacent-merge tests remain green.

## Milestone 3: Service-ready agent lifecycle

Refactor resource ownership before adding HTTP endpoints.

- Create one `RagAgent` during application startup. Do not call the convenience
  `answer()` function per request because it currently constructs a new Groq
  client and resolves the embedding fingerprint each time.
- Resolve and validate the chunking/embedding configuration once at startup.
  Fail readiness if the model files are absent or no matching vectors exist.
- Keep one loaded ONNX tokenizer/session per process and serialize embedding
  inference with a lock. Continue embedding all queries for one round as a single
  batch.
- Replace per-call database connections and executors with a shared bounded search
  executor and `psycopg2.pool.ThreadedConnectionPool`. Each concurrent search must
  still own a separate checked-out connection and cursor.
- Add stage timings for planning, embedding, database retrieval, evidence
  assembly, corrective retrieval, and final answering.
- Add typed service exceptions that distinguish invalid input, unavailable model
  assets, database failure, Groq failure, timeout, and genuine insufficient
  evidence. Never include credentials, prompts, or full evidence text in logs.

Initial operational defaults:

```text
maximum question length:          4,000 characters
maximum concurrent requests:      2
maximum queries per round:        3
maximum retrieval rounds:         2
maximum concurrent DB searches:   3 per request
database pool size:               6
embedding inference concurrency:  1
database statement timeout:       15 seconds
whole-request timeout:            90 seconds
```

All limits are server-controlled environment settings with positive-integer
validation. A request cannot override the model, vector version, concurrency,
timeouts, or database filters.

## Milestone 4: Minimal FastAPI service

Add `requirements/rag-api.txt` extending `requirements/rag-agent.txt` with
FastAPI and Uvicorn. Keep `.venv-rag` usable for both the CLI and API; do not add
API dependencies to the ingestion or Spark worker environments.

Implement the following endpoints:

### `POST /v1/ask`

Request:

```json
{
  "question": "إزاي الفلوس بتتنقل بين الدول؟"
}
```

The request model trims whitespace and requires 1–4,000 characters. The response
contains:

```json
{
  "request_id": "uuid",
  "answer": "...",
  "sufficient": true,
  "citations": ["p39:c0"],
  "sources": [],
  "queries": [],
  "evidence": [],
  "retrieval_rounds": 2,
  "timings_ms": {
    "planning": 0,
    "embedding": 0,
    "retrieval": 0,
    "assembly": 0,
    "answering": 0,
    "total": 0
  }
}
```

Evidence remains present even when `sufficient` is false. Each evidence block
retains full text, chunk IDs, query IDs, cosine similarity, and all source
provenance. Do not expose database configuration or raw vectors.

### `GET /health/live`

Return success when the HTTP process and event loop are responsive. Do not call
PostgreSQL, Groq, or ONNX from liveness.

### `GET /health/ready`

Return success only after startup initialization, when embedding assets and
required environment configuration are valid and PostgreSQL answers `SELECT 1`.
Do not spend a Groq request on health checks; verify only that its API key is
configured.

### Error and operational behavior

- Return `422` for invalid request bodies, `429` when bounded request capacity
  cannot be acquired promptly, `502` for Groq failures, `503` for database/model
  unavailability, and `504` for the whole-request timeout.
- A grounded `insufficient` result is a successful `200` response, not a server
  error.
- Generate a request ID, return it in the body and response header, and include it
  in structured logs. Log timings, status, language, query count, round count, and
  evidence count—not question or evidence content.
- Disable permissive CORS by default. Add authentication and rate limiting at the
  deployment boundary before exposing the service outside a trusted network.
- Do not implement streaming in v1: structured output parsing, citation
  validation, and evidence assembly must finish before a trustworthy response is
  returned.

## Testing and rollout

- Unit-test request validation, status mapping, evidence-on-failure responses,
  request IDs, timeouts, capacity limits, startup/shutdown cleanup, and secret
  redaction.
- Integration-test the API with a disposable PostgreSQL schema, a fake Groq chain,
  and a fake embedder. Keep one opt-in live smoke test for the real local BGE-M3
  model, database, and Groq account.
- Load-test two concurrent requests first, then increase only after measuring CPU,
  memory, database latency, Groq latency, and ONNX contention on the target host.
- Run the multilingual retrieval evaluation before and after Milestones 2–4. Block
  rollout on unexplained recall regressions or citation-validation failures.
- Keep exact cosine for the initial API. Evaluate a separate indexed retriever only
  when measured database latency becomes material; compare it against the same
  labeled set before changing the default.

## Deferred work

- Conversation memory and multi-turn question rewriting.
- Streaming answers.
- Semantic near-duplicate detection, reranking, or grounding models.
- LangGraph or durable workflow checkpoints.
- Public authentication implementation and deployment-specific rate limiting.
- HNSW retrieval until corpus size and latency measurements justify approximate
  search.
