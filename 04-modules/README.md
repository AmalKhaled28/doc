# 04 — Modules

[Docs home](../README.md) · Previous: [03 — Cluster design](../03-cluster-design.md) · Next: [05 — Data flow](../05-data-flow.md)

---

> **Code baseline.** Every page in this folder reads `master` at `d794b8c`. Differences on
> `airflow_migration` at `2769828` are marked **Branch**. Statements are *Verified in
> code* unless labelled otherwise.

One page per top-level package. Each page lists the files, the entry points, the rules
the code applies, and the failure behavior.

| Module | Page | Responsibility |
|---|---|---|
| `ingestion/` | [ingestion.md](ingestion.md) | Stream WARC files, extract articles, filter by language, respect quotas |
| `chunking/` | [chunking.md](chunking.md) | Clean article text, split it, flag noise |
| `tagging/` | [tagging.md](tagging.md) | LLM summary, category, and tags per eligible chunk |
| `embeddings/` | [embeddings.md](embeddings.md) | BGE-M3 INT8 vectors |
| `retrieval/` | [retrieval.md](retrieval.md) | The RAG agent: planning, search, reranking, cited answers |
| `db/` | [db.md](db.md) | Connections, migrations, transactional commits, stage stores |
| `jobs/` | [jobs.md](jobs.md) | Spark session, task batching, configuration, `.env` loading |
| `scripts/` | [scripts.md](scripts.md) | Launchers, setup, export, monitoring, Modal tools, ingestion operations |
| `query_service/` | [query_service.md](query_service.md) | Empty placeholder for the proposed HTTP API |
| `tests/` | [tests.md](tests.md) | Unit, integration, and smoke tests |

## How the packages depend on each other

| Package | Imports from | Runs on |
|---|---|---|
| `ingestion` | `jobs`, `db` | Driver and executors |
| `chunking` | `jobs`, `db` | Driver and executors |
| `tagging` | `jobs`, `db` | Driver and executors |
| `embeddings` | `jobs`, `db` | Driver and executors |
| `retrieval` | `db`, `embeddings`, `chunking`, `jobs` | Local process in `.venv-rag` |
| `db` | No other project package | Driver or local process |
| `jobs` | Imports stage and `db` code lazily inside functions | Driver |

Package initializers stay light, so importing a stage does not load a model or an
optional dependency. Executors never receive a database connection: they get plain
dictionaries and return plain dictionaries. See
[03 §3](../03-cluster-design.md#3-how-workloads-are-split).

The launcher ships six packages to executors: `ingestion`, `db`, `chunking`, `tagging`,
`embeddings`, and `jobs`. `retrieval`, `scripts`, and `tests` are not shipped.
