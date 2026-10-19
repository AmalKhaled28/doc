# 01 — System Overview

[Docs home](README.md) · Next: [02 — Hardware and infrastructure](02-hardware-and-infrastructure.md)

---

## 1. Purpose

The system builds a grounded question-answering service over recent multilingual news.
It ingests CC-NEWS web archives, keeps Arabic, English, and French articles, turns them
into versioned text chunks, LLM analyses, and vector embeddings, and answers questions
with citations to the stored chunks.

## 2. Scope

| In scope (implemented) | Confidence |
|---|---|
| Quota-bounded, resumable ingestion of one CC-NEWS monthly manifest per run | Verified in code |
| HTML extraction, language detection, and filtering to `ar`, `en`, `fr` | Verified in code |
| Deterministic cleaning, splitting (800 characters, 100 overlap), and noise filtering | Verified in code |
| LLM chunk analysis through a pluggable backend (Groq adapter included) | Verified in code (stage is unstable, see Known Issues) |
| CPU embedding with BGE-M3 INT8 ONNX into pgvector | Verified in code |
| CLI RAG agent: query planning, exact cosine search, cross-encoder reranking, cited answers | Verified in code |
| Database inspection, CSV export, and embedding progress monitoring | Verified in code |

| Out of scope today | Status |
|---|---|
| Continuous or scheduled operation of any stage | Proposed (not implemented) |
| HTTP query API (`query_service/` is an empty package) | Proposed (not implemented) |
| Monitoring, alerting, secrets management, backups | Proposed (not implemented) |
| Infrastructure provisioning (VMs, Hadoop, PostgreSQL) | Not in repository; TODO(team) |
| Arabic dialect identification (`metadata.arabic_dialect` is always `NULL`) | Not implemented (Verified in code: `_candidate` in [ingestion/pipeline.py](../ingestion/pipeline.py)) |
| Languages other than Arabic, English, French | Excluded by design (Verified in code: [ingestion/text.py](../ingestion/text.py)) |

## 3. System context

**Level 1: big picture**

![System context](diagrams/01-system-context.svg)

```mermaid
flowchart LR
    OP["Operator"]:::ext
    AN["Analyst"]:::ext
    CC["Common Crawl"]:::ext
    SYS["News RAG system"]:::svc
    LLM["LLM APIs"]:::ext
    PG[("PostgreSQL")]:::store

    OP -->|"runs jobs"| SYS
    AN -->|"asks questions"| SYS
    CC -->|"news archives"| SYS
    SYS <-->|"prompts"| LLM
    SYS <-->|"reads, writes"| PG

    classDef svc fill:#E3F2FD,stroke:#1565C0,color:#0D47A1
    classDef store fill:#E8F5E9,stroke:#2E7D32,color:#1B5E20
    classDef ext fill:#FFF3E0,stroke:#EF6C00,color:#E65100
    classDef proposed fill:#F5F5F5,stroke:#9E9E9E,stroke-dasharray:5 5,color:#616161
```

An operator runs the batch jobs and an analyst asks questions; both are external actors,
drawn in orange. News enters only from Common Crawl, the LLM APIs are Groq (tagging and
answering) and Google Gemini (answering only), and PostgreSQL with pgvector holds all
state. One-time model downloads from Hugging Face are omitted. *(Verified in code: [retrieval/llm.py](../retrieval/llm.py), setup scripts in [scripts/](../scripts/))*

## 4. High-level architecture

The system splits into a **processing plane** (Spark jobs that write data) and a
**query plane** (a local process that reads it). The two diagrams below show each
plane; every module is listed in the [component map](#5-component-map).

### 4.1 Processing plane

**Level 2: processing pipeline**

![Processing pipeline](diagrams/01-processing-pipeline.svg)

```mermaid
flowchart LR
    CC["Common Crawl"]:::ext
    PG[("PostgreSQL")]:::store

    subgraph SPARK["Spark cluster"]
        ING["Ingestion"]:::svc
        CHK["Chunking"]:::svc
        TAG["Tagging"]:::svc
        EMB["Embeddings"]:::svc
    end

    CC --> ING --> CHK
    CHK --> TAG --> PG
    CHK --> EMB --> PG

    classDef svc fill:#E3F2FD,stroke:#1565C0,color:#0D47A1
    classDef store fill:#E8F5E9,stroke:#2E7D32,color:#1B5E20
    classDef ext fill:#FFF3E0,stroke:#EF6C00,color:#E65100
    classDef proposed fill:#F5F5F5,stroke:#9E9E9E,stroke-dasharray:5 5,color:#616161
    style SPARK fill:#FFFFFF,stroke:#BDBDBD
```

Arrows show the logical order of data. In practice every stage reads its input from
PostgreSQL and writes its output back there, so the stages never call one another and
tagging and embeddings can run in parallel. Launch scripts (`scripts/`), the shared
runtime (`jobs/`), the database stores (`db/`), and the Groq calls made by tagging are
described in [§5](#5-component-map) and [04-modules/](04-modules/). *(Verified in code)*

### 4.2 Query plane

**Level 2: query side**

![Query side](diagrams/01-query-side.svg)

```mermaid
flowchart LR
    Q["Question"]:::svc
    PL["Planner (LLM)"]:::svc
    VS["Vector search"]:::svc
    RR["Reranker"]:::svc
    AN["Answer (LLM)"]:::svc
    CA["Cited answer"]:::svc
    PG[("PostgreSQL")]:::store

    Q --> PL --> VS --> RR --> AN --> CA
    VS <--> PG

    classDef svc fill:#E3F2FD,stroke:#1565C0,color:#0D47A1
    classDef store fill:#E8F5E9,stroke:#2E7D32,color:#1B5E20
    classDef ext fill:#FFF3E0,stroke:#EF6C00,color:#E65100
    classDef proposed fill:#F5F5F5,stroke:#9E9E9E,stroke-dasharray:5 5,color:#616161
```

The LLM turns the question into at most three search queries, each embedded with
BGE-M3 and searched by exact cosine distance in PostgreSQL. A local cross-encoder
reranks the candidates, and the LLM writes an answer that cites chunk IDs, with one
optional follow-up search round. This runs in `.venv-rag` through `scripts/ask.py`; the
empty `query_service/` package is reserved for the proposed API. *(Verified in code:
[retrieval/](../retrieval/))*

## 5. Component map

| Module | Path | Responsibility | Runs on | Python env | Entry point | Reads | Writes |
|---|---|---|---|---|---|---|---|
| Ingestion | [ingestion/](../ingestion/) | Manifest selection, WARC streaming, extraction, language filter, quotas | Spark driver + executors | `.venv` (packed) | `scripts/run_ingestion.sh` | Common Crawl | `websites`, `authors`, `pages`, `metadata`, `content`, `ingest_*` |
| Chunking | [chunking/](../chunking/) | Cleaning, splitting, noise flags, chunk hashing | Spark | `.venv` (packed) | `scripts/run_chunking.sh` | `pages`, `content`, `metadata` | `article_chunks`, `chunk_materializations` |
| Tagging | [tagging/](../tagging/) | LLM summary, category, tags per eligible chunk | Spark executors to Groq | `.venv` (packed) | `scripts/run_tagging.sh` | `article_chunks`, `metadata` | `chunk_analyses` |
| Embeddings | [embeddings/](../embeddings/) | BGE-M3 INT8 ONNX vectors (1024-dim, L2-normalized) | Spark executors | `.venv-embedding-onnx` (packed) | `scripts/run_embeddings.sh` | `article_chunks` | `chunk_embeddings` |
| Retrieval | [retrieval/](../retrieval/) | RAG agent, vector search, reranking, evidence, LLM providers | Local process | `.venv-rag` | `scripts/ask.py` | `chunk_embeddings` plus joins | stdout only |
| DB | [db/](../db/) | Connections, migrations, transactional commits, stage stores, inspection | Driver / local | any | `scripts/inspect_db.sh` | all tables | all tables |
| Jobs | [jobs/](../jobs/) | Spark session, wave executor, config resolution, `.env` loading | Driver | `.venv` | used by launchers | — | — |
| Scripts | [scripts/](../scripts/) | Launchers, packaging, model setup, export, monitoring | Spark master / local | various | — | — | `exports/`, `.build-tmp/`, `.models/` |
| Query service | [query_service/](../query_service/) | Empty placeholder | — | — | — | — | — |
| Tests | [tests/](../tests/) | Unit, integration (`WARC_TEST_DSN`), and Spark smoke tests | Local | `.venv` + optional deps | `python -m unittest` | disposable DB | disposable schemas |

Each module has a detailed page in [04-modules/](04-modules/).

## 6. Design principles

| Principle | Mechanism | Source |
|---|---|---|
| Single writer per stage | Only the driver touches PostgreSQL; executors receive and return plain dicts | [jobs/batching.py](../jobs/batching.py), stage `pipeline.py` files |
| One coordinator per stage | Session advisory locks; the ingestion lock is global across manifests | `acquire_coordinator_lock` in [db/db_handler.py](../db/db_handler.py), `acquire_lock` in [db/chunks.py](../db/chunks.py) |
| Bounded memory | Item, input-byte, and result-byte limits per task; HTML and article size caps | [jobs/runtime.py](../jobs/runtime.py), [ingestion/pipeline.py](../ingestion/pipeline.py) |
| Idempotent commits | Ingestion chunk receipts; `ON CONFLICT` keys; hash checks on chunks and vectors | [db/](../db/) stores |
| Versioned derived data | New version strings produce new rows instead of overwriting | `CHUNKING_VERSION`, `analysis_version`, `model_version()` |
| Resumability | Re-running a stage selects only rows missing for the current version; ingestion resumes by `RESUME_RUN_ID` | stage stores, `restore_run_config` |
| No hidden fallbacks | Missing model files, hash drift, or quota overflow raise errors instead of degrading | [retrieval/reranker.py](../retrieval/reranker.py), [db/chunks.py](../db/chunks.py) |

All rows above are *Verified in code*.

## 7. Version keys connecting the stages

Downstream stages find their input by version key. The table shows the defaults
currently in code.

| Key | Default value | Set by | Source |
|---|---|---|---|
| `chunking_version` | `recursive-800-100-clean-v2.4.1-v1` | `CHUNKING_VERSION` constant; `--chunking-version` flag | [chunking/__init__.py](../chunking/__init__.py) |
| Tagging `backend` | `groq` | `TAGGING_BACKEND` / `--backend` | [jobs/runtime.py](../jobs/runtime.py) |
| Tagging `model` | `qwen/qwen3.8-27b` | `TAGGING_MODEL` / `--model` | [jobs/runtime.py](../jobs/runtime.py) |
| `analysis_version` | `chunk-analysis-v2.4.1` | `TAGGING_ANALYSIS_VERSION` / `--analysis-version` | [jobs/runtime.py](../jobs/runtime.py) |
| `embedding_model` | `BAAI/bge-m3-int8` | `EMBEDDING_MODEL` / `--model` | [jobs/runtime.py](../jobs/runtime.py) |
| `embedding_version` | `bge-m3-int8-<20 hex chars>`, computed from model files and `max_length` | `model_version()`; RAG may override with `EMBEDDING_VERSION` | [embeddings/embeddings.py](../embeddings/embeddings.py) |

The current data snapshot in `exports/` uses `embedding_version =
bge-m3-int8-9fb3afc85bfa0f8662dd`. *(Verified from exports/chunk_embeddings.csv)*

## 8. Technology stack

| Layer | Technology | Version | Source |
|---|---|---|---|
| Language | Python 3; Bash | Python version TODO(team) | scripts; `str \| None` annotations in [tagging/contracts.py](../tagging/contracts.py) need Python 3.10+ (Inferred) |
| Processing | Apache Spark on YARN (client mode), HDFS | Spark 4.2.0 recorded; Hadoop TODO(team) | [scripts/run_processing.sh](../scripts/run_processing.sh); [archive/validation.md](archive/validation.md) |
| Database | PostgreSQL + pgvector | PostgreSQL 18 recorded in tests; pgvector TODO(team) | [db/vector_schema.py](../db/vector_schema.py); [archive/validation.md](archive/validation.md) |
| WARC parsing | FastWARC | `==1.0.9` | [requirements/base.txt](../requirements/base.txt) |
| Extraction | trafilatura, selectolax, charset-normalizer | `>=2,<3`; `>=0.3,<0.5`; `>=3.4,<4` | [requirements/base.txt](../requirements/base.txt) |
| Language ID | lingua-language-detector | `==2.2.0` | [requirements/base.txt](../requirements/base.txt) |
| DB driver | psycopg2-binary, pgvector (Python) | `>=2.9,<3`; `>=0.3,<1` | requirements files |
| Text splitting | langchain-text-splitters, langchain-core | `>=1,<2` | [requirements/rag.txt](../requirements/rag.txt) |
| LLM clients | langchain-groq, langchain-google-genai | `>=1,<2`; `>=4,<5` | [requirements/rag-agent.txt](../requirements/rag-agent.txt) |
| Model runtime | onnxruntime (CPU), transformers (tokenizers) | `>=1.20,<2`; `>=4.45,<6` | [requirements/embedding-worker-onnx.txt](../requirements/embedding-worker-onnx.txt) |
| Embedding model | BGE-M3, INT8 ONNX (`onnx-community/bge-m3-ONNX`, pinned revision) | revision `25b9af8e...` | [scripts/download_quantized_embedding_model.sh](../scripts/download_quantized_embedding_model.sh) |
| Reranker model | bge-reranker-v2-m3, INT8 ONNX (pinned, SHA-256 verified) | revision `90213ffc...` | [scripts/download_quantized_reranker_model.sh](../scripts/download_quantized_reranker_model.sh) |
| Schemas | pydantic | `>=2,<3` | requirements files |
| Packaging | venv-pack, zip, HDFS cache | `==0.2.0` | [scripts/run_processing.sh](../scripts/run_processing.sh) |

### Python environments

| Environment | Requirements file | Used by | Shipped to executors |
|---|---|---|---|
| `.venv` | `requirements/base.txt`, plus `rag.txt` and `tagging-groq.txt` for processing | Launcher bootstrap, ingestion, chunking, tagging, `inspect_db.sh` | Yes (packed per fingerprint) |
| `.venv-embedding-onnx` | `requirements/embedding-worker-onnx.txt` | Embedding stage, `watch_embeddings.sh` | Yes |
| `.venv-rag` | `requirements/rag-agent.txt` | `scripts/ask.py`, reranker download | No |
| (dev) `.venv` + `requirements/dev.txt` | `base.txt` + `warcio` | Tests | No |

`requirements/embeddings.txt` installs FastAPI, uvicorn, and langdetect, but no code
imports them (see Known Issues). *(Verified in code)*

## 9. Prototype and production at a glance

| Aspect | Prototype (today) | Production (Proposed, not implemented) |
|---|---|---|
| Ingestion trigger | Operator runs `run_ingestion.sh` with quotas | Long-running service watching the CC-NEWS manifest |
| Processing trigger | Operator runs each stage script | Scheduler (cron or Airflow) or after each ingestion batch |
| Query interface | `scripts/ask.py` CLI | FastAPI service in `query_service/` wrapping `retrieval.answer()` |
| Observability | stdout, Spark UI, `inspect_db.sh`, `watch_embeddings.sh` | Metrics, dashboards, alerting |
| Secrets | `.env` file and environment variables | Secrets manager |
| Backups | None in repository | Scheduled PostgreSQL backups |

The full per-module comparison and migration path are in
[10-prototype-vs-production.md](10-prototype-vs-production.md).

## 10. Conventions

- **Exit codes** *(Verified in code)*:
  - Ingestion: `0` complete, `1` failure, `2` shortfall or no files selected.
  - Chunking, tagging, embeddings: `0` success, `1` failure, `2` tagging quota or long provider cooldown, `130` interrupted.
  - `ask.py`: `0` sufficient answer, `1` insufficient evidence.
- **Configuration precedence** for launchers: CLI flag, then an exported variable, then `.env`, then the code default. *(Verified in code: [jobs/environment.py](../jobs/environment.py) loads `.env` with `override=False`)*
- **Working directory**: run all commands from the project root. Launchers `cd` to the root themselves. *(Verified in code)*
