# 07 — Configuration

[Docs home](README.md) · Previous: [06 — Database](06-database.md) · Next: [08 — Deployment and running](08-deployment-and-running.md)

---

> **Code baseline.** `master` at `d794b8c`. Differences on `airflow_migration` at
> `2769828` are marked **Branch**. Defaults are *Verified in code* unless labelled
> otherwise.

Every setting the code reads is on this page.

## 1. Where settings live

| Source | Purpose |
|---|---|
| Command-line flags | Choices for one run |
| Exported environment variables | Overrides and service settings |
| `.env` in the project root | Local defaults and secrets. Ignored by git |
| [.env.example](../.env.example) | Template for part of `.env` |
| Code constants | Everything else |

`.env.example` lists only database, embedding, retrieval, and reranker settings, and it
has `GEMINI_API_KEY` twice.

## 2. How settings are resolved

The order is: flag, then exported variable, then `.env`, then the code default. `.env`
loads with `override=False` and is never run as shell code.
*(Verified in code: [jobs/environment.py](../jobs/environment.py))*

`scripts/ask.py` and `scripts/watch_embeddings.py` load `.env` themselves. The Modal
comparison scripts load it too, when `python-dotenv` is installed. `inspect_db.sh` and
`export_csv.sh` do not load `.env`, so export the `PG*` variables before using them.

Validation happens before any work: integers have minimums, memory sizes must match
`[1-9][0-9]*[kKmMgG]?`, and `DRY_RUN`, `NO_PROGRESS`, and `USE_TRAFILATURA` must be `0` or
`1`. `MAX_EXTRACTED_PER_ROUND` was removed, and setting it is an error.

## 3. Database settings

| Variable | Default | Purpose |
|---|---|---|
| `PGHOST` | `localhost` | Host |
| `PGPORT` | `5432` | Port |
| `PGDATABASE` | `warcdb` | Database |
| `PGUSER` | `warc_user` | Role |
| `PGPASSWORD` | `password` | Password. Always set a real value |
| `PGCONNECT_TIMEOUT` | `10` | Connection timeout in seconds |
| `DB_INSPECT_SAMPLES` | `5` | Sample rows in `db.inspect` |
| `DB_INSPECT_PREVIEW` | `300` | Preview length in `db.inspect` |
| `DEAD_LETTER_DIR` | `dead_letter` | Folder for rejected rows |
| `WARC_TEST_DSN` | Not set | Disposable database for tests |

The supervisor and watchdog read different names from `.env`: `POSTGRES_HOST`
(default `localhost`), `POSTGRES_USER`, `POSTGRES_DB`, and `POSTGRES_PASSWORD`. See
[13 §7.2](13-ingestion-operations.md#72-supervisor-behavior).

## 4. Ingestion settings

Each variable has a flag with the same name in lower-kebab-case, for example
`--worker-max-candidates` for `WORKER_MAX_CANDIDATES` and `--lingua-min-confidence` for
`LINGUA_MIN_CONFIDENCE`. Exceptions are in the last table.

**Selection and quotas**

| Variable | Default | Minimum | Purpose |
|---|---:|---:|---|
| `YEAR`, `MONTH` | `2026`, `05` | — | Monthly manifest |
| `NUM_FILES` | `15`. **Branch:** `all` | — | File limit. `all`, `none`, `0`, or empty select the rest |
| `START_FILE_OFFSET` | `0` | 0 | Manifest entries to skip |
| `MAX_ACCEPTED_ARTICLES` | `0` | 0 | Target of new articles. `0` is unlimited |
| `MAX_ACCEPTED_PER_FILE` | `0` | 0 | Per-file target. `0` is unlimited |
| `RESUME_RUN_ID` | Empty | — | Run to resume |

**Budgets**

| Variable | Default | Minimum | Purpose |
|---|---:|---:|---|
| `MAX_FILES_PER_ROUND` | Executor count | 1 | Files per wave |
| `WORKER_MAX_CANDIDATES` | `100` | 1 | Candidates per task |
| `WORKER_MAX_RESULT_BYTES` | `4194304` | 1024 | Result per task. Must be at least `MAX_ARTICLE_BYTES` |
| `WORKER_MAX_INPUT_BYTES` | `33554432` | 65536 | Compressed input per task |
| `WORKER_MAX_SECONDS` | `120` | 1 | Time per task |
| `MAX_HTML_BYTES` | `4194304` | 1024 | Decoded HTML limit |
| `MAX_ARTICLE_BYTES` | `1048576` | 1024 | Serialized article limit |
| `DB_BATCH_SIZE` | `25` | 1 | SQL batch size |
| `MAX_RETRIES` | `3` | 1 | Download and commit attempts |

**Extraction**

| Variable | Default | Purpose |
|---|---:|---|
| `MIN_WORD_COUNT` | `80` | Minimum words |
| `USE_TRAFILATURA` | `1` | Use Trafilatura before selectolax |
| `LINGUA_MIN_CONFIDENCE` | `0.5` | Minimum language confidence, 0 to 1 |

**Flags that differ**

| Variable | Flags |
|---|---|
| `USE_TRAFILATURA` | `--use-trafilatura`, `--no-use-trafilatura` |
| `NO_PROGRESS` | `--no-progress`, `--progress` |
| `DRY_RUN` | `--dry-run`, `--no-dry-run` |
| `RESUME_RUN_ID` | `--resume-run-id` |
| `SPARK_EXECUTOR_INSTANCES` | `--executor-instances` |

`NO_PROGRESS` defaults to `0`, and `DRY_RUN` to `0`.

## 5. Processing stage settings

**Shared flags:** `--dry-run`, `--executor-instances`, `--chunking-version`,
`--parallel-tasks`, `--items-per-task`, `--input-bytes`, and `--result-bytes`.

| Setting | chunk | tag | embed | Variable |
|---|---:|---:|---:|---|
| Parallel tasks | Executor count | Executor count | Executor count | `CHUNK_MAX_PARALLEL_TASKS`, `TAG_MAX_PARALLEL_TASKS`, `EMBED_MAX_PARALLEL_TASKS` |
| Items per task | 25 | 10 | 10 | `CHUNK_ITEMS_PER_TASK`, `TAG_ITEMS_PER_TASK`, `EMBED_ITEMS_PER_TASK` |
| Input bytes | 2 MiB | 2 MiB | 2 MiB | `CHUNK_TASK_INPUT_BYTES`, `TAG_TASK_INPUT_BYTES`, `EMBED_TASK_INPUT_BYTES` |
| Result bytes | 4 MiB | 1 MiB | 1 MiB | `CHUNK_TASK_RESULT_BYTES`, `TAG_TASK_RESULT_BYTES`, `EMBED_TASK_RESULT_BYTES` |

`--chunking-version` defaults to the `CHUNKING_VERSION` constant in code. In the stage
jobs there is no environment override for it. Only the RAG query reads a
`CHUNKING_VERSION` variable.

**Stage flags**

| Stage | Flag | Default | Variable |
|---|---|---|---|
| chunk | `--document-limit` | None | — |
| tag | `--analysis-limit` | None | — |
| tag | `--retry-errors` | Off | — |
| tag | `--backend` | `groq` | `TAGGING_BACKEND` |
| tag | `--model` | `qwen/qwen3.8-27b` | `TAGGING_MODEL` |
| tag | `--analysis-version` | `chunk-analysis-v2.4.1` | `TAGGING_ANALYSIS_VERSION` |
| tag | `--export-json` | None | — |
| embed | `--embedding-limit` | None | — |
| embed | `--model` | `BAAI/bge-m3-int8` | `EMBEDDING_MODEL` |
| embed | `--max-length` | `256` | `EMBEDDING_MAX_LENGTH` |
| embed | `--embedding-batch-size` | `1` | `EMBEDDING_BATCH_SIZE` |

**Tagging pacing**

| Variable | Default | Purpose |
|---|---:|---|
| `TAG_REQUEST_INTERVAL_SECONDS` | `1.5` | Minimum seconds between requests |
| `TAG_REQUESTS_PER_MINUTE` | `0` (off) | Request pacing |
| `TAG_TOKENS_PER_MINUTE` | `0` (off) | Token pacing |
| `TAG_PARALLELISM` | `3` | Set by the job to `parallel_tasks`; do not set it by hand |

**Embedding**

| Variable | Default | Purpose |
|---|---:|---|
| `EMBEDDING_MODEL_DIR` | `.models/bge-m3-int8` | Model folder on the launcher. Executors use `./embedding-model` |
| `EMBEDDING_INTRA_OP_THREADS` | `1` | ONNX threads. The job forces `1` on executors |
| `EMBEDDING_VERSION` | Not set | Read only by the RAG query and the Modal compare script. See the warning in [01 §7](01-system-overview.md#7-version-keys-connecting-the-stages) |

## 6. Retrieval settings

| Variable | Default | Purpose |
|---|---:|---|
| `RAG_PROVIDER` | Inferred | `groq` or `gemini` |
| `RAG_MODEL` | Provider default | `openai/gpt-oss-120b` or `gemini-3.5-flash-lite` |
| `RAG_REASONING_EFFORT` | `medium` | Groq only. Do not go below `medium` with `gpt-oss-120b` |
| `TOP_K` | `5` | Hits kept per query after reranking |
| `RERANKER_CANDIDATE_K` | `20` | Cosine candidates per query. Must be at least `TOP_K` |
| `RAG_DB_CONCURRENCY` | `3` | Concurrent searches |
| `RERANKER_MODEL_DIR` | `.models/bge-reranker-v2-m3-int8` | Reranker folder |
| `RERANKER_MAX_LENGTH` | `512` | Tokens per pair |
| `RERANKER_BATCH_SIZE` | `4` | Pairs per batch |
| `RERANKER_INTRA_OP_THREADS` | `1` | ONNX threads |
| `CHUNKING_VERSION` | Code constant | Version to search |
| `EMBEDDING_MODEL` | `BAAI/bge-m3-int8` | Model to search |
| `EMBEDDING_MAX_LENGTH` | `256` | Must match the stored vectors |
| `SUPPORTED_LANGUAGES` | `ar,en,fr` in `.env.example` | No code reads it |
| `RAG_FETCH_BATCH_SIZE` | `1000` | Rows per fetch in `db.documents` |
| `RAG_DOCUMENT_LIMIT` | Not set | Limit for `load_documents_distributed` |

## 7. Launcher and Spark settings

| Variable | Default | Purpose |
|---|---:|---|
| `SPARK_HOME` | Required | Spark installation |
| `SPARK_MASTER_URL` | Not set | Overrides the master for direct runs |
| `SPARK_EXECUTOR_INSTANCES` | `3`; embed `2` | Executors |
| `SPARK_EXECUTOR_CORES` | `1` | Cores per executor |
| `SPARK_DRIVER_MEMORY` | `512m` | Driver heap |
| `SPARK_EXECUTOR_MEMORY` | `512m` | Executor heap |
| `SPARK_EXECUTOR_MEMORY_OVERHEAD` | `512m`; embed `1g` | Container overhead |
| `EMBEDDING_EXECUTOR_MEMORY` | Generic value | Embed heap |
| `EMBEDDING_EXECUTOR_MEMORY_OVERHEAD` | Generic value | Embed overhead |
| `SPARK_YARN_AM_MEMORY` | `512m` | ApplicationMaster heap |
| `SPARK_YARN_AM_MEMORY_OVERHEAD` | `384m` | ApplicationMaster overhead |
| `USE_HDFS_CACHE` | `1` | Share archives through HDFS |
| `HDFS_APPS_DIR` | `/user/$USER/apps/spark` | HDFS cache folder |
| `VENV_DIR` | `.venv` | Main environment |
| `EMBEDDING_VENV_DIR` | Not set | Embedding environment, first choice |
| `EMBEDDING_ONNX_VENV_DIR` | `.venv-embedding-onnx` | Embedding environment, second choice |
| `SPARK_WARC_EXPORTED_ENV_KEYS` | Set by the launcher | Do not set it by hand |

The launcher also sets `RAYON_NUM_THREADS`, `OMP_NUM_THREADS`, and `OPENBLAS_NUM_THREADS`
to `1`, and sets `PYSPARK_DRIVER_PYTHON` and `PYSPARK_PYTHON`. Do not set these by hand.
`MODEL_ARCHIVE_SPEC`, `MODEL_FILE`, and `TARGET_MODEL` are internal script variables, not
settings. **Branch:** the DAG sets `PYTHONPATH` for its tasks.

**Setup scripts**

| Variable | Default | Script |
|---|---|---|
| `EMBEDDING_SETUP_PYTHON` | `python3` | Environment setup |
| `EMBEDDING_SOURCE_MODEL_DIR` | `.models/bge-m3` | Embedding download |
| `EMBEDDING_QUANTIZED_MODEL_DIR` | `.models/bge-m3-int8`, after `EMBEDDING_MODEL_DIR` | Embedding download |
| `EMBEDDING_ONNX_MODEL_REVISION` | `25b9af8e…` | Embedding download |
| `RERANKER_VENV_DIR` | Not set | Reranker download, first choice |
| `RAG_VENV_DIR` | `.venv-rag` | Reranker download, second choice |
| `HF_TOKEN` | Not set | Hugging Face access for both downloads |

**Airflow units** set these in each systemd file: `HOME`, `AIRFLOW_HOME`, `PATH`,
`SPARK_HOME`, `HADOOP_HOME`, `HADOOP_CONF_DIR`, `JAVA_HOME`, `USER`, `HDFS_APPS_DIR`,
`PYTHONUNBUFFERED`, `NO_PROGRESS=1`, and `TZ=UTC`. Extra values come from
`/home/spark-master/airflow/airflow.env`, which is not in the repository.
`airflow/systemd/install.sh` also reads `AIRFLOW_HOME` and `UNIT_DIR`.

**Branch:** the `ingestion_pipeline` tasks set `TZ=EET`, while the units keep `TZ=UTC`.

## 8. Secrets

| Secret | Where | Used by |
|---|---|---|
| `PGPASSWORD` | `.env` or environment | All database access |
| `GROQ_API_KEY` | `.env` or environment | Tagging, RAG with Groq |
| `GEMINI_API_KEY` or `GOOGLE_API_KEY` | `.env` or environment | RAG with Gemini |
| `HF_TOKEN` | Environment | Model downloads |
| `MODAL_PROXY_TOKEN` | `.env` or environment | Modal tools |
| `POSTGRES_PASSWORD` | `.env` | Supervisor, watchdog |
| `~/tg_bot_token`, `~/sudo_pw` | Files on the master | [13 §6](13-ingestion-operations.md#6-files-outside-the-repository) |

**How secrets are protected**

- `.env` and `.env.*` are ignored by git, except `.env.example`.
- Spark's redaction settings hide secret, password, token, and API-key values, plus `gsk_…` strings.
- `GROQ_API_KEY` is removed from error messages and stored analysis errors.

**Limits.** `GROQ_API_KEY` is passed to executors as `spark.executorEnv.GROQ_API_KEY`. The
code has a default password of `password`. A secrets manager is
**Proposed (not implemented)**.

## 9. Modal settings

These apply only to the tools in [scripts §6](04-modules/scripts.md#6-modal-tools).

| Variable | Default | Purpose |
|---|---:|---|
| `MODAL_MAX_CONTAINERS` | `1` | Keep at 1 when generating a corpus |
| `MODAL_MIN_CONTAINERS` | `0` | Warm containers |
| `MODAL_SCALEDOWN_WINDOW` | `30` | Idle seconds |
| `MODAL_CLOUD`, `MODAL_REGION` | Not set | Placement |
| `MODAL_EMBEDDING_ENDPOINT` | Built-in URL | Endpoint for `modal_pin_avx2.py` |
| `MODAL_EMBEDDING_MODEL` | `bge-m3-int8` | Model name for the compare script |
| `MODAL_PROXY_TOKEN` | Not set | Proxy authentication |

## 10. Version settings

Changing these creates new derived rows, instead of changing existing ones:

| Change | Effect | Next step |
|---|---|---|
| Cleaning, filters, or chunk size | Update `CHUNKING_VERSION` in code | Rerun chunking, then tagging and embedding |
| Tagging prompt or rules | Change `TAGGING_ANALYSIS_VERSION` | Rerun tagging |
| Tagging model | Change `TAGGING_MODEL` | Rerun tagging |
| Embedding model files or `EMBEDDING_MAX_LENGTH` | `embedding_version` changes by itself | Rerun embedding |

Steps are in [09 §6](09-operations.md#6-reprocessing).
