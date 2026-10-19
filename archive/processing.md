# Spark processing stages

Independent YARN jobs materialize chunks, analyze eligible chunks, and embed
eligible chunks. They default to one concurrent Spark task per requested executor. PostgreSQL reads and writes stay
on the driver; executors receive bounded payloads and do not need DB credentials.

All four launchers (ingestion, chunking, tagging, embedding) delegate packaging and YARN
submission to `scripts/run_processing.sh`. Spark startup and task submission live
in `jobs/runtime.py` and `jobs/batching.py`. Ingestion retains its original Spark
retry settings, one-partition-per-file wave, ordered commits, quotas, and recovery
logic; chunking, tagging, and embedding use independent jobs with per-batch completion.

```text
PostgreSQL articles -> driver batches -> Spark cleaning/splitting -> article_chunks
article_chunks -> driver batches -> Spark analyzer backend -> chunk_analyses
article_chunks -> driver batches -> Spark ONNX inference -> chunk_embeddings
```

## Embedding

Prepare the dedicated embedding environment and local INT8 model once. The
other services continue to use `.venv`:

```bash
bash scripts/setup_embedding_onnx_env.sh
bash scripts/download_quantized_embedding_model.sh
./scripts/run_embeddings.sh --embedding-limit 100
```

The model download uses the existing BGE-M3 tokenizer assets and a pinned ONNX
INT8 graph. The launcher fingerprints and archives the model for YARN workers;
set `EMBEDDING_MODEL_DIR` when the quantized model lives outside
`.models/bge-m3-int8`; set `EMBEDDING_SOURCE_MODEL_DIR` when its base tokenizer
assets live outside `.models/bge-m3`.
Workers run CPU inference, with max length 256, batch size 1, and one native ONNX
thread by default. The embedding limit counts scheduled chunks.

Vectors are stored in `chunk_embeddings`, linked to `article_chunks` by
`page_id`, `chunking_version`, and `chunk_index`. They retain the chunk text hash
and use `embedding_model` plus a fingerprinted `embedding_version` to identify
the model files and vector-affecting settings. A migration labels any preexisting
unversioned vectors `legacy-v0`.

Embedding runs resume by selecting eligible chunks without a row for the chosen
model and embedding version. Each completed task commits independently; restart
the same command to continue. Committed rows are idempotent, while a text-hash
conflict fails explicitly. A different model/configuration fingerprint creates
a separate vector identity. PostgreSQL must have the pgvector extension
available to the database role used for schema initialization.

## Run

From the project root, install the optional dependencies into the virtual
environment that the launchers package for YARN:

```bash
.venv/bin/python -m pip install -r requirements/rag.txt
./scripts/run_chunking.sh --document-limit 100

.venv/bin/python -m pip install -r requirements/tagging-groq.txt
export GROQ_API_KEY=your-key
./scripts/run_tagging.sh --analysis-limit 30
```

Export `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, and `PGPASSWORD` as for ingestion.
Set `SPARK_HOME` to the cluster Spark installation. All four shell launchers
load project-root `.env` defaults; direct Python module commands do not.
The loader uses the existing `.venv` interpreter (or an externally supplied
`VENV_DIR`), so that bootstrap environment must contain `requirements/base.txt`.
The Groq key is delivered through executor environment configuration
with Spark redaction enabled; it is never included in source/environment archives
or command-line arguments. Use only a trusted cluster: redaction is not a secret
store and cluster administrators can access executor environments.

`--dry-run` validates arguments without opening Spark, PostgreSQL, or a provider
connection and without packaging. All launchers support `--help`.

## Configuration

Precedence is CLI flag > exported/inline variable > `.env` > code default.
Use flags for run-specific choices and environment variables for credentials,
cluster paths, and reusable defaults. `--no-dry-run` overrides `DRY_RUN=1`.

| Choice | Flag | Environment fallback |
|---|---|---|
| Executor count (3; embedding 2) | `--executor-instances` | `SPARK_EXECUTOR_INSTANCES` |
| Task concurrency (defaults to executor count) | `--parallel-tasks` | `CHUNK_MAX_PARALLEL_TASKS`, `TAG_MAX_PARALLEL_TASKS`, `EMBED_MAX_PARALLEL_TASKS` |
| Task size and byte bounds | `--items-per-task`, `--input-bytes`, `--result-bytes` | Stage-prefixed `*_ITEMS_PER_TASK`, `*_TASK_INPUT_BYTES`, `*_TASK_RESULT_BYTES` |
| Ingestion selection and budgets | `--year`, `--month`, `--num-files`, `--max-files-per-round`, etc. | Uppercase flag name; see [ingestion controls](../../README.md#configuration) |
| Ingestion resume | `--resume-run-id` | `RESUME_RUN_ID` |
| Embedding model/settings | `--model`, `--max-length`, `--embedding-batch-size` | `EMBEDDING_MODEL`, `EMBEDDING_MAX_LENGTH`, `EMBEDDING_BATCH_SIZE` |

For embedding, use `EMBEDDING_VENV_DIR` to move the dedicated environment
(`EMBEDDING_ONNX_VENV_DIR` remains an alias). Executor memory/overhead default to
`512m`/`1g`, fitting one 1.5 GiB YARN container on each 2 GiB worker;
embedding-specific variables override generic `SPARK_EXECUTOR_MEMORY`
and `SPARK_EXECUTOR_MEMORY_OVERHEAD`, which override those defaults. Spark fixes
embedding worker ONNX threads at one. Ingestion resume restores saved selection,
quotas, and filters: changed `.env` defaults are ignored, but conflicting flags or
exported variables are rejected.

With three workers at 1.5 GiB each, the application master occupies one worker;
embedding therefore defaults to two executors and two concurrent tasks. For 50
pending chunks in batches of ten:

```bash
./scripts/run_embeddings.sh --items-per-task 10 --embedding-limit 50
```

Monitor corpus-wide pending embeddings every 10 seconds with
`./scripts/watch_embeddings.sh` (Ctrl+C stops the monitor, not the job). For a
limited run, add `--run-limit 50 --starting-embedded N`, where `N` is the
embedded count recorded before that run started; without a saved baseline, the
database cannot distinguish that run's remaining work from other pending chunks.

Omit limits to process all pending work. Rerun the same command to resume. The
chunking document limit counts pending documents, including documents that clean
to no chunks. The tagging limit counts scheduled chunks, including model errors.
`--retry-errors` makes previously failed analyses eligible. Terminal `SUCCESS`
and `NOISE` rows are skipped. Tagging does not implicitly run chunking.

```bash
./scripts/run_chunking.sh --items-per-task 25 --parallel-tasks 3
./scripts/run_tagging.sh --items-per-task 10 --parallel-tasks 3 --retry-errors
./scripts/run_tagging.sh --analysis-limit 5 --export-json results.json
```

Export writes all stored analyses for the selected chunking/backend/model/analysis
versions, including errors, in document/index order. It atomically replaces the
specified output file. Legacy JSON results are not imported.

## Batches, quotas, and recovery

Defaults are 25 documents per chunking task and 10 chunks per tagging or embedding
task, with three concurrent tasks for chunking/tagging and two for embedding by default. Input payloads are bounded to 2 MiB per task. Output
limits are 4 MiB for chunking and 1 MiB for tagging/embedding. Override with `--input-bytes`
and `--result-bytes`; a single oversized input or output fails explicitly. Reduce
batch count or increase byte limits to accommodate it. A document is never
partially materialized.

Spark dynamic allocation is disabled. Ingestion uses `--max-files-per-round` for
file concurrency. Higher task concurrency increases driver memory usage and
simultaneous provider requests.

The driver submits each task as an independent single-partition Spark job. A
bounded wave has at most the configured number of parallel jobs, and each completed result is committed
before waiting for slower siblings. Database inserts use bulk values. Chunk
transactions cover one document, while tagging transactions cover one task batch.
Selection is ordered by document/index; parallel completion order is not.

Groq uses one sequential request stream per active task. A Groq task batch is not
a provider bulk API request. Set `TAG_REQUEST_INTERVAL_SECONDS` (default 1.5) to
pace each worker, or set `TAG_REQUESTS_PER_MINUTE` and `TAG_TOKENS_PER_MINUTE` to
your account's shared limits. The adapter divides throughput among configured
workers and conservatively estimates input tokens from UTF-8 bytes. This is
rate shaping, not a cross-process quota ledger: concurrent startup requests and
other applications sharing the account can still receive 429 responses. Workers
honor retry delays, use at most five explicit attempts, and disable SDK retries.

Daily quota exhaustion returns completed outcomes and stops later waves after
all active siblings finish. The blocked item remains pending. Exit codes are 0
for completion, 1 for failure, 2 for quota/long provider cooldown, and 130 for an
interrupt. A job can finish with recorded per-item ERROR rows; inspect the printed
counts and rerun with `--retry-errors`.

Ctrl+C drains and commits the current small wave before exiting; a second
interrupt can force an earlier exit. Each batch prints its committed counts,
while final corpus totals are queried once at the end of the run.

Speculation is disabled and Spark task retries are limited to one attempt.
Database outcomes are idempotent, but an external call cannot be committed
atomically with PostgreSQL. A failed task can repeat up to its batch size on
restart; a driver crash can repeat all uncommitted active batches (up to 30 calls
at defaults). Cumulative attempt counts cover committed outcomes, not calls lost
to a worker/driver crash. There is no exactly-once billing guarantee.

Stage-specific advisory locks prevent simultaneous coordinators of the same
kind, while ingestion, chunking, tagging, and embedding can run independently. Changes
committed after a job's source snapshot are picked up on the next invocation.

## Backend interface

`tagging.contracts.AnalyzerBackend` accepts a list of `AnalysisRequest` and returns
`BatchResult`, with one `AnalysisOutcome` per request. A quota stop may return a
partial batch. Implement `close()` to release model/client resources. The worker
validates outcome identities, duplicates, and statuses. Backends are initialized
once per reused Python worker and cached by backend/model identity.

The Groq adapter lives under `tagging/backends/`; deterministic cleaning and
splitting stay under `chunking/`. A future Transformers adapter can tokenize a
whole request list and run native model batches behind the same interface:

```bash
TAGGING_BACKEND=my_adapter:TransformersBackend \
TAGGING_MODEL=my-model \
TAGGING_ANALYSIS_VERSION=my-model-v1 \
./scripts/run_tagging.sh --items-per-task 8
```

Install custom adapters and their dependencies in `VENV_DIR` before packaging.
No Transformers model or GPU resource configuration is included in this change.
GPU scheduling and memory limits must be configured when adopting such a backend.

Chunk identity includes `chunking_version`, covering cleaning, splitting, and
eligibility rules. Analysis identity also includes backend, model, and
`analysis_version`. Change versions when those behaviors change. Completed
materializations are skipped; article text is assumed immutable after ingestion.
The storage API rejects incompatible content if explicitly rematerialized under
the same version, including missing/extra indexes and eligibility changes.

## Validation

```bash
.venv/bin/python -m unittest discover -s tests -v
WARC_TEST_DSN='dbname=disposable_test_db user=tester' \
  .venv/bin/python -m unittest tests.test_processing -v
.venv/bin/python -m tests.processing_spark_smoke
```

The smoke test runs three local Spark chunking jobs and three fake-backend tagging
jobs. It needs Java and local socket access, but consumes no Groq quota and writes
no article data. PostgreSQL tests create and drop isolated schemas in the supplied
test database. Full YARN validation requires the real cluster and worker network
access to the selected model provider.
