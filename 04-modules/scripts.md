# Module: `scripts/`

[Modules](README.md) · [Docs home](../README.md) · Commands in context: [08 — Deployment and running](../08-deployment-and-running.md)

---

> **Code baseline.** `master` at `d794b8c`. Differences on `airflow_migration` at
> `2769828` are marked **Branch**. Statements are *Verified in code* unless labelled
> otherwise.

Run every command from the project root. The current command for each task on each ref
is in [12 §5.4](../12-airflow-and-python-migration.md#54-command-changes-after-cutover).

## 1. Stage launchers

| Script | Runs | Branch |
|---|---|---|
| `scripts/run_ingestion.sh` | `run_processing.sh ingest` | Deleted |
| `scripts/run_chunking.sh` | `run_processing.sh chunk` | Deleted |
| `scripts/run_tagging.sh` | `run_processing.sh tag` | Deleted |
| `scripts/run_embeddings.sh` | `run_processing.sh embed` | Deleted |
| [scripts/run_processing.sh](../../scripts/run_processing.sh) | The real launcher | Deleted; replaced by `jobs/launcher.py` |

The steps are in [03 §4](../03-cluster-design.md#4-job-lifecycle) and
[03 §5](../03-cluster-design.md#5-code-environment-and-model-distribution).

## 2. Spark entry points

`scripts/ingest.py`, `scripts/chunk.py`, `scripts/tag.py`, and `scripts/embed.py` each call
one stage's `main`. `spark-submit` needs a real `.py` file, so these stay after the
migration. Do not run them directly.

## 3. Setup

| Script | Does | Notes |
|---|---|---|
| [scripts/setup_embedding_onnx_env.sh](../../scripts/setup_embedding_onnx_env.sh) | Creates the embedding venv, installs `requirements/embedding-worker-onnx.txt` from PyPI, runs `pip check` | Fails if `torch` or `sentence_transformers` is present. Ignores local pip configuration |
| [scripts/download_quantized_embedding_model.sh](../../scripts/download_quantized_embedding_model.sh) | Combines the base model files with the pinned INT8 graph | The graph must be at least 500,000,000 bytes and must load. Resumes a partial download |
| [scripts/download_quantized_reranker_model.sh](../../scripts/download_quantized_reranker_model.sh) | Downloads the pinned reranker | Checks the SHA-256 and the architecture, then runs a small ranking test |

All three are still Bash on the branch (Phase 3).

## 4. Inspection and export

| Script | Does |
|---|---|
| `scripts/inspect_db.sh` | Runs `db.inspect`: row counts for 9 tables, checkpoints, the last 10 runs and failed files, and sample rows. Chunk, analysis, and embedding tables are not included. **Branch:** deleted; run `python -m db.inspect` |
| [scripts/export_csv.sh](../../scripts/export_csv.sh) | Exports 9 tables to CSV. It writes to a staging folder first, then moves the files, prints row counts, and copies them to the shared folder. Arguments: `[csv_dir] [dest_dir]`. **Branch:** `python -m scripts.export_csv` |
| [scripts/watch_embeddings.py](../../scripts/watch_embeddings.py) | Read-only embedding progress. Prints `eligible`, `embedded`, `remaining`, and `corpus_pending`, and stops at zero |
| `scripts/watch_embeddings.sh` | Runs the monitor in the embedding venv. **Branch:** deleted; run `python -m scripts.watch_embeddings` |

**`watch_embeddings` flags:**

| Flag | Default | Purpose |
|---|---|---|
| `--interval` | `10` | Seconds between checks |
| `--once` | Off | Print one snapshot and exit |
| `--run-limit` | None | The `--embedding-limit` of this run. Needs `--starting-embedded` |
| `--starting-embedded` | None | The embedded count before a limited run began |
| `--model` | `EMBEDDING_MODEL` | Embedding model |
| `--max-length` | `EMBEDDING_MAX_LENGTH` | Token limit |
| `--chunking-version` | `CHUNKING_VERSION` | Chunking version |

## 5. Query tools

| Script | Does |
|---|---|
| [scripts/ask.py](../../scripts/ask.py) | Answers one question. See [retrieval.md](retrieval.md) |
| [scripts/vector_search_test.py](../../scripts/vector_search_test.py) | Embeds a query and prints the top matches, with no LLM and no reranker. Run it in the embedding venv |

`vector_search_test.py` takes `--top-k` or `-k` (default 5), `--chunking-version`,
`--embedding-model`, and `--embedding-version`. With no filters, it uses the most common
vector configuration in the database. Unlike the RAG query, it does not filter on
`eligible`.

## 6. Modal tools

These are investigation tools. They do not change the pipeline. Background is in
[archive/embedding-runtime-consistency.md](../archive/embedding-runtime-consistency.md).
The `modal` package is not in any requirements file.

| Script | Does | Writes |
|---|---|---|
| [scripts/modal_embedding_app.py](../../scripts/modal_embedding_app.py) | Deploys the embedding endpoint | Nothing in the database |
| [scripts/modal_embedding_compare.py](../../scripts/modal_embedding_compare.py) | Compares Modal vectors with stored vectors | Read-only |
| [scripts/modal_embedding_parity.py](../../scripts/modal_embedding_parity.py) | Compares Modal vectors with the local model; does not use the database | Optional JSON |
| [scripts/modal_pin_avx2.py](../../scripts/modal_pin_avx2.py) | Replaces the single warm container until it lands on an AVX2-only host | Nothing in the database. Run it before a re-embed, because stopping a container cancels active requests |
| [scripts/modal_ort_build.py](../../scripts/modal_ort_build.py) | Builds an AVX2-only ONNX Runtime 1.30.0 wheel | The `warc-ort-build` Modal volume |

**Endpoint.** The app is named `warc-embedding-parity`. It uses Python 3.12, 2 CPUs,
4,096 MB of memory, up to 4 concurrent requests, and Modal proxy authentication. Its
ONNX Runtime is the stock build (`stock-1.30.0`).

| Route | Purpose |
|---|---|
| `GET /health` | Status, model, dimension, ONNX build |
| `GET /v1/models` | Model list |
| `GET /debug` | Host and CPU details |
| `POST /v1/embeddings` | Vectors for `input`, as a string or a list of strings. Only 1,024 dimensions and `float` encoding are accepted |

Container settings are in [07 §9](../07-configuration.md#9-modal-settings).

**`modal_embedding_compare.py` flags:**

| Flag | Default | Purpose |
|---|---|---|
| `--endpoint-url` | Required | Endpoint URL |
| `--token` | `MODAL_PROXY_TOKEN` | Proxy token |
| `--model` | `MODAL_EMBEDDING_MODEL`, then `bge-m3-int8` | Model name sent to Modal |
| `--embedding-model` | Project default | Stored model to compare with |
| `--embedding-version` | `EMBEDDING_VERSION` | Stored version to compare with |
| `--languages` | `en,fr,ar` | Languages to sample |
| `--per-language` | `2` | Chunks per language |
| `--limit` | `8` | Maximum chunks |
| `--threshold` | `0.999` | Minimum cosine similarity |
| `--timeout` | `300` | Request timeout in seconds |
| `--no-local-baseline` | Off | Skip the local baseline |
| `--no-pipeline-check` | Off | Skip the pipeline check |

**`modal_embedding_parity.py` flags:**

| Flag | Default | Purpose |
|---|---|---|
| `--endpoint-url` | Required | Endpoint URL |
| `--token` | `MODAL_PROXY_TOKEN` | Proxy token |
| `--model` | `bge-m3-int8` | Model name sent to Modal |
| `--texts-file` | Built-in texts | JSON list of strings |
| `--threshold` | `1 - 1e-7` | Minimum cosine similarity |
| `--max-abs-tolerance` | `1e-7` | Largest allowed difference in one value |
| `--no-local` | Off | Skip the local comparison |
| `--timeout` | `600` | Request timeout in seconds |
| `--json-out` | None | File for the full response and metrics |

**`modal_pin_avx2.py` flags:**

| Flag | Default | Purpose |
|---|---|---|
| `--endpoint-url` | `MODAL_EMBEDDING_ENDPOINT`, then a built-in URL | Endpoint URL |
| `--region` | `MODAL_REGION`, then `us-east` | Region for deployment |
| `--no-deploy` | Off | Use an app that is already deployed |
| `--max-attempts` | `8` | Attempts to reach an AVX2 host |
| `--debug-attempts` | `60` | Polls of `/debug` |
| `--debug-delay` | `5` | Seconds between polls |
| `--request-timeout` | `300` | Request timeout in seconds |
| `--settle` | `2` | Seconds to wait after a change |

## 7. Ingestion operations

| Script | Page |
|---|---|
| [scripts/supervise_ingest.py](../../scripts/supervise_ingest.py) | [13 §4](../13-ingestion-operations.md#4-ingest-supervisor) |
| [scripts/tg_alert.py](../../scripts/tg_alert.py) | [13 §1](../13-ingestion-operations.md#1-what-changed). Commands: `send`, `inbox`, `ack`, `wait-reply` |
| `scripts/ops_watchdog.py` (branch only) | [13 §5](../13-ingestion-operations.md#5-ops-watchdog) |

## 8. Airflow files

| File | Purpose |
|---|---|
| `airflow/dags/airflow_smoke_test.py` | Deployment check. See [12 §3](../12-airflow-and-python-migration.md#3-airflow-deployment-today) |
| `airflow/dags/ingestion.py` | Broken on `master`. **Branch:** the `ingestion_pipeline` DAG ([12 §4.1](../12-airflow-and-python-migration.md#41-what-exists-on-the-branch)) |
| `airflow/systemd/airflow-api-server.service` | API server and web UI unit |
| `airflow/systemd/airflow-dag-processor.service` | DAG processor unit |
| `airflow/systemd/airflow-scheduler.service` | Scheduler unit |
| `airflow/systemd/install.sh` | Copies the units to `UNIT_DIR` (default `/etc/systemd/system`) and reloads systemd |
