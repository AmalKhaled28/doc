# 08 — Deployment and Running

[Docs home](README.md) · Previous: [07 — Configuration](07-configuration.md) · Next: [09 — Operations](09-operations.md)

---

> **Code baseline.** `master` at `d794b8c`. The commands below are for `master`. On
> `airflow_migration` at `2769828`, use the commands in
> [§2](#2-commands-on-each-ref). Repository commands are *Verified in code*. Nobody ran
> them for this page. Steps marked *Standard* are ordinary tool commands that the
> repository does not contain.

This page goes from prepared machines to a running system. The repository has no
infrastructure-as-code, so it starts from a cluster that already has Java, Hadoop, and
Spark. See [02](02-hardware-and-infrastructure.md).

## 1. Order of work

1. Check the prerequisites.
2. Prepare PostgreSQL.
3. Create the main environment and `.env`.
4. Run a small ingestion.
5. Run chunking.
6. Run tagging.
7. Set up embeddings and run them.
8. Set up the query environment and ask a question.
9. Optionally, install Airflow services.

Each step ends with a check. Do not continue until the check passes.

## 2. Commands on each ref

| Task | `master` | `airflow_migration` |
|---|---|---|
| Ingest | `./scripts/run_ingestion.sh` | `.venv/bin/python -m jobs.launcher ingest` |
| Chunk | `./scripts/run_chunking.sh` | `.venv/bin/python -m jobs.launcher chunk` |
| Tag | `./scripts/run_tagging.sh` | `.venv/bin/python -m jobs.launcher tag` |
| Embed | `./scripts/run_embeddings.sh` | `.venv/bin/python -m jobs.launcher embed` |
| Inspect | `./scripts/inspect_db.sh` | `.venv/bin/python -m db.inspect` |
| Watch embeddings | `./scripts/watch_embeddings.sh` | `.venv-embedding-onnx/bin/python -m scripts.watch_embeddings` |
| Export | `./scripts/export_csv.sh` | `.venv/bin/python -m scripts.export_csv` |

## 3. Prerequisites

| Host | Needs |
|---|---|
| Spark master | Linux, Java, Spark, the Hadoop client (`hdfs`, `yarn`), Python 3 with `venv`, `curl`, `tar`, `sha256sum`, `psql`. On `master`, also `zip` |
| Workers | Java, Hadoop/YARN, and the same Python version and interpreter path as the master |
| PostgreSQL host | PostgreSQL with pgvector |
| Network | [02 §5.1](02-hardware-and-infrastructure.md#51-network-flows) |

**Check** on the master (see [02 §7](02-hardware-and-infrastructure.md#7-commands-to-collect-real-specs)):

```bash
java -version
spark-submit --version
hdfs dfs -test -e / && echo "HDFS ok"
yarn node -list | grep RUNNING
python3 --version
```

All workers must show as `RUNNING`.

`cluster_up.sh` and `cluster_down.sh`, which start the VMs and Hadoop on the Debian host,
are not in the repository. See J01 in [FILL-IN.md](FILL-IN.md).

## 4. PostgreSQL

The database and role must exist before the first run. The application creates its own
tables. *Standard:*

```sql
-- As a PostgreSQL superuser
CREATE ROLE warc_user LOGIN PASSWORD '<secret>';
CREATE DATABASE warcdb OWNER warc_user;
\c warcdb
CREATE EXTENSION IF NOT EXISTS vector;
```

The role must be able to create tables. `init_db` also runs `CREATE EXTENSION IF NOT
EXISTS vector`; creating it as a superuser first avoids a permission error on that step.

**Check** from the master:

```bash
psql -h <host> -U warc_user -d warcdb -c "SELECT extversion FROM pg_extension WHERE extname='vector';"
```

**Expected output:** one row with a version.

## 5. Main environment and `.env`

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements/base.txt
.venv/bin/python -m pip install -r requirements/rag.txt
.venv/bin/python -m pip install -r requirements/tagging-groq.txt
export SPARK_HOME=/opt/spark
export PATH="$SPARK_HOME/bin:$PATH"
cp .env.example .env
```

Edit `.env`: set `PGPASSWORD`, `GROQ_API_KEY`, and the host values. Add other values from
[07](07-configuration.md) as needed. Do not use `pyspark` from pip: the cluster's
`spark-submit` supplies it.

**Check:**

```bash
./scripts/run_ingestion.sh --dry-run --max-accepted-articles 1000
```

**Expected output:** the resolved configuration as JSON, with no packaging and no job.

## 6. First ingestion

```bash
./scripts/run_ingestion.sh --num-files 1 --max-accepted-articles 10 --max-accepted-per-file 10
```

**Expected output:** `Created ingestion run <id>`, a resume command, then
`Run <id>: complete; committed 10/10 new articles`. The first run packs the environment,
so it takes longer.

**Check:** `./scripts/inspect_db.sh` shows 10 more rows in `pages`, and the run as
`complete`.

| Exit code | Meaning | Next step |
|---|---|---|
| 0 | Done | Continue |
| 1 | Failure | Read the error. See [09 §5](09-operations.md#5-troubleshooting) |
| 2 | The selected files could not meet the target | Select more files or lower the target |

## 7. Chunking

```bash
./scripts/run_chunking.sh --document-limit 100
```

**Expected output:** JSON lines with `committed_documents`, and a final line with
`documents`, `stored`, and `filtered`.

## 8. Tagging

Tagging calls a paid API. Start small.

```bash
./scripts/run_tagging.sh --analysis-limit 50
```

**Expected output:** JSON lines with status counts, and a final line with `pending` and
`quota_exhausted`.

| Exit code | Meaning | Next step |
|---|---|---|
| 0 | Done | Continue |
| 1 | Failure, such as a wrong API key | Fix it and rerun |
| 2 | Daily quota, or a wait over 15 minutes | Rerun after the quota resets |

For the free tier, slow it down with `TAG_REQUESTS_PER_MINUTE` and
`TAG_TOKENS_PER_MINUTE`. See [tagging §6](04-modules/tagging.md#6-pacing).

Rerun failed chunks with `./scripts/run_tagging.sh --retry-errors`.

## 9. Embeddings

```bash
bash scripts/setup_embedding_onnx_env.sh
```

**Expected output:** `Ready: ONNX Runtime <version> Transformers <version>`.

The next script needs six base files in `.models/bge-m3`: `config.json`, `modules.json`,
`tokenizer.json`, `tokenizer_config.json`, `sentencepiece.bpe.model`, and
`1_Pooling/config.json`. No repository script downloads them. See M01 in
[FILL-IN.md](FILL-IN.md).

```bash
bash scripts/download_quantized_embedding_model.sh
```

**Expected output:** `Quantized model ready: <path>`.

Before you embed, check that every worker has the same CPU instruction set. See the
warning in [02 §3.1](02-hardware-and-infrastructure.md#31-known-and-unknown-specifications).

```bash
./scripts/run_embeddings.sh --embedding-limit 100
./scripts/watch_embeddings.sh --once
```

**Expected output:** a line with `eligible=`, `embedded=`, `remaining=`, and
`corpus_pending=`. To follow a limited run from another terminal, note the `embedded` value
first, then:

```bash
./scripts/watch_embeddings.sh --run-limit 100 --starting-embedded <embedded before the run>
```

## 10. Query environment

```bash
python3 -m venv .venv-rag
.venv-rag/bin/python -m pip install -r requirements/rag-agent.txt
bash scripts/download_quantized_reranker_model.sh
.venv-rag/bin/python scripts/ask.py "What happened, and why did it matter?"
```

**Expected output:** the answer, `Sources:`, then `Retrieved evidence:`. Exit code 0 means
a sufficient answer, and 1 means insufficient evidence.

With only a few vectors, insufficient evidence is a normal result. To check search without
an LLM:

```bash
.venv-embedding-onnx/bin/python scripts/vector_search_test.py "your query" -k 5
```

To use Gemini, set `RAG_MODEL=gemini-3.5-flash-lite` and `GEMINI_API_KEY`.

## 11. Full runs

Remove the limits once the small run works:

```bash
./scripts/run_ingestion.sh --num-files all
./scripts/run_chunking.sh
./scripts/run_tagging.sh
./scripts/run_embeddings.sh
```

A full month is about 2.4 million articles and 0.35 TiB of WARC data
([13 §2](13-ingestion-operations.md#2-scale-of-real-runs)). Size the database first with
[02 §6.2](02-hardware-and-infrastructure.md#62-storage). Long ingestion is usually run
with the [supervisor](13-ingestion-operations.md#4-ingest-supervisor).

Stages can run at the same time, but YARN capacity is the limit. See
[02 §4](02-hardware-and-infrastructure.md#4-yarn-container-budget-per-stage).

To stop a stage, press Ctrl+C once. Committed work is kept, and processing stages finish
their active wave first. To continue, rerun the same command. For ingestion, use the
printed resume command.

## 12. Airflow

Run on the master. The units expect `/home/spark-master/airflow-venv` and a
`/home/spark-master/airflow/airflow.env` file, and neither is created by the repository.

```bash
bash airflow/systemd/install.sh
sudo systemctl enable --now airflow-scheduler airflow-dag-processor airflow-api-server
airflow dags test airflow_smoke_test
```

**Expected output:** the task `finish` prints `airflow-smoke-test-ok`.

The smoke test uses the fixed path `/home/spark-master/rag_news_project/spark_warc_v2`. On
`master`, no DAG runs the pipeline. On the branch, see
[12 §4.1](12-airflow-and-python-migration.md#41-what-exists-on-the-branch).

## 13. Tests

See [tests.md](04-modules/tests.md).
