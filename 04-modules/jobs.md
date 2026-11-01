# Module: `jobs/`

[Modules](README.md) · [Docs home](../README.md) · Spark settings: [03 §3.3](../03-cluster-design.md#33-spark-settings-applied-by-the-repository)

---

> **Code baseline.** `master` at `d794b8c`. Differences on `airflow_migration` at
> `2769828` are marked **Branch**. Statements are *Verified in code* unless labelled
> otherwise.

## 1. Purpose

Shared runtime for the Spark stages: configuration, session setup, task batching, and
`.env` loading.

## 2. Files

| File | Responsibility |
|---|---|
| [jobs/runtime.py](../../jobs/runtime.py) | `config`, `start_spark`, `processing_session`, `cli`, `report` |
| [jobs/batching.py](../../jobs/batching.py) | Task packing and execution |
| [jobs/environment.py](../../jobs/environment.py) | `.env` loading |
| [jobs/launch_config.py](../../jobs/launch_config.py) | Resolves executor count and memory for the launcher |
| [jobs/validation.py](../../jobs/validation.py) | Integer validation for settings and flags |

**Branch only:**

| File | Responsibility |
|---|---|
| `jobs/launcher.py` | `python -m jobs.launcher <stage>` and the `launch()` API. See [12 §5.1](../12-airflow-and-python-migration.md#51-launcher-progress) |
| `jobs/artifacts.py` | Project ZIP, venv and model archives, fingerprints, HDFS publishing |
| `jobs/spark.py` | `SparkPlan` and the `spark-submit` command |
| `jobs/errors.py` | `LaunchError` with an exit code |

## 3. Configuration

`jobs.runtime.config(stage, argv)` builds the configuration for `chunk`, `tag`, and
`embed`. Ingestion has its own `load_config`. Every value follows the precedence in
[07 §2](../07-configuration.md#2-how-settings-are-resolved).

`dry_run` stops before any Spark or database work. With it, the stage prints its resolved
configuration as JSON and exits 0.

## 4. Session

`processing_session(stage, cfg)` is a context manager:

1. `init_db` applies the schema.
2. `acquire_lock` takes the stage lock.
3. `start_spark` builds the session.
4. On exit, the session stops and the lock connection closes.

`cli(run)` turns errors into exit codes: `KeyboardInterrupt` gives 130, and any other
exception gives 1, with `FATAL <type>: <message>` on stderr. It removes `GROQ_API_KEY` from
that message. `report` prints one JSON line.

`SPARK_MASTER_URL`, when set, overrides the Spark master. The launchers always use YARN,
so this is for direct or local runs.

## 5. Batching

| Function | Behavior |
|---|---|
| `batches` | Groups items by count and serialized bytes. One item above the byte limit is an error |
| `batch_tasks` | Builds tasks lazily, so pending rows are not held in memory |
| `execute_batch` | One Spark job with one partition per task. Waits for all of them. Used by ingestion |
| `run_waves` | Up to `parallel_tasks` single-task Spark jobs at a time, committing each as it finishes. Used by chunking, tagging, and embeddings |
| `check_result_size` | Raises an error when a result is over its byte limit |

`run_waves` drains the current wave before it raises an error or stops for quota, so
finished work is kept. On the first `KeyboardInterrupt`, it finishes the active wave and
then stops. A second interrupt forces the exit.

## 6. Environment loading

On `master`, a shell launcher calls `jobs/environment.py` first. It records the names of
the variables already set in `SPARK_WARC_EXPORTED_ENV_KEYS`, loads `.env` with
`override=False`, and replaces itself with the real command.

`.env` is parsed as data and never run as shell. A missing file is fine.

**Branch:** `python -m jobs.environment --json` prints only the values that `.env` adds,
and `jobs.launcher` passes them to its child processes.

## 7. Launch configuration

`jobs.launch_config` runs the same parser as the stage, so the launcher rejects bad flags
before packaging. It prints five tab-separated values: executor count, dry-run flag,
driver memory, executor memory, and memory overhead.

Memory values must match `[1-9][0-9]*[kKmMgG]?`. For `embed`, memory comes from
`EMBEDDING_EXECUTOR_MEMORY` and `EMBEDDING_EXECUTOR_MEMORY_OVERHEAD`, then the generic
variables. The defaults are in
[02 §4](../02-hardware-and-infrastructure.md#4-yarn-container-budget-per-stage).
