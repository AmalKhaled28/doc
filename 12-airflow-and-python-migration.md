# 12 — Airflow and the Python Migration

[Docs home](README.md) · Related: [13 — Ingestion operations](13-ingestion-operations.md) · [14 — Article metadata and deduplication](14-article-metadata-and-deduplication.md)

---

> **Code baseline.** This document reads two remote refs of the team repository:
> `origin/master` at `d794b8c` and `origin/airflow_migration` at `e668b69`
> (2026-10-29). Files that exist only on the branch are written as plain code text, not
> as links. The migration is in progress, so check both refs before relying on a detail.

## 1. Status at a glance

The team is replacing the Bash launchers with Python and moving orchestration to Apache
Airflow. The pipeline stages themselves do not change. Today the migration has reached
Step 1.2 of Phase 1, and no stage runs through Airflow or through the new launcher.

| Item | State | Where | Confidence |
|---|---|---|---|
| Airflow services (API server, DAG processor, scheduler) | Installed and running on the master | [airflow/systemd/](../airflow/systemd/), [archive/airflow-migration.md](archive/airflow-migration.md) | Unit files: Verified in code. Running: Recorded |
| Smoke-test DAG | Exists, manual trigger only | [airflow/dags/airflow_smoke_test.py](../airflow/dags/airflow_smoke_test.py) | Verified in code |
| Ingestion DAG file | Broken: the file stops after `@task` | [airflow/dags/ingestion.py](../airflow/dags/ingestion.py) | Verified in code |
| `news_pipeline` and `news_export` DAGs | Not written | [§4](#4-target-pipeline) | Proposed (not implemented) |
| Python launcher | Bootstrap only: it cannot submit a job yet | `jobs/launcher.py` (branch only) | Verified in code |
| Shell launchers | Still the only way to run a stage | [scripts/run_processing.sh](../scripts/run_processing.sh) | Verified in code |
| Airflow metadata on PostgreSQL | Not done: metadata is on SQLite | [archive/airflow-migration.md](archive/airflow-migration.md) Phase 4 | Recorded |

## 2. Decisions the team has recorded

These come from the "Decisions already made" section of
[archive/airflow-migration.md](archive/airflow-migration.md). *(Recorded)*

| Decision | Detail |
|---|---|
| Orchestrator | Keep Airflow 3.3.2; do not switch to Dagster or Prefect |
| Scope of the shell migration | All shell scripts, including one-off model download and setup scripts |
| Pipeline shape | One DAG covering `ingest → chunk → tag → embed`. Export stays out of the chain |
| Recovery | Airflow scheduling and retries replace `scripts/supervise_ingest.py` |
| Airflow metadata | Moves to PostgreSQL |
| Cutover style | Hard cutover: each `.sh` file is deleted in the change that adds its replacement |

`scripts/{ingest,chunk,tag,embed}.py` stay as they are, because `spark-submit` needs a
real `.py` file as its script argument. The plan changes the glue and the operations
around Spark, not Spark or the pipeline code.

The plan covers neither Modal nor the ops watchdog. For Modal, the `embed` task in the
plan is still a Spark stage, so the open question in
[FILL-IN.md](FILL-IN.md) (P01) still stands. For the watchdog, see
[13 §8](13-ingestion-operations.md#8-what-happens-at-cutover).

## 3. Airflow deployment today

**Level 1: how the services fit together**

![Airflow services](diagrams/12-airflow-services.svg)

*Source: [diagrams/src/12-airflow-services.mmd](diagrams/src/12-airflow-services.mmd)*

The DAG processor reads the DAG files into the metadata database, and the scheduler
starts task runs from there. Operators reach the same database through the API server,
which also serves the web UI. This component split is standard Airflow 3 behavior
*(Inferred)*.

| Setting | Value | Confidence |
|---|---|---|
| Airflow version | 3.3.2 | Recorded |
| Services | `airflow-api-server`, `airflow-dag-processor`, `airflow-scheduler` (systemd) | Verified in code |
| Run as | User and group `spark-master` | Verified in code |
| `AIRFLOW_HOME` | `/home/spark-master/airflow` | Verified in code |
| Airflow virtual environment | `/home/spark-master/airflow-venv` | Verified in code |
| Extra configuration | `/home/spark-master/airflow/airflow.env` (required by `install.sh`; not in the repository) | Verified in code |
| Metadata database | SQLite, moving to a separate `airflow` database on PostgreSQL | Recorded |
| Web UI port | `8080` | Inferred (Airflow default; neither the unit file nor the repository sets it, and `airflow.env` could override it) |

**Install the units** (run on the master, after creating `airflow.env`):

```bash
bash airflow/systemd/install.sh
sudo systemctl enable --now airflow-scheduler airflow-dag-processor airflow-api-server
```

**Check the deployment** with the smoke test, which starts no Spark job and takes no
coordinator lock:

```bash
airflow dags test airflow_smoke_test
```

It runs four tasks: a start task, an `environment` task (tools, `.venv`, the embedding
model, HDFS, PostgreSQL, a running YARN node), a dry run of
`scripts/run_ingestion.sh`, and a finish task. *(Verified in code)*

## 4. Target pipeline

**Level 2: the planned `news_pipeline` DAG**

![news_pipeline DAG](diagrams/12-news-pipeline.svg)

*Source: [diagrams/src/12-news-pipeline.mmd](diagrams/src/12-news-pipeline.mmd)*

An operator starts a run from a form in the Airflow UI. The probe checks Common Crawl
first, so retries during an outage do not load the cluster, and then each stage runs as
its own task. Every grey node is **Proposed (not implemented)**.

| Part | Plan |
|---|---|
| Stage tasks | Thin TaskFlow functions that call `python -m jobs.launcher <stage>`. Launch logic stays out of the DAG file |
| Run parameters | `year`, `month`, `num_files`, `max_accepted_articles`, `max_accepted_per_file` |
| Ingest retries | `retries=3`, with a `retry_delay` of a few minutes |
| Exit code 2 | Logged as "sources exhausted before quota", so that it does not look like a crash |
| `news_export` | Separate DAG, manual trigger only, runs `python -m scripts.export_csv` |
| Logs | Progress bars stay off (`NO_PROGRESS=1`) because they are unreadable in the UI |

The plan does not give a schedule for `news_pipeline`. Schedule:
[[FILL: schedule of the news_pipeline DAG (hourly cycle or manual trigger)]].

## 5. Shell to Python

### 5.1 Launcher progress

**Level 2: what `jobs/launcher.py` does so far**

![Launcher progress](diagrams/12-launcher-progress.svg)

*Source: [diagrams/src/12-launcher-progress.mmd](diagrams/src/12-launcher-progress.mmd)*

Blue steps exist in the branch and grey steps do not. After choosing the venv, the
launcher stops without running anything. *(Verified in code: `jobs/launcher.py`, branch
only)*

The launcher reads `VENV_DIR` for the main environment. For `embed` it reads
`EMBEDDING_VENV_DIR`, then `EMBEDDING_ONNX_VENV_DIR`, then falls back to
`.venv-embedding-onnx`. It records `SPARK_WARC_EXPORTED_ENV_KEYS` before it loads
`.env`, and it uses `SPARK_WARC_LAUNCHER_STARTED` to mark the second start.

| Input | Exit code |
|---|---|
| No stage given | 1 |
| Unknown stage | 2 |
| Main or embedding venv missing | 1 |
| Valid stage | 0, with no work done (see [§8](#8-known-issues)) |

### 5.2 Work order

**Level 2: migration phases and their dependencies**

![Migration phases](diagrams/12-migration-phases.svg)

*Source: [diagrams/src/12-migration-phases.mmd](diagrams/src/12-migration-phases.mmd)*

The launcher and the Airflow database must both land before the DAGs, and the supervisor
is retired only after the DAGs have proved themselves on real runs. Utilities and setup
scripts are independent, so they can fill waiting time. Blue is in progress; grey has not
started.

| Phase | Size | Depends on |
|---|---|---|
| 1. Launcher port | About 2–3 days | None |
| 2. Utilities | About ½ day | None |
| 3. Setup scripts | About ½ day | None |
| 4. Airflow metadata database | About 1 hour | None; do it before phase 5 |
| 5. DAGs | About 1 day | 1 and 4 |
| 6. Retire the supervisor | Tiny | 5, proven in production |
| 7. Docs and tests | Ongoing | All |

*(Recorded: [archive/airflow-migration.md](archive/airflow-migration.md))*

### 5.3 Which team document covers what

The two team documents number their phases differently.

| Work | [Migration plan](archive/airflow-migration.md) | [Step guide](archive/shell-to-python-step-by-step.md) |
|---|---|---|
| Baseline capture | Not covered | Phase 0 |
| Launcher | Phase 1 | Phase 1 (Steps 1.1–1.16) |
| Utilities | Phase 2 | Phase 2 |
| Setup and downloaders | Phase 3 | Phase 3 |
| Airflow metadata database | Phase 4 | Not covered |
| DAGs | Phase 5 | Not covered |
| Retire the supervisor | Phase 6 | Not covered |
| Docs and closeout | Phase 7 | Phase 4 |

### 5.4 Command changes after cutover

Use the left column until the cutover (Step 1.16). Nothing in the right column works yet
for a real run.

| Today | After cutover |
|---|---|
| `./scripts/run_ingestion.sh …` | `python -m jobs.launcher ingest …` |
| `./scripts/run_chunking.sh …` | `python -m jobs.launcher chunk …` |
| `./scripts/run_tagging.sh …` | `python -m jobs.launcher tag …` |
| `./scripts/run_embeddings.sh …` | `python -m jobs.launcher embed …` |
| `./scripts/export_csv.sh` | `python -m scripts.export_csv` |
| `./scripts/inspect_db.sh` | `python -m db.inspect` |
| `./scripts/watch_embeddings.sh` | `python -m scripts.watch_embeddings` |
| `scripts/supervise_ingest.py` | `news_pipeline` retries in Airflow |

## 6. Risks the team has named

| Risk | Phase | Why it matters |
|---|---|---|
| Fingerprint or cache drift | 1 | A hash difference between Bash and Python causes needless repacks or, at worst, a mismatch between worker and runtime |
| Export during an active run | 2 | Today's export captures only committed rows; the port must keep that behavior |
| Airflow restart | 4 | Scheduler downtime builds a backlog, so do it in a quiet window with no DAG run in flight |
| Exit code 2 | 5 | If Airflow treats it as a plain failure, retries run against a manifest that is already exhausted |

*(Recorded: [archive/airflow-migration.md](archive/airflow-migration.md))*

## 7. Out of scope for the migration

The plan excludes pipeline and model changes, anything in `retrieval/`, Airflow
authentication hardening, Celery or Kubernetes executors, and high-availability setups.
*(Recorded)*

## 8. Known issues

These are recorded only, not fixed. Evidence is from reading the code; nothing here was
executed. They move to the Known Issues table when document 11 is written.

| Issue | Evidence | Effect | Confidence |
|---|---|---|---|
| Launcher exits 0 for a valid stage and does nothing | `main()` in `jobs/launcher.py` ends after the stage lookup. Step 1.1 of the guide asks for a clear "not implemented yet" error | A caller, such as a DAG, would see success with no work done | Verified in code |
| `airflow/dags/ingestion.py` is truncated | The file ends after `@task` and reuses `dag_id="airflow_smoke_test"` | DAG parsing fails for this file. The team plans to delete it in Phase 5 | Verified in code; also Recorded in the plan |
| Unknown stage prints a message | The launcher prints an error to stderr; Step 1.1 says "exit 2 with no output" | Differs from the guide's target | Verified in code |
| Step guide references are missing | `shell-to-python-migration.md` and `.opencode/plan/probe_launcher.py` are not tracked on either ref. Steps 0.1 and 1.2 depend on them. The "empty `scripts/run_processing.py`" in the guide is not tracked either. Location: [[FILL: where shell-to-python-migration.md and probe_launcher.py are kept]] | Nobody without those files can follow Phase 0 | Verified in code (absence) |
| Venv re-execution can be skipped | The launcher compares `os.path.realpath` of the venv Python with `sys.executable`. A venv Python is often a symlink to the base interpreter | Started from that base interpreter, the launcher can stay outside the venv | Inferred |
