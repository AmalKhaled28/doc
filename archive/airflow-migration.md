# Airflow migration plan: shell scripts → Python

**Audience:** a developer handling this pipeline for the first time.
**Size:** roughly 1–2 weeks of focused work for one engineer.

---

## Part 0 — Background: how the project runs today

Before touching anything, understand the pieces. If any of this is new, read the
relevant section twice before starting.

### The pipeline itself (already Python, don't touch)

The product of this project is a searchable database of news articles. Article pages
arrive as raw HTML inside large compressed archives (WARC files) downloaded from
Common Crawl. The work is split into four **stages**, and each stage mutates documents
in turn:

| Stage | Program | What it does |
|---|---|---|
| `ingest` | `ingestion/pipeline.py` | Picks WARC files from a Common Crawl monthly index, extracts articles, filters by language/length, writes them into PostgreSQL. |
| `chunk` | `chunking/pipeline.py` | Splits each article's clean text into overlapping chunks sized for retrieval. |
| `tag` | `tagging/pipeline.py` | Sends each chunk to an LLM (Groq) that assigns structured labels/metadata. |
| `embed` | `embeddings/pipeline.py` | Converts each chunk into a vector (embedding) so similar text can be found by math. |
| `export` | (a psql script, today `scripts/export_csv.sh`) | Debug helper: dumps 9 database tables to CSV files. Not product-critical. |

The stages have to happen in roughly this order (you cannot embed a chunk that has
not been tagged, etc.), but in practice each stage is a **batch job** that grabs all
pending work, does it, and exits. Nothing runs forever; nothing is a server.

The stage code runs on an **Apache Spark** cluster. Spark is a framework that takes a
Python program and runs parts of it in parallel across several machines. You will
interact with exactly three Spark things:

- `spark-submit` — the command that launches a program on the cluster. You hand it a
  `.py` file and config; it distributes workers, runs, then exits.
- **YARN** — the cluster's resource manager; the thing that decides which machine runs
  which piece.
- **HDFS** — the cluster's shared filesystem, used here as a cache for files that every
  worker needs (see "why we package a whole venv" below).

Those three are external systems. The migration does not change anything about them.
What it changes is all the **glue code** — the bash scripts that decide what to run,
where, and with which settings.

### Why the scripts exist, and why there are so many

To run a stage you cannot just call `python chunking/pipeline.py`. The workers on the
cluster are separate computers; they do not have the project's dependencies installed.
So before each run, the project:

1. **Loads configuration** from a `.env` file at the project root (KEY=VALUE lines,
   Git-ignored) — this is done by `jobs/environment.py`, an existing piece of Python
   executed by the bash scripts.
2. **Picks a virtual environment.** A "venv" is a private, disposable copy of Python
   plus specific package versions, ending in a directory like `.venv/bin/python`. This
   project has two: `.venv` (the main one) and `.venv-embedding-onnx` (a lean one for
   the embed stage only, which complains loudly if heavy packages leak into it).
3. **Zips the project source** (`ingestion db chunking tagging embeddings jobs`) so it
   can be shipped to workers with the run (`--py-files` in Spark terms).
4. **Packs the whole venv as a tarball** (`venv pack`). Spark has a feature where you
   provide archives with `--archives environment.tar.gz#environment`, and workers
   unpack them at `/environment/...` before the job. `scripts/run_processing.sh` uses
   a hash (a "fingerprint": a sha256 sum of everything that affects the environment)
   as part of the tarball filename so it only re-packs when dependencies change.
5. **Uploads the archive to HDFS** so workers pull it from the cluster's filesystem
   (fast, shared) rather than from the master over SSH-like access each run. Same for
   the embedding model files. Every upload uses "write to a temp name, then rename"
   so a crash never leaves a half-uploaded file at the final name.
6. **Builds the `spark-submit` command** — a fairly long argument list (`--master yarn`,
   memory sizes, environment variables to keep each worker to a single thread, and so
   on). One is chosen to point at the right worker venv; others keep memory limits sane.

All of the above is bash. Every stage call goes through `scripts/run_processing.sh`;
`scripts/run_ingestion.sh` and friends are 3-line files that just call it with `ingest`
or `chunk`, etc., as a first argument.

### Terminology you'll hit in this doc

- **Dry run** — a *dress rehearsal*. "Do everything a run would do to decide what it
  would do (read config, walk the file manifest, validate flags) **but produce no
  side effects**: no Spark job, no database writes, no archive uploads. It prints what
  it *would* have done and exits. Used to check settings without the 10-minute wait.
- **Fingerprint / content hash** — a sha256 digest computed over a set of files. If the
  digest is the same as yesterday's, the underlying bytes are the same, so reuse the
  cached artifact instead of rebuilding.
- **Exit code** — every program tells its caller "how it went" using a number:
  0 = success, anything else = something's wrong. This pipeline uses 0 = success,
  1 = ordinary failure, 2 = "ran fine but the requested quota could not be met because
  the source files ran out". Tests and Airflow respond differently to the three.
- **XCom** — Airflow's mechanism for tasks to hand small values (a run-id, a row
  count) to other tasks, visible in the UI.
- **DAG** — "Directed Acyclic Graph" — Airflow's word for a named pipeline made of
  ordered tasks. Shown in the UI as boxes and arrows.
- **Dag file** — a Python file in `airflow/dags/` defining a DAG. Airflow's *scheduler*
  reads these files automatically.

### Airflow status in this repo

Airflow is already installed and running (three systemd services:
`airflow-api-server`, `airflow-dag-processor`, `airflow-scheduler`) with its web UI
served by the API server. Details in `airflow/systemd/install.sh`. Two problems:

- There is only a smoke-test DAG (`airflow/dags/airflow_smoke_test.py`).
- The file `airflow/dags/ingestion.py` is **broken** — it is a truncated copy of the
  smoke test that ends mid-file, so DAG parsing fails. It gets deleted in Phase 5.

Airflow's own bookkeeping (metadata DB) is currently SQLite. Fine for one user; migrate
it to the existing PostgreSQL server in Phase 4 for headroom.

---

## Part 1 — The actual work

### Decisions already made (do not relitigate)

- Keep Airflow 3.3.2; do not switch to Dagster/Prefect.
- Migrate ALL shell scripts, including the one-off model download/setup scripts.
- One DAG covering `ingest → chunk → tag → embed`. Export stays out of the operative
  chain — it is a debug tool, triggered manually if needed.
- Scheduler + retries replace `scripts/supervise_ingest.py`.
- Airflow's metadata DB moves to PostgreSQL.
- Hard cutover: delete `.sh` wrappers in the same change that adds their Python
  replacements.

`scripts/{ingest,chunk,tag,embed}.py` are already Python Spark entry points and stay
as-is: `spark-submit` needs a real `.py` file as its script argument. The migration
covers the *glue and operations*, not Spark itself.

### Do the Airflow UI outputs need script changes first?

Keep Airflow's UI. The work needed to *look good in it* is mostly about where the
boundaries sit, not pipeline internals:

1. Per-stage tasks (`ingest → chunk → tag → embed`) so each stage gets its own
   box/state/log/retry button in the UI, instead of one big amber square.
2. Trigger forms via Airflow Params: `num-files`, `max-accepted-articles`,
   `max-accepted-per-file`, `year/month` become run-time parameters instead of edited
   `.env` files.
3. Short structured end-of-task summaries (committed counts, run-id, YARN app id,
   quota state) in the task log, and pushed as XComs where useful. Progress bars stay
   off (`NO_PROGRESS=1`) — they are unreadable in UI logs.
4. Exit-code mapping: ingestion exits `2` when sources cannot fulfill the quota; that
   must read as a human explanation, not an ambiguous red failure.
5. Retries configured in Airflow (`retries`, `retry_delay`) rather than bash loops.

## Phase 1 — Port the launcher (the backbone)

The biggest and most important item; everything else is easy once it lands.

`scripts/run_processing.sh` today, in order:

1. Bootstraps environment from `.env` (via Python `jobs/environment.py` — already fine).
2. Picks the right venv per stage: `.venv` normally, `.venv-embedding-onnx` for `embed`.
3. Exits early with clear errors if a venv is missing.
4. Runs the pipeline module directly for `--help` or dry runs (no packaging).
5. Zips `ingestion db chunking tagging embeddings jobs` into `.build-tmp/`.
6. For `embed`: tars the embedding model dir, keyed by a sha256 of all file contents.
7. Fingerprints the venv (requirements + installed distributions + Python version) and
   caches a `venv_pack` archive under `.build-tmp/` so repeat runs skip packing.
8. Publishes archives to HDFS (`/user/$USER/apps/spark`) with upload→mv→verify, so
   workers do not re-download archives from the master every run.
9. Assembles and runs `spark-submit` with a specific set of `--conf` flags,
   `--archives`, and `--py-files`, pointing at `scripts/<stage>.py`.

**Task:** create `jobs/launcher.py`, run as `python -m jobs.launcher <stage> --flags…`,
replicating the above 1:1. "1:1" means: same flags, same env precedence, same exit
codes (0 ok, 2 quota shortfall, …), same Spark confs, same build-dir caching. Do not
redesign anything — this is a **translation**, not an improvement opportunity. Use
`subprocess` for the external binaries (`hdfs`, `spark-submit`, `venv_pack`); HDFS and
Spark are other people's tools, we only own our glue.

Before writing code: read `scripts/run_processing.sh` top to bottom and annotate each
block with what it actually does (comment in your notes). Then write the Python
side-by-side. Keep the same names where possible (e.g. `HDFS_APPS_DIR`,
`USE_HDFS_CACHE`) so a person can cross-check against the bash version.

**Delete when done:** `scripts/run_processing.sh`, plus `scripts/run_ingestion.sh`,
`scripts/run_chunking.sh`, `scripts/run_tagging.sh`, `scripts/run_embeddings.sh`.

Checklist:

- [ ] `python -m jobs.launcher ingest --dry-run --max-accepted-articles 1000` prints
      the same config summary the shell version did, exit code 0.
- [ ] Same for `chunk`, `tag`, `embed` `--dry-run`.
- [ ] `--help` works for all four stages without a blocking venv check.
- [ ] A real smoke ingest (`--max-accepted-articles 10`) commits articles and prints
      the same final summary (counts, per-file status, resume command).
- [ ] The second run does NOT re-pack the venv (fingerprint cache hit).
- [ ] Missing `.venv` produces the same friendly error the shell version gave.
- [ ] Unit tests cover flag parsing, venv selection, fingerprint stability, and the
      assembled spark-submit argv.

Children of Phase 1 (independent of the phases below; fill wait time with them):

| Phase | Size | Notes |
|---|---|---|
| 2. Utilities | small, ~½ day | Independent of Phase 1 |
| 3. Setup scripts | small, ~½ day | Independent of Phase 1 |

## Phase 2 — Port the utility scripts

| Current | Replace with | Notes |
|---|---|---|
| `scripts/export_csv.sh` | `scripts/export_csv.py` | psycopg2 `COPY … TO STDOUT` per table, same 9 tables / same ORDER BY, staging dir then rename (atomic swap), row-count report, optional copy to the HGFS share. Keep the `csv_dir dest_dir` positional override interface. |
| `scripts/inspect_db.sh` | (delete) | The script just executes `python -m db.inspect`; point docs at that command directly. |
| `scripts/watch_embeddings.sh` | (delete) | The script just executes `python -m scripts.watch_embeddings`; point docs at that command directly. |

Explanations for the unfamiliar:

- **psycopg2** is the standard Python driver for PostgreSQL — a library that lets a
  Python file speak the database's native protocol. `COPY … TO STDOUT` is a Postgres
  statement that streams table rows as text (CSV here) fast; far faster than `SELECT`.
  Today's shell version opens a `psql` command-line session per table; the Python one
  opens a psycopg2 connection per table, or one connection reused across all nine.
- **Atomic swap** — write all rows to a hidden temp name like `.export-XXXXXX/`, verify
  it worked, then `os.replace` the finished files over the previous ones. So a partial
  failure never leaves broken files where users expect clean ones.
- **HGFS** — the VMware "Host-Guest File System" shared folder between this Linux VM
  and its Windows/macOS host. `export_csv.sh` copies the files to
  `/mnt/hgfs/copy_path/warcdb_csv/` so someone on the host machine can open them in
  Excel. Keep this step; it is a real deliverable.

Checklist:

- [ ] `python -m scripts.export_csv` produces the same 9 CSVs in `exports/` with
      byte-identical content to the last shell export (spot-check two).
- [ ] HGFS copy still lands in `/mnt/hgfs/copy_path/warcdb_csv/`.
- [ ] `python -m db.inspect` and `python -m scripts.watch_embeddings` still work.

## Phase 3 — Port the one-off setup scripts

Nobody runs these daily, but "single language" means they go too. These are mostly
sequential `subprocess.run` calls — the easiest ports in the plan.

| Current | Replace with | Notes |
|---|---|---|
| `scripts/setup_embedding_onnx_env.sh` | `scripts/setup_embedding_onnx_env.py` | Same venv creation, pinned requirements install with the same pip flags, `pip check`, and the "must not contain torch" guard (a regression catcher: if a heavy library sneaks into the lean worker env, Spark workers OOM). |
| `scripts/download_quantized_embedding_model.sh` | `scripts/download_quantized_embedding_model.py` | Same pinned ONNX revision (a specific published file version, chosen deliberately and kept constant — do not "upgrade" it), tokenizer/pooling layout, checksum-vs-existing-file logic. |
| `scripts/download_quantized_reranker_model.sh` | `scripts/download_quantized_reranker_model.py` | Preserve resume-if-partially-downloaded behavior if present. |

Checklist:

- [ ] Running the Python versions on a clean directory reproduces the same
      `.venv-embedding-onnx` and `.models/*` contents (verify via the fingerprint /
      `pip check` assertions the scripts already print).

## Phase 4 — Move Airflow's metadata DB to PostgreSQL

**Do this BEFORE creating real DAGs**, so nothing gets registered twice.

What is "Airflow's metadata DB"? Airflow is a long-running service; it needs somewhere
to store "what DAGs exist, which runs happened, which tasks succeeded, when to retry".
That storage is its metadata DB. It currently points at **SQLite** — a single-file
database fine for one writer, but a known ceiling once a scheduler, a dag processor,
and an API server all hit it concurrently. Your machine already runs a real
PostgreSQL server; give Airflow a database on it.

1. Create a dedicated `airflow` database on the existing PG server (not `warcdb`) and
   a role/password for it.
2. Update the Airflow config — the `airflow.cfg` under the Airflow Home
   (`/home/spark-master/airflow/`), or the `airflow.env` env file the systemd units
   load — with `sql_alchemy_conn = postgresql+psycopg2://USER:PASSWORD@HOST/airflow`.
3. Run `airflow db migrate`, then restart the three systemd units
   (`airflow-api-server`, `airflow-dag-processor`, `airflow-scheduler`).
4. Verify the UI loads, the existing smoke-test DAG is visible, and a short run works.

Checklist:

- [ ] `airflow db check` passes against Postgres.
- [ ] All three units show `active (running)` after restart.
- [ ] UI log viewer still shows live logs from a test task run.

## Phase 5 — Build the DAGs

First **delete `airflow/dags/ingestion.py`** — it is a truncated copy of
`airflow_smoke_test.py` that ends mid-file, so DAG parsing fails. It was never a real
DAG.

Then create:

**`news_pipeline`** — the operative DAG.

- Task chain: `probe → ingest → chunk → tag → embed`.
- `probe` checks `data.commoncrawl.org` with a small range request so Airflow retries
  during a real outage do not hammer the cluster (mirrors `supervise_ingest.py`).
- Each stage task is a thin Airflow TaskFlow function running
  `python -m jobs.launcher <stage> …` via subprocess. Launch logic lives in
  `jobs/launcher.py`, not in the DAG file — DAGs stay readable at a glance.
- Parameters render as a trigger form in the UI: `year`, `month`, `num_files`,
  `max_accepted_articles`, `max_accepted_per_file`, plus launcher flags as needed.
- Retries on the ingest step (`retries=3`, `retry_delay` a few minutes) replace
  `supervise_ingest.py`.
- Exit code `2` (sources exhausted) logs a clear "sources exhausted before quota"
  message near the end of the task log; it should not look like a random crash.

**`news_export`** — optional, manual-trigger only (`schedule=None`).

- One task calling `python -m scripts.export_csv` with optional `csv_dir/dest_dir`
  params. Not chained into `news_pipeline`; export is a debug tool.

**Update `airflow_smoke_test.py`** at the end of this phase: dry-run the launcher's
new Python path instead of the shell one.

Checklist:

- [ ] `airflow dags list-import-errors` returns nothing.
- [ ] `news_pipeline` can be triggered from the UI with parameters and shows all four
      stages green for a small quota run.
- [ ] The UI log viewer shows the ingest summary output.
- [ ] Killing an ingest task mid-run and re-running retries sensibly.
- [ ] `news_export` runs end-to-end from a manual UI trigger.

## Phase 6 — Retire `supervise_ingest.py`

Only after Phase 5 has proven out over a few real runs — this is the moment the
scheduler takes over the resume-on-outage job.

- [ ] Confirm a killed/failed ingest in Airflow is retried per config.
- [ ] Delete `scripts/supervise_ingest.py` and its pidfile/logfile convention.
- [ ] Remove or rewrite mentioning docs; point at the DAG instead.

## Phase 7 — Docs and tests

Do these continuously, but note them as their own item.

- Update `README.md` run commands: `python -m jobs.launcher <stage>`, the Python
  export/DB commands, and how to trigger the DAGs from the UI. Remove shell
  invocations.
- Update `docs/processing.md` and `docs/workflows.md` for the new interfaces.
- Unit tests for `jobs/launcher.py` are the important new asset; keep
  `tests/test_launcher.py` focused on it.
- Do **not** touch pipeline logic (`ingestion/`, `chunking/`, `tagging/`,
  `embeddings/`, `retrieval/`, `db/`) — those are already Python and are not part of
  the migration.

---

## Order and sizing

| Phase | Size | Depends on |
|---|---|---|
| 1. Launcher port | largest, ~2–3 days | – |
| 2. Utilities | ~½ day | – |
| 3. Setup scripts | ~½ day | – |
| 4. Airflow DB | ~1 hr | – (do before 5) |
| 5. DAGs | ~1 day | 1, 4 |
| 6. Retire supervisor | tiny | 5 proven in production |
| 7. Docs/tests | ongoing | all |

## Out of scope

- Any change to pipeline internals or model behavior.
- Any change to `retrieval/` / the RAG agent.
- Airflow auth hardening, Celery/Kubernetes executors, HA setups.

## Risks to watch

- **Fingerprint/cache drift (Phase 1).** The venv and model archives are cached by
  content hash. A subtle hash-input difference between the bash and Python versions
  causes useless repacks (annoying but safe) or, worst case, worker/runtime mismatch
  (bad). Test by running the same stage twice and confirming the cache is hit, then
  once with a modified requirements file to confirm invalidation.
- **Export during an active run (Phase 2).** Exporting during an embedding run
  captures only committed rows today; preserve that behavior, do not make it
  transactional.
- **Airflow/DB restart (Phase 4).** Brief scheduler downtime builds up backlog; do it
  in a quiet window and check no DAG runs are mid-flight.
- **Exit 2 semantics (Phase 5).** If Airflow treats "sources exhausted" as a plain
  failure, retries burn pointlessly against an exhausted manifest. Handle this case
  specifically in message and retry logic.

## Command mapping after the migration

| Today | After |
|---|---|
| `./scripts/run_ingestion.sh …` | `python -m jobs.launcher ingest …` |
| `./scripts/run_chunking.sh …` | `python -m jobs.launcher chunk …` |
| `./scripts/run_tagging.sh …` | `python -m jobs.launcher tag …` |
| `./scripts/run_embeddings.sh …` | `python -m jobs.launcher embed …` |
| `./scripts/export_csv.sh` | `python -m scripts.export_csv` |
| `./scripts/inspect_db.sh` | `python -m db.inspect` |
| `./scripts/watch_embeddings.sh` | `python -m scripts.watch_embeddings` |
| manual babysitting via `scripts/supervise_ingest.py` | Airflow DAG `news_pipeline` retries |
