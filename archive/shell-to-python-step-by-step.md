# Build guide: shell → Python, one step at a time

Companion to `shell-to-python-migration.md` (measured facts, argv, fingerprints).
That file is the reference; this one is the order of work. No implementation
code here on purpose — only what to build, what to run, and when to stop.

**Rules**
- One step = one commit. Run the gate before starting the next step.
- Steps 1.1–1.5 are pure Python and testable without a cluster.
- Never change pipeline behaviour, flag names, Spark confs, or hashes in a step
  whose job is translation.
- If a gate fails, fix it in that step. Do not stack unverified changes.

---

## Phase 0 — Baseline (do once, ~30 min)

### Step 0.1 — Save the current behaviour
- Create `/tmp/launcher-baseline/`.
- Save the dry-run output for all four stages (`run_ingestion.sh --dry-run
  --no-progress --max-accepted-articles 17`, `run_chunking.sh --dry-run`,
  `run_tagging.sh --dry-run`, `run_embeddings.sh --dry-run`).
- Save each `--help` output.
- Save the recorded argv from the fake-`spark-submit` probe
  (`.opencode/plan/probe_launcher.py` writes it as JSON; keep that JSON).
- Save `ls .build-tmp` and the five fingerprints from the reference file §0.4.

**Gate:** four dry runs exit 0; the files exist; fingerprints saved.

### Step 0.2 — Confirm the baseline test suite is green
- Run `tests.test_launcher`, `tests.test_environment`, `tests.test_export_csv`.

**Gate:** 6 tests pass (about 7 s). If not, stop and fix the environment first.

---

## Phase 1 — `jobs/launcher.py`

Work in `jobs/launcher.py`, not `scripts/run_processing.py`. Delete the empty
`scripts/run_processing.py` at cutover, not now.

### Step 1.1 — CLI skeleton and error codes
- Create the module with a `main` and a stage table mapping the four stage names
  to their pipeline modules.
- Implement stage parsing only: no stage → message + exit 1; unknown stage →
  exit 2 with no output.
- For real runs, raise a clear "not implemented yet" error. Do not package or
  submit yet.

**Verify:** `python -m jobs.launcher` → 1; `python -m jobs.launcher bogus` → 2;
`python -m jobs.launcher ingest --help` → still not implemented at this point.
**Done when:** both error paths match the saved baseline.

### Step 1.2 — Environment bootstrap
- Add the two-hop bootstrap described in the reference §1.1: first hop finds and
  execs the bootstrap interpreter, second hop records
  `SPARK_WARC_EXPORTED_ENV_KEYS` **before** loading `.env`, then re-execs the
  stage venv with the private sentinel.
- Keep `SPARK_WARC_EXPORTED_ENV_KEYS` exactly as named; never rewrite it after
  `.env` is loaded.
- Change directory to the project root at the start, like the shell.
- Missing bootstrap interpreter: same message, exit 1.

**Verify:** run from `/tmp`; `.env` values are visible; `--help` still reaches
the "not implemented" path; a dry run with `MAX_ACCEPTED_ARTICLES=11` shows 11.
**Done when:** the module can be launched from any cwd and `.env` is loaded once.

### Step 1.3 — Venv selection
- Implement stage venv choice: embedding variables in the same precedence order
  as the shell, then the two defaults.
- Reject a missing stage interpreter with the shell's message and exit 1.
- Keep the path absolute but do **not** resolve symlinks: `sys.executable` is
  part of the fingerprint, and bash `pwd` is logical.

**Verify:** the embed stage prints/uses `.venv-embedding-onnx`; the other three
use `.venv`; `EMBEDDING_VENV_DIR` overrides; a bad path exits 1.

### Step 1.4 — Help passthrough
- If any flag is `--help` or `-h`, replace the process with the stage module
  (`python -m <module> <flags>`), no packaging, no `.build-tmp`.
- Do this **after** the bootstrap and stage-venv checks, because the current
  behaviour requires both venvs even for help.

**Verify:** all four help outputs match the baseline byte-for-byte; exit 0; no
new files in `.build-tmp`.

### Step 1.5 — Config resolution and dry run
- Resolve launcher settings with the existing `jobs.launch_config` resolver in
  process (the launcher is already in the stage venv).
- Export the executor count like the shell does before any dry run.
- On dry run, exec the stage module with the same flags; no packaging.

**Verify:** all four dry runs match the baseline key-for-key; `DRY_RUN=1` works;
`--no-dry-run` overrides it; invalid memory values fail non-zero.
**Done when:** Phase-1 tests can already pin the JSON output.

### Step 1.6 — Project zip
- Create the temp dir under `.build-tmp`, write the project zip there.
- Package exactly `ingestion db chunking tagging embeddings jobs`; skip
  `__pycache__` and `.pyc`; use POSIX relative names; do not include `scripts/`.
- Do not try to match zip bytes; only contents.

**Verify:** entry count is 34 files (the shell also emits 7 dir entries; that is
irrelevant); `jobs/runtime.py` and `jobs/batching.py` are present; no
`__pycache__`, no `.pyc`, no `scripts/`; size is around 58 KB today.

### Step 1.7 — Venv fingerprint and cache lookup
- Implement the venv hash exactly as in reference §0.4: requirements files for
  the stage, `pyvenv.cfg`, the sorted installed-distribution listing, then
  Python version and executable.
- Use the cached archive if the file exists; only compute the name at this step.

**Verify (the important gate):** on this machine the values must be
`3e93d890…` for ingest, `fa731176…` for chunk/tag, `94b594…` for embed.
Ingest must match the existing `.build-tmp/pyspark_venv-3e93….tar.gz`.
If ingest differs, stop — hashing input or interpreter path is wrong.
**Commit message:** include the three digests.

### Step 1.8 — Venv packing
- If the cache file is absent, run `venv_pack` with the stage interpreter into
  the temp dir, then move it into the cache atomically.
- Print the "Packing worker environment..." line like the shell does.

**Verify:** first run creates the cache file; second run does not repack; a
modified requirements file changes the name.
**Note:** chunk/tag/embed are cold today, so their first run will pack. That is
expected; ingest is the cache-hit case to test.

### Step 1.9 — Embedding model staging
- Resolve the model dir with the shell's precedence; export it.
- Require the ONNX graph and tokenizer; error text should point at the setup and
  download scripts.
- Hash all model files with the same path-inclusive recipe as the shell; cache
  the tarball under the existing naming scheme; pack with `tar` (subprocess).
- Run the four-package import check with the stage interpreter.

**Verify:** the model hash is `29df210b…` on this tree; the cache name uses it;
a second run reuses it; a touched file changes it.
**Note:** no cached archive matches today, so the first embed run packs 564 MB
either way. Do not use it as a parity signal.

### Step 1.10 — HDFS publishing
- Implement availability check, publish (test → mkdir → put to a unique temp name
  → move → verify, with cleanup on failure), and the three call sites: venv,
  model, Spark jars.
- Keep the "empty means default" semantics for `USE_HDFS_CACHE` and keep
  `SPARK_HOME` required only inside this branch.
- Keep the HDFS apps path and the upload suffix identical.

**Verify:** unit-test the command sequence with a recorder; with
`USE_HDFS_CACHE=0` nothing HDFS-related runs; with a stub `hdfs` on `PATH`, the
returned archive spec becomes `hdfs://...#environment`.

### Step 1.11 — Spark jars cache
- Hash `$SPARK_HOME/jars/*.jar` with the same filesystem recipe (absolute paths
  are part of the hash), cache the zip, publish it, and add the archive conf.
- Keep the ordering: jars conf before the non-ingest retry confs.

**Verify:** jars hash is `d977abce…` on this machine and matches the existing
`spark-libs-d977….zip`.

### Step 1.12 — Assemble the submission argv
- Build the argument list in the measured order (reference §0.3): common Spark
  confs, optional jars conf, non-ingest speculation/failure confs, archives,
  py-files, `scripts/<stage>.py`, then user flags untouched.
- Ingest uses `spark-submit` from `PATH`; the other stages require `SPARK_HOME`
  and use its binary.
- Export the PySpark interpreter variables before submitting.

**Verify:** unit-test the full list against the saved baseline argv for all four
stages. Check the two differences explicitly: ingest has no
speculation/maxFailures confs; embed has `1g` overhead and a two-entry archives
spec.

### Step 1.13 — Execute, clean up, propagate exit code
- Run the submission as a child process, wait for it, delete the temp dir in a
  `finally`, and return the child's exit code unchanged.
- Return 130 on interrupt after the child exits.
- Do not replace the process at this point: the shell does not `exec` the final
  submission, and that is what makes the temp-dir cleanup work.

**Verify:** fake submit returning 7 makes the launcher exit 7; no `pack-*` dir
remains after success or failure; the recorded argv equals the baseline for all
four stages.

### Step 1.14 — Rewrite the existing tests
- `tests/test_launcher.py`: invoke the module instead of `bash scripts/…`; keep
  the fake `python` and `spark-submit` shims; keep the packaging assertions.
- `tests/test_environment.py`: same three cases against the module, including
  "run from another directory" and "secret never printed".
- `tests/test_processing.py`: change the two dry-run launcher calls only.
- `tests/package_smoke.py`: last line switches to the module.
- Add unit tests for: unknown stage (2), missing stage (1), venv precedence,
  fingerprint stability, zip contents, argv, exit passthrough.

**Gate:** the four rewritten test files pass; no test still shells out to a
`.sh` file.

### Step 1.15 — Real-run gates (needs the cluster)
- Diff the four dry runs and help outputs against the baseline.
- Run a small real ingest (`--max-accepted-articles 10`).
- Confirm the submitted `--archives` path is the existing
  `pyspark_venv-3e93….tar.gz` and that no new venv archive was written.
- Run each other stage once; accept one repack (cold cache) and confirm the
  second run is a cache hit.
- Confirm the final summary, resume command, and exit code match the shell.

**Gate:** cache hit for ingest; no unexplained argv differences; no secrets in
the log.

### Step 1.16 — Cutover
- Delete the five launcher shell files and the empty `scripts/run_processing.py`.
- Update every call site: the four tests, `scripts/supervise_ingest.py`, the
  resume-command print in `ingestion/pipeline.py`, the Airflow smoke DAG.
- Run `git grep -n '\.sh'` and fix everything that is not historical prose.

**Gate:** full test suite + package smoke pass; grep is clean; docs still mention
the old commands (that is the next phase).

---

## Phase 2 — Utilities

### Step 2.1 — `scripts/export_csv.py`
- Keep the same nine tables and their `ORDER BY` clauses, the same two
  positional arguments, the same staging-dir + atomic rename, the same row-count
  report, and the same copy to the HGFS share.
- Use one database connection; export each table with Postgres `COPY … TO
  STDOUT` CSV with header.
- Keep the "old files survive a failed export" property; do not add a
  transaction or the stage lock.

**Verify:** export to a temp directory; compare two files against a `psql`
export; simulate a failure on the last table and confirm old files are intact
and no `.export-*` directory remains.
**Test:** rewrite `tests/test_export_csv.py` against a stubbed connection (or
keep `psql` if you decide that is simpler — decide before writing).

### Step 2.2 — Delete the two thin shell shims
- Delete `inspect_db.sh` and `watch_embeddings.sh`.
- Replace their mentions with the module commands in README and docs.

**Verify:** `python -m db.inspect` and `python -m scripts.watch_embeddings`
work; the export test file no longer references a shell script.

---

## Phase 3 — Setup and model downloaders

For each of the three scripts: port as a straight sequence of subprocess calls;
keep pinned revisions and checksums hardcoded; keep resume/retry behaviour; keep
all validation and printed messages.

### Step 3.1 — `setup_embedding_onnx_env.py`
- Same venv creation/validation, same pip flags and index, same temp dir under
  `.build-tmp`, same `pip check` with `PYTHONPATH` removed, same torch guard.
**Verify:** run against a scratch venv path; the guard fires when torch is
present; the "Next:" hint is updated.

### Step 3.2 — `download_quantized_embedding_model.py`
- Same source/target dirs, revision, tokenizer/pooling layout, resume download,
  size guard, and ONNX session validation.
**Verify:** rerun resumes/uses the existing file; corrupt the graph and confirm
the size/session check fails.

### Step 3.3 — `download_quantized_reranker_model.py`
- Same revision, sha256, reuse-if-verified, and ranking smoke check.
**Verify:** second run says "reusing verified"; a wrong checksum fails loudly.

**Gate for Phase 3:** on a clean target directory, the Python versions reproduce
the same files and the same validation lines as the shell versions.

---

## Phase 4 — Docs and closeout

### Step 4.1 — Documentation sweep
- README: run commands, setup commands, resume command.
- `docs/processing.md`, `docs/workflows.md`, `docs/architecture.md`,
  `docs/module_map.md`, `docs/validation.md`, `docs/airflow-migration.md`.
- In the migration doc, correct the two wrong checklist claims: help does need
  the venvs, and a missing stage exits 1 (unknown stage exits 2).

### Step 4.2 — Final acceptance
- `git grep -n '\.sh'` shows only history/notes.
- Full test suite green; package smoke green.
- Four dry runs + help match baseline.
- One real ingest from a clean shell (no exported variables) works and loads
  `.env`.
- Interrupting a run leaves no `pack-*` directory and exits 130.

### Step 4.3 — Tag and write down the cache keys
- Record the five fingerprints and the fact that chunk/tag/embed were cold, so
  the next person does not panic at a repack.

---

## Troubleshooting quick reference

| Symptom | Look at |
|---|---|
| Ingest fingerprint is not `3e93…` | file order in the hash, `pyvenv.cfg` read, distribution listing, `sys.executable` (symlink resolution) |
| Jars cache misses every run | absolute path format, sort order, locale, `SPARK_HOME` trailing slash |
| Dry run differs from baseline | the resolver is running in the wrong venv, or `.env` was loaded twice |
| Help fails with no venv | expected today; only change it deliberately with a test |
| Spark gets the wrong Python on workers | `spark.pyspark.python` must be `./environment/bin/python`, driver value is the stage interpreter |
| Temp dir lingers | the final submission was replaced with an exec, or cleanup is outside `finally` |
| Exit 2 from a normal run | pipeline quota/validation exit, not a launcher bug — do not translate it |

**Order to fill idle time:** 2.1 and 3.1–3.3 are independent of Phase 1; do them
while waiting for cluster runs.
