# Validation

## Project restructuring — 2026-10-01

After organizing the application into top-level `ingestion/`, `db/`, and
`chunking/` packages:

- `.venv/bin/python -m unittest discover -s tests -v`: 32 tests,
  17 passed and 15 skipped (12 require `WARC_TEST_DSN`; 3 require optional
  LangChain dependencies).
- `scripts/run_ingestion.sh` passed dry run from outside the repository without
  packaging or starting Spark. Legacy root wrappers have been removed.
- Application modules and `scripts/ingest.py` passed dry run using an isolated
  worker ZIP outside the checkout, with optional LangChain imports blocked.
- All three files in `requirements/` parsed locally with their relative includes.
- CSV exports now live in `exports/`; generated Spark archives stay in `.build-tmp/`.
- Python syntax, shell syntax, and `git diff --check` passed.

Live PostgreSQL, Spark/YARN, and full virtual-environment relocation checks were
not rerun for this restructuring. The results below describe the earlier layout.

## Historical validation — 2026-09-30 (updated with YARN results)

Implementation, local validation, and distributed YARN validation are complete.
No production ingestion was started; YARN runs used an isolated test schema
(`yarn_validation`) on a disposable PostgreSQL server.

## Checks performed

| Check | Result |
|---|---|
| Core suite in the new `.venv` | 28 passed; 2 optional RAG tests skipped |
| Optional RAG tests in the existing sibling environment with RAG dependencies | 2 passed |
| PostgreSQL integration coverage | 12 tests, PostgreSQL 18, isolated schemas in a disposable local server |
| Local Spark integration | Spark 4.2.0, `local[3]`, three real Python worker processes |
| Quota smoke | Exactly 7 inserts; per-file counts 3 / 2 / 2 against a cap of 3 |
| Parallelism | All three initial task execution intervals overlap |
| Worker memory, default extraction | Initial-wave Python peak RSS 99.0 / 99.0 / 99.1 MiB |
| Repeated bounded results | 20 chunks, 4,009,372 candidate JSON bytes per chunk |
| Repeated-result memory | Peak RSS 89.98 → 90.32 MiB; about 0.02 MiB growth after warmup |
| Packaging | Real launcher built venv/code archives; relocated Python imported modules and classified English |
| Config dry run | Passed without packaging, Spark, or database access |
| Dependency consistency | `PYTHONPATH= .venv/bin/python -m pip check`: no broken requirements |
| Python and shell syntax | Passed |
| YARN 1-file smoke (real CC-NEWS) | 10/10 new, `application_1790757150855_0001 SUCCEEDED`, 9.284s, peak Python RSS 185.7 MiB |
| YARN 3-file smoke (real CC-NEWS) | 9/9 new (3/3/3), `application_1790757150855_0002 SUCCEEDED`, 10.639s, peaks 173.1/174.4/176.5 MiB |
| YARN parallelism | 3 simultaneous executors on worker1/worker2/worker3; 3x TaskEnd success in eventlog |
| YARN resume + DB integrity | 19 pages / 19 distinct IDs, 19 complete articles; resume of completed run idempotent |

The Spark test uses generated WARC files served over real localhost HTTP and real
PostgreSQL transactions. It enables the default trafilatura-first extraction. The
measured ingestion portion took 2.76 seconds, excluding Spark startup; this is a
fixture result, not an estimate of Common Crawl throughput.

The repeated-result probe uses selectolax fallback and real Lingua classification
on large generated articles. It ran with the existing sibling environment. Its
RSS measurements cover Python, not the JVM or a complete YARN container. This does
not establish a maximum RSS for arbitrary web pages.

The new `.venv` installs the declared ingestion/test dependencies. Lingua 2.2.0 was
installed from a wheel reconstructed from the existing matching installation after
an initial download exhausted the small `/tmp` filesystem; other dependencies came
from the package index. Packaging uses temporary directories on the project
filesystem and publishes local archives on that same filesystem.

## Behaviors covered

- Targets below batch size, non-divisible allowances, per-file caps, duplicate-only
  chunks, duplicate IDs in a batch, and exact inserted-row counting.
- Transaction rollback, uncertain commit acknowledgement recovery, invalid-row
  isolation, advisory-lock exclusion, stale/stalled cursor rejection, failed-file
  resume, verified partial-cursor reuse, and preservation of legacy completed files.
- Real gzip member offsets, ignored HTTP Range, unknown response length, changed
  source validators, malformed ranges, truncated HTTP/gzip, CRC corruption, decoded
  oversized HTTP bodies, result-budget replay, and non-HTML input-budget progress.
- French accents, declared encodings, JSON-LD metadata, nonoverlapping language
  samples, and real English/French/Arabic versus Spanish/Persian/Urdu classification.
- Streaming RAG consumption and early-close cleanup; eager-loader limit enforcement.

## YARN cluster validation (2026-09-30, `spark-master:8032`, 3 workers RUNNING)

```bash
NUM_FILES=1 MAX_ACCEPTED_ARTICLES=10 MAX_ACCEPTED_PER_FILE=10 ./run_to_db.sh
NUM_FILES=3 MAX_ACCEPTED_ARTICLES=9 MAX_ACCEPTED_PER_FILE=3 ./run_to_db.sh
```

* 1-file run `7dfd5c9d-deaa-41c6-b9fe-831216cf82c8`: `complete; committed 10/10 new`,
  `CC-NEWS-20260501024825-07751.warc.gz: 10 new; quota_reached`.
  Counters: records 47, non_html 24, language_rejected 9, too_short_or_corrupt 4,
  compressed_bytes_read 1441792, result_bytes 38952, elapsed 9.284s.
* 3-file run `aac233f9-25f9-4043-89f0-ed4707e88c34`: `complete; committed 9/9 new`
  (3/3/3 across `...-07751/-07752/-07753`, all `quota_reached`).
  Counters: records 56, non_html 29, language_rejected 17, too_short_or_corrupt 1,
  compressed_bytes_read 1441792, result_bytes 30691, elapsed 10.639s.
* Both YARN apps `FINISHED SUCCEEDED` (`...0001`, `...0002`; `...0002` memorySeconds
  68985, vcoreSeconds 64). Eventlog `...0002` shows 3 ExecutorAdded / 3 TaskStart /
  3 TaskEnd success on worker1/worker2/worker3, 4 StageExecutorMetrics.
* Container metrics (StageExecutorMetrics): PythonRSS ~195–221 MiB, JVMRSS ~223–233 MiB,
  JVMHeap ~50–64 MiB per executor; driver JVMRSS ~400 MiB. Worker-reported peak Python
  RSS matches: 185.7 MiB (1-file), 173.1/174.4/176.5 MiB (3-file).
* DB: `ingest_runs` 10 + 9 `complete`; `pages` 19 / distinct IDs 19; complete articles
  (pages+metadata+content) 19. Chunks on worker3/worker2/worker3/worker1 with verified
  next_offsets. `RESUME_RUN_ID=aac233f9... ./run_to_db.sh` returns the stored 9-article
  summary without new inserts. Eventlogs under `/spark-logs/eventlog_v2_...`.
* Atomic HDFS artifact publication exercised (venv + `spark-libs-*.zip` via
  `*.upload-*` + rename, `spark.yarn.archive`).

## Remaining work

Measure sustained real-CC-NEWS throughput before raising defaults. There is no
production before/after throughput comparison in this validation.

Reproducible local checks are in `tests/test_ingestion.py`, `tests/test_launcher.py`,
`tests/test_rag_loader.py`, `tests/spark_smoke.py`, `tests/memory_probe.py`, and
`tests/package_smoke.py`. The disposable PostgreSQL server used during this session
was stopped after testing.
