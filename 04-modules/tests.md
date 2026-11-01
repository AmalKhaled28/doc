# Module: `tests/`

[Modules](README.md) · [Docs home](../README.md) · Recorded results: [archive/validation.md](../archive/validation.md)

---

> **Code baseline.** `master` at `d794b8c`, with 113 test functions. `airflow_migration`
> at `2769828` has 141. Test counts are *Verified in code*. This page does not record a
> test run.

## 1. Running the tests

```bash
.venv/bin/python -m pip install -r requirements/dev.txt
.venv/bin/python -m unittest discover -s tests -v
```

**Expected result:** a final line of `OK`, with skipped tests listed. Database tests are
skipped when `WARC_TEST_DSN` is not set, and optional RAG tests need the RAG packages.

| Requirement | Enables |
|---|---|
| `requirements/dev.txt` | Ingestion tests. It adds `warcio` to `base.txt` |
| `requirements/tagging-groq.txt` | Optional processing tests |
| `requirements/rag-agent.txt` | Retrieval tests |
| `WARC_TEST_DSN` | Integration tests on a disposable PostgreSQL database. They create and drop isolated schemas |

Never point `WARC_TEST_DSN` at `warcdb`.

## 2. Unit and integration tests

| File | Tests | Covers |
|---|---:|---|
| `tests/test_ingestion.py` | 34 | Parsing, extraction, quotas, resume, checkpoints, and database commits |
| `tests/test_retrieval.py` | 15 | Evidence, search, the agent, and LLM provider selection |
| `tests/test_processing.py` | 13 | Batching, chunking, Groq rules, and persistence |
| `tests/test_ranking.py` | 12 | Reranker inputs, score checks, attaching scores, sorting |
| `tests/test_reranker.py` | 11 | Sigmoid, logit checks, and `OnnxReranker` |
| `tests/test_shared_helpers.py` | 9 | Lazy reads, limits, and ownership of resources |
| `tests/test_job_runtime.py` | 5 | Each job's Spark execution policy |
| `tests/test_environment.py` | 3 | `.env` defaults: shared, not executed, and weaker than exported values |
| `tests/test_rag_loader.py` | 3 | Document loading |
| `tests/test_embedding_monitor.py` | 2 | Progress relative to a limited run |
| `tests/test_embedding_service.py` | 2 | The embedding worker and commit contract |
| `tests/test_launcher.py` | 2 | Launcher behavior |
| `tests/test_export_csv.py` | 1 | CSV export without a live database |
| `tests/test_packaging.py` | 1 | Worker imports outside the checkout |

**Branch only:** `tests/test_artifacts.py`, `tests/test_launcher_api.py`,
`tests/test_spark.py`, and the baseline `tests/fixtures/launch_baseline.json`, which holds
the dry-run output and fingerprints captured from the shell launcher.

## 3. Smoke tests and helpers

These are not part of `unittest discover`. Run them by hand.

| File | Purpose | Run with |
|---|---|---|
| `tests/spark_smoke.py` | Local Spark run with real HTTP, extraction, Lingua, and PostgreSQL | `spark-submit --master 'local[3]' --driver-memory 512m --conf spark.eventLog.enabled=false tests/spark_smoke.py`. Needs `WARC_TEST_DSN` |
| `tests/processing_spark_smoke.py` | Local Spark run of the chunking and tagging executor functions, with no database or provider | `.venv/bin/python -m tests.processing_spark_smoke`. Needs Java and PySpark |
| `tests/package_smoke.py` | Real launcher packaging, with a fake `spark-submit` | `.venv/bin/python tests/package_smoke.py` |
| `tests/memory_probe.py` | Repeats near-limit chunks and reports RSS after warm-up | `.venv/bin/python tests/memory_probe.py` |
| `tests/fake_backend.py` | Tagging backend for tests. It never calls a provider | `--backend tests.fake_backend:FakeBackend` |
