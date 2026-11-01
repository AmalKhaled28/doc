# 11 — Roadmap and Open Questions

[Docs home](README.md) · Previous: [10 — Prototype vs production](10-prototype-vs-production.md)

---

> **Code baseline.** `master` at `d794b8c` and `airflow_migration` at `2769828`. Evidence
> comes from reading the code; nothing here was executed. Priorities are
> **Recommended, not measured**, and the team has not agreed on them.

This page lists every known issue in one place. Details stay in the linked pages. Open
blanks stay in [FILL-IN.md](FILL-IN.md).

**Priority.** High: can lose data, stop a run, or expose a secret. Medium: gives wrong
behavior or misleads an operator. Low: cleanup.

## 1. Data and schema

| ID | Priority | Issue | Where | Ref |
|---|---|---|---|---|
| K01 | High | A database from before the metadata change does not get the new columns, and inserts fail | [14 §7](14-article-metadata-and-deduplication.md#7-known-issues) | Both |
| K02 | Medium | `UPGRADE_V5` is defined but never called, and the version 5 check does nothing. A version 4 database from before `embedding_version` would not be upgraded | [db/vector_schema.py](../db/vector_schema.py), [db §6](04-modules/db.md#6-migrations) | Both |
| K03 | Medium | Rows from before the metadata change have empty new columns | [14 §7](14-article-metadata-and-deduplication.md#7-known-issues) | Both |
| K04 | Medium | Deduplication columns exist, but nothing fills them, so duplicates are processed and billed | [14 §6](14-article-metadata-and-deduplication.md#6-deduplication-status) | Both |
| K05 | Low | `ingest_chunks` grows by one row per task, and nothing cleans it | [06 §8](06-database.md#8-retention) | Both |

## 2. Embeddings and retrieval

| ID | Priority | Issue | Where | Ref |
|---|---|---|---|---|
| K06 | High | Vectors differ between CPU instruction sets under one `embedding_version` | [02 §3.1](02-hardware-and-infrastructure.md#31-known-and-unknown-specifications) | Both |
| K07 | Medium | `EMBEDDING_VERSION` affects the query but not the embedding stage, so a set value can search a different version | [01 §7](01-system-overview.md#7-version-keys-connecting-the-stages) | Both |
| K08 | Medium | Exact search scans every vector, and the HNSW index does not serve it while still being maintained on each insert *(cost Inferred)* | [03 §9](03-cluster-design.md#9-scaling-considerations) | Both |
| K09 | Medium | A full re-embed blocks regular embedding runs, because the lock has no version | [03 §8](03-cluster-design.md#8-concurrency-control) | Both |
| K10 | Medium | Tagging results are stored but retrieval does not use them, although each one costs an LLM call | [05 §2](05-data-flow.md#2-formats-at-each-step) | Both |
| K11 | Low | Sources show the site domain as `publisher`, not the stored `metadata.publisher` | [retrieval §9](04-modules/retrieval.md#9-result) | Both |
| K12 | Low | `requirements/embeddings.txt` is not installed by any script, and nothing imports its `uvicorn` or `langdetect` | [01 §8](01-system-overview.md#8-technology-stack) | Both |

## 3. Orchestration

| ID | Priority | Issue | Where | Ref |
|---|---|---|---|---|
| K13 | High | The DAG has no retries or probe, and exit code 2 fails the task | [12 §4.1](12-airflow-and-python-migration.md#41-what-exists-on-the-branch) | Branch |
| K14 | Medium | The DAG does not match the plan: name, shape, and order of phases | [12 §5.2](12-airflow-and-python-migration.md#52-work-order) | Branch |
| K15 | Medium | An error in the first month stops ingestion of the second | [12 §4.1](12-airflow-and-python-migration.md#41-what-exists-on-the-branch) | Branch |
| K16 | Low | Airflow units use `TZ=UTC`, and the DAG tasks use `TZ=EET` | [07 §7](07-configuration.md#7-launcher-and-spark-settings) | Branch |
| K17 | Low | The exit behavior for an unknown stage needs a test run | [12 §8](12-airflow-and-python-migration.md#8-known-issues) | Branch |
| K18 | Low | `airflow/dags/ingestion.py` is truncated and fails to parse | [12 §8](12-airflow-and-python-migration.md#8-known-issues) | `master` |

The launcher no-op, the venv re-execution issue, and the missing guide references are
closed on the branch. See [12 §8](12-airflow-and-python-migration.md#8-known-issues).

## 4. Ingestion operations

| ID | Priority | Issue | Where | Ref |
|---|---|---|---|---|
| K19 | High | Free-text Telegram messages reach an agent running with `--auto` | [13 §7.3](13-ingestion-operations.md#73-recycling-and-security) | Branch |
| K20 | High | The sudo password is in a plain file | [13 §7.3](13-ingestion-operations.md#73-recycling-and-security) | Branch |
| K21 | Medium | Telegram commands are never read at the default interval | [13 §7.1](13-ingestion-operations.md#71-watchdog-behavior) | Branch |
| K22 | Medium | The stall alert fires only if a check runs late | [13 §7.1](13-ingestion-operations.md#71-watchdog-behavior) | Branch |
| K23 | Medium | `stop` from Telegram does not last | [13 §7.1](13-ingestion-operations.md#71-watchdog-behavior) | Branch |
| K24 | Medium | A failed database query skips the watchdog check | [13 §7.1](13-ingestion-operations.md#71-watchdog-behavior) | Branch |
| K25 | Medium | The cleanup approval text differs from the command, and cleanup always reports success | [13 §7.1](13-ingestion-operations.md#71-watchdog-behavior) | Branch |
| K26 | Medium | Completion may never be detected across several runs | [13 §7.2](13-ingestion-operations.md#72-supervisor-behavior) | Branch |
| K27 | Medium | Any Spark job counts as an ingestion driver, and `stop --with-run` could kill it | [13 §7.2](13-ingestion-operations.md#72-supervisor-behavior) | Both |
| K28 | Medium | Any advisory lock blocks a relaunch | [13 §7.2](13-ingestion-operations.md#72-supervisor-behavior) | Both |
| K29 | Medium | The supervisor reads `POSTGRES_*`, while the project uses `PG*` | [13 §7.2](13-ingestion-operations.md#72-supervisor-behavior) | Both |
| K30 | Low | Recycling retries permanent errors, such as a changed ETag | [13 §7.3](13-ingestion-operations.md#73-recycling-and-security) | Both |
| K31 | Low | `wait-reply` does not check the chat | [13 §7.3](13-ingestion-operations.md#73-recycling-and-security) | Both |
| K32 | Low | Manifest ID, repository path, and worker addresses are fixed in code | [13 §7.3](13-ingestion-operations.md#73-recycling-and-security) | Both |
| K33 | Low | Log files are tracked in git | [13 §7.3](13-ingestion-operations.md#73-recycling-and-security) | Both |

## 5. Security and configuration

| ID | Priority | Issue | Where | Ref |
|---|---|---|---|---|
| K34 | High | The database code has a default password of `password` | [db/db_handler.py](../db/db_handler.py) | Both |
| K35 | Medium | No backup exists in the repository | [09 §7](09-operations.md#7-backup-and-recovery) | Both |
| K36 | Medium | `GROQ_API_KEY` is passed to executors as a Spark setting | [07 §8](07-configuration.md#8-secrets) | Both |
| K37 | Low | `.env.example` lists part of the settings, repeats `GEMINI_API_KEY`, and includes `SUPPORTED_LANGUAGES`, which no code reads | [07 §1](07-configuration.md#1-where-settings-live) | Both |
| K38 | Low | The Groq 429 errors in the snapshot show the default pacing is too fast for the account in use | [02 §6.4](02-hardware-and-infrastructure.md#64-llm-tagging-cost-and-duration) | Both |

## 6. Code and documentation cleanup

| ID | Priority | Issue | Where | Ref |
|---|---|---|---|---|
| K39 | Low | `RETRY_TEMPERATURE` and `CHUNK_MAX_TOKENS` are unused, and `groq.py` repeats their values | [tagging §9](04-modules/tagging.md#9-differences-from-the-constants) | Both |
| K40 | Low | A docstring mentions `clean_results.py`, which is not in the repository | [tagging §9](04-modules/tagging.md#9-differences-from-the-constants) | Both |
| K41 | Low | The `init_db` docstring follows a `raise`, so it is not a docstring | [14 §7](14-article-metadata-and-deduplication.md#7-known-issues) | Both |
| K42 | Low | The project README says there are no empty placeholder packages, but `query_service/` is one | [README.md](../README.md) | Both |
| K43 | Low | `docs/airflow-migration.md` on the branch still plans `news_pipeline`, and to delete `ingestion.py` | [12 §5.2](12-airflow-and-python-migration.md#52-work-order) | Branch |
| K44 | Low | `cluster_up.sh` and `cluster_down.sh` are mentioned but not in the repository | [FILL-IN.md](FILL-IN.md), J01 | Both |

## 7. Limitations by design

| Limitation | Note |
|---|---|
| Languages | Arabic, English, and French only |
| Arabic dialect | Not identified: `arabic_dialect` is always `NULL` |
| Text length for embedding | Cut at 256 tokens by default, with no warning |
| Query rounds | At most two search rounds |
| Answers | At most 180 words |
| Query interface | CLI and Python only |
| Ingestion layout | Needs one WARC record per gzip member |

## 8. Roadmap

The steps are in [10 §2](10-prototype-vs-production.md#2-migration-path).

| Horizon | Work | Blocked by |
|---|---|---|
| Now | Check the live schema (K01). Set a real password and remove the default (K34). Confirm backups (K35). Record worker CPUs (K06) | Access to the cluster |
| Next | Merge the branch. Make the DAG safe (K13, K15). Fix the supervisor settings (K26 to K29) | Team review |
| Later | Deduplication, Modal, the HTTP API | P01, P10 |
| Production | Monitoring, secrets manager, sizing | P03 to P08 |

## 9. Open questions

There are 46 open questions in [FILL-IN.md](FILL-IN.md). These decide the most:

| ID | Question | Why it matters |
|---|---|---|
| P03 | Production targets | Everything about sizing depends on it |
| P01 | Moving embeddings to Modal | Decides the runtime and a full re-embed |
| P09 | DAG schedule | The branch uses `@daily`, as a Hint only |
| P10 | Deduplication owner | Affects cost and answer quality |
| D05 | Current backups | Decides the risk of data loss today |
| D06 | Live schema | Decides if K01 is already a live problem |
| H09 | Worker CPUs | Decides if stored vectors are consistent |
