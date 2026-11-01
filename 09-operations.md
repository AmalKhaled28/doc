# 09 — Operations

[Docs home](README.md) · Previous: [08 — Deployment and running](08-deployment-and-running.md) · Next: [10 — Prototype vs production](10-prototype-vs-production.md)

---

> **Code baseline.** `master` at `d794b8c`. Repository behavior is *Verified in code*.
> Steps marked *Standard* are ordinary tool commands. Commands use the `master` names;
> see [08 §2](08-deployment-and-running.md#2-commands-on-each-ref) for the branch.

## 1. What exists today

| Need | Today | Status |
|---|---|---|
| Stage progress | stdout, `inspect_db.sh`, `watch_embeddings.sh`, Spark UI | Exists |
| Ingestion recovery | Failed-file recycling, supervisor | Exists ([13](13-ingestion-operations.md)) |
| Ingestion alerts | Telegram watchdog | Branch only |
| Alerts for other stages | None | Proposed (not implemented) |
| Metrics and dashboards | None | Proposed (not implemented) |
| Database backups | None in the repository | See D05 in [FILL-IN.md](FILL-IN.md) |

## 2. Routine checks

```bash
./scripts/inspect_db.sh
./scripts/watch_embeddings.sh --once
yarn application -list -appStates RUNNING   # Standard
df -h                                       # Standard
```

`inspect_db.sh` shows core tables only. For the derived tables:

```sql
SELECT 'chunks' AS item, count(*) FROM article_chunks
UNION ALL SELECT 'eligible', count(*) FROM article_chunks WHERE eligible
UNION ALL SELECT 'analyses', count(*) FROM chunk_analyses
UNION ALL SELECT 'vectors', count(*) FROM chunk_embeddings;

-- Files left in a manifest's latest runs
SELECT r.run_id, r.status, f.status AS file_status, count(*)
FROM ingest_runs r JOIN ingest_run_files f USING (run_id)
WHERE r.manifest_id = '2026/05'
GROUP BY 1, 2, 3 ORDER BY 1;

-- Failed tagging by error text
SELECT left(error, 80) AS error, count(*)
FROM chunk_analyses WHERE status = 'ERROR'
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

-- Who holds a lock
SELECT pid, objid, granted FROM pg_locks WHERE locktype = 'advisory';
```

## 3. Logs

| Log | Where |
|---|---|
| Driver output | The terminal, or the redirected file |
| Executor logs | YARN: `yarn logs -applicationId <id>` *(Standard)* |
| Spark event logs | HDFS `/spark-logs/` (configured outside the repository) |
| Supervisor | `~/supervise_ingest.log`; relaunched runs in `~/ingest_wave.log` |
| Watchdog (branch) | `~/ops_watchdog.log` |
| Airflow services | `journalctl -u airflow-scheduler` *(Standard)* |

Stage drivers print one JSON line per commit, plus a final summary, so they are easy to
filter with `grep` or `jq`.

## 4. Rejected rows

`dead_letter/*.jsonl` on the driver host holds articles that PostgreSQL rejected. Each
line has `record` and `error`. The run continues without them, and
`invalid_database_rows` in `ingest_chunks.stats` counts them.

```sql
SELECT sum((stats->>'invalid_database_rows')::int) FROM ingest_chunks WHERE run_id = '<run id>';
```

Inspect the file, fix the cause, and remove the file when finished. Nothing reads these
files automatically. Each file name holds the run ID and the chunk ID.

## 5. Troubleshooting

| Symptom | Likely cause | Action |
|---|---|---|
| `Missing virtual environment: …` | The venv is missing or `VENV_DIR` is wrong | Create it ([08 §5](08-deployment-and-running.md#5-main-environment-and-env)) |
| `another ingestion coordinator is active for …` | A driver holds the global lock, for any manifest | Wait or stop it. Check with the lock query in [§2](#2-routine-checks) |
| `another chunking coordinator is active`, or `tagging`, `embedding` | Same, per stage | Same |
| `Set SPARK_HOME …` | `SPARK_HOME` is not set | Export it |
| Job stays in `ACCEPTED` | YARN has no room | Check running apps. Lower `SPARK_EXECUTOR_INSTANCES`, or wait for other stages |
| Executors lost, or exit 137 | Container memory | Raise `SPARK_EXECUTOR_MEMORY_OVERHEAD`, or lower task sizes |
| Exit 1 with `FAILED <url>: …` lines | Source errors | Read `ingest_run_files.last_error`, then resume |
| `source ETag changed` | The file changed since the saved offset | Investigate. The file cannot continue from that offset |
| `WARC must have one record per gzip member` | The source format changed | Investigate; ingestion needs this layout |
| `column … does not exist` at commit | A database from before the metadata change | [14 §7](14-article-metadata-and-deduplication.md#7-known-issues) |
| `chunking drift for page N; use a new version` | Cleaning or splitting changed under the same version | Change `CHUNKING_VERSION` |
| `… exceeds byte limit`, `… exceeds --result-bytes` | A task result is too large | Lower `--items-per-task` or raise `--result-bytes` |
| `page N exceeds task input-byte limit` | One article or chunk is above `--input-bytes` | Raise `--input-bytes` |
| `Groq configuration failure (HTTP 401)` | Wrong key or model | Fix `GROQ_API_KEY` or `--model` |
| Tagging exits 2 | Daily quota, or a wait over 15 minutes | Rerun later |
| Many `ERROR` rows with 429 | Pacing is too fast for the account | Set `TAG_TOKENS_PER_MINUTE`, then use `--retry-errors` |
| `embedding model asset missing`, `Missing INT8 embedding model` | The model is not installed | [08 §9](08-deployment-and-running.md#9-embeddings) |
| `source chunk changed or is ineligible` | Chunks changed during embedding | Check `chunking_version`, then rerun |
| `Missing reranker model` | The reranker is not installed | Run `download_quantized_reranker_model.sh` |
| `GROQ_API_KEY is required …` | Key not set | Add it to `.env` or export it |
| Insufficient evidence for every question | No vectors for this version, or `EMBEDDING_VERSION` is set | Use the vector query in [06 §10](06-database.md#10-useful-checks), and unset `EMBEDDING_VERSION` |
| Answer in the wrong language | `RAG_REASONING_EFFORT=low` | Use `medium` |
| `Number of candidates can not be less than …` | `RERANKER_CANDIDATE_K` is below `TOP_K` | Fix the values |

Exit codes are in [01 §10](01-system-overview.md#10-conventions).

## 6. Reprocessing

Derived data is versioned, so a change adds rows. It never overwrites them.

**New chunking version**

1. Change `CHUNKING_VERSION` in [chunking/__init__.py](../chunking/__init__.py).
2. Rerun chunking, tagging, and embeddings.
3. Old rows stay until someone deletes them. The RAG query uses the version in code, or the `CHUNKING_VERSION` variable.

**New analysis version**

```bash
TAGGING_ANALYSIS_VERSION=chunk-analysis-v3 ./scripts/run_tagging.sh
```

**New embedding model or `max_length`**

1. Install the new model, or change `EMBEDDING_MAX_LENGTH` everywhere it is set. The version changes by itself.
2. Run embeddings to the end. Until then, the RAG query reads an incomplete version.
3. Use the same model and `max_length` for the query process.

The embedding lock has no version in its key, so a full re-embed blocks regular embedding
runs. See [03 §8](03-cluster-design.md#8-concurrency-control).

**Removing an old version.** *Standard.* Back up first, and check the row count with a
`SELECT` before you delete:

```sql
DELETE FROM chunk_embeddings WHERE embedding_version = '<old version>';
```

## 7. Backup and recovery

The repository has no backup job. *(Verified in code: absence)* Whether one exists outside
it is D05 in [FILL-IN.md](FILL-IN.md). Until that is answered, assume none exists.

**What must be backed up**

| Data | Why | Can it be rebuilt? |
|---|---|---|
| Whole `warcdb` | All articles, paid LLM results, vectors | Only by paying for and rerunning every stage |
| `.env`, `~/tg_*` | Secrets and bindings | Must be recreated by hand |
| `/home/spark-master/airflow/airflow.env` | Service settings | By hand |
| `.models/` | Models | Yes, except the `.models/bge-m3` base files (M01) |
| `.build-tmp/`, HDFS archives | Build cache | Yes, automatically |

**Recommended method** *(Recommended, not measured)*:

```bash
# Backup
pg_dump -Fc -d warcdb -f warcdb-$(date +%F).dump

# Restore into an empty database that has the vector extension
pg_restore -d warcdb warcdb-YYYY-MM-DD.dump
```

Keep backups off the database host, and test a restore before you rely on it. For a large
database, take the backup when no ingestion is running, and measure its time and size.

**Recovery notes**

- Stop every driver before a restore.
- `init_db` makes no destructive change on a restored database.
- After a restore, rerun the stages: they continue from the restored state.

## 8. Disk maintenance

Nothing in the repository removes these. *(Verified in code)*

| Location | Growth | Safe to clean? |
|---|---|---|
| `.build-tmp/` | One archive per fingerprint | Yes, when no launcher is running. The next run rebuilds what it needs |
| HDFS `HDFS_APPS_DIR` | One archive per fingerprint | Yes, when no job is running *(Inferred)* |
| HDFS `/spark-logs/` | Per application | Yes, by age *(Inferred)* |
| `dead_letter/` | Per rejected batch | After inspection |
| `~/ingest_wave.log`, supervisor and watchdog logs | Continuous | Yes, with care while they run |
| YARN logs on workers | Per application | See [13 §5.3](13-ingestion-operations.md#53-approval-for-worker-cleanup) |

## 9. Rotating secrets

1. Create the new key with the provider.
2. Change it in `.env`, and in `airflow.env` if used.
3. Restart anything long-lived: running stages keep the old value.
4. Revoke the old key.

For `PGPASSWORD`, also update `POSTGRES_PASSWORD` for the supervisor and watchdog.

## 10. Common tasks

| Task | Command |
|---|---|
| Next month's data | `./scripts/run_ingestion.sh --year 2026 --month 06 --num-files all` |
| Resume a run | `./scripts/run_ingestion.sh --resume-run-id <id>` |
| Check settings | Add `--dry-run` to any stage |
| Export to CSV | `./scripts/export_csv.sh [csv_dir] [dest_dir]` |
| Export analyses to JSON | Add `--export-json analyses.json` to a tagging run |
| Apply the schema only | `.venv/bin/python -m db.db_handler`, with the `PG*` variables exported |

Exporting during a run captures only committed rows, and `chunk_embeddings.csv` can be
large.
