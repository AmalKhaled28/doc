# 10 — Prototype vs Production

[Docs home](README.md) · Previous: [09 — Operations](09-operations.md) · Next: [11 — Roadmap and open questions](11-roadmap-and-open-questions.md)

---

> **Status.** The Prototype column is *Verified in code* at `master` `d794b8c`. Everything
> in the Production column is **Proposed (not implemented)** unless it says otherwise. The
> branch column is *Verified in code* at `airflow_migration` `2769828`.

The overall view is in [01 §9](01-system-overview.md#9-prototype-and-production-at-a-glance)
and [03 §11](03-cluster-design.md#11-prototype-vs-production-cluster). This page compares
each module and gives the path between them.

## 1. Per-module comparison

| Area | Prototype (`master`) | On the branch | Production target | Gap |
|---|---|---|---|---|
| Launching | Shell scripts | `jobs.launcher`, `launch()` | Python launcher | Merge the branch |
| Orchestration | Manual; supervisor for ingestion | `ingestion_pipeline`, `@daily` | One DAG for all stages | Retries, probe, exit-code handling, decision on schedule (P09) |
| Ingestion | Bounded runs of one manifest | Previous and current month per DAG run | Continuous, new WARC data only | Retries, a probe, and a clear result for exit 2 |
| Deduplication | Values stored, nothing compares them | Same | Skip duplicates | Decision and job (P10) |
| Chunking | Batch, resumable | Same | Scheduled | None in the module |
| Tagging | Batch, Groq, 429 errors recorded | Same | Scheduled, with enough quota | Account tier and price (M03) |
| Embeddings | Spark on workers | Same | Modal with an AVX2-only ONNX Runtime | Decision (P01), a remote client, version identity |
| Retrieval | CLI | Same | HTTP API | `query_service/` |
| Search | Exact cosine | Same | Cosine or normalized index when needed | Index type; measurements |
| Database | Single instance, no index for search | Same | Sized from the target | Targets (P03, P06) |
| Secrets | `.env` and plain files | Same | Secrets manager | Not started |
| Monitoring | stdout, Telegram for ingestion | Same | Metrics and alerts for all stages | Not started |
| Backups | None in the repository | Same | Scheduled and tested | Not started; see D05 |
| Infrastructure | Not in the repository | Same | Reproducible | See J01 |

## 2. Migration path

The order is **Recommended, not measured**. The team has not agreed on it.

### Step 1: Merge the launcher

1. Finish the open check from Phase 2: a live byte-for-byte export comparison.
2. Merge `airflow_migration` into `master`.
3. Update the commands in the baseline documents ([08 §2](08-deployment-and-running.md#2-commands-on-each-ref)).

**Done when:** a real run of each stage passes through `jobs.launcher`. Only ingestion has
a recorded real run so far.

### Step 2: Make Airflow reliable

1. Move Airflow metadata to PostgreSQL (Phase 4).
2. Add a Common Crawl probe, retries, and a clear result for exit code 2 ([12 §4.1](12-airflow-and-python-migration.md#41-what-exists-on-the-branch)).
3. Decide what happens when one of the two months fails.
4. Prove it on real runs, including a killed task.

**Done when:** the plan's Phase 5 checklist passes.

### Step 3: Retire the supervisor

1. Delete `scripts/supervise_ingest.py` (Phase 6).
2. Decide what happens to the watchdog first (P11). It restarts the supervisor today.

### Step 4: Close the data gaps

1. Run the database upgrade check (D06). Add the missing columns if needed.
2. Decide on deduplication (P10).
3. Decide if tags, categories, or summaries should feed retrieval. They are stored but unused.

### Step 5: Embedding runtime

1. Record the CPU of every worker (H09). If they all match, the Spark setup is safe for now.
2. Decide on Modal (P01).
3. If adopted: add the ONNX Runtime build to `embedding_version`, make the stage and the query resolve the version the same way, and re-embed the corpus.

See [archive/embedding-runtime-consistency.md](archive/embedding-runtime-consistency.md).

### Step 6: Query API

1. Build the FastAPI service in `query_service/`, wrapping `retrieval.answer()`.
2. Add authentication, rate limits, and request logs.
3. Run the evaluation baseline in [archive/rag-api-plan.md](archive/rag-api-plan.md).
4. Measure query time. Add a cosine-compatible index when exact search is too slow.

### Step 7: Operations

1. Scheduled backups, with a tested restore.
2. Metrics and alerts for every stage.
3. A secrets manager, replacing plain files.
4. Reproducible infrastructure.
5. Production sizing from measured values (P03 to P08).

## 3. Readiness checklist

| Check | State |
|---|---|
| Every stage runs without a person | Not yet |
| A failed run recovers or alerts by itself | Ingestion only |
| The database has a tested backup | Unknown (D05) |
| Secrets are out of plain files | No |
| Duplicates are removed | No |
| Search stays fast at full corpus size | Not measured |
| Embedding vectors come from one CPU type | Unknown (H09) |
| Production targets exist | No (P03) |
