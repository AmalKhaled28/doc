# Module: `db/`

[Modules](README.md) · [Docs home](../README.md) · Schema reference: [06 — Database](../06-database.md)

---

> **Code baseline.** `master` at `d794b8c`. `db/` is unchanged on `airflow_migration` at
> `2769828`. Statements are *Verified in code* unless labelled otherwise.

## 1. Purpose

All PostgreSQL access goes through this package: connections, schema migration, locks,
and one store per stage. Only drivers and local tools use it. Executors never connect.

## 2. Files

| File | Responsibility | Used by |
|---|---|---|
| [db/db_handler.py](../../db/db_handler.py) | Connections, `init_db`, ingestion state and commits | All stages |
| [db/enrichment_schema.py](../../db/enrichment_schema.py) | DDL for chunk and analysis tables (schema version 3) | `init_db` |
| [db/vector_schema.py](../../db/vector_schema.py) | DDL for `chunk_embeddings` (schema version 4) | `init_db` |
| [db/chunks.py](../../db/chunks.py) | Processing locks, chunk reads and commits | Chunking; all processing stages for the lock |
| [db/analyses.py](../../db/analyses.py) | Pending chunks for tagging, analysis commits, JSON export | Tagging |
| [db/embeddings.py](../../db/embeddings.py) | Pending chunks for embedding, vector commits | Embeddings |
| [db/documents.py](../../db/documents.py) | Streaming articles as LangChain documents | Applications |
| [db/inspect.py](../../db/inspect.py) | Read-only status report | `inspect_db.sh`; on the branch, `python -m db.inspect` |

## 3. Ingestion writes

`commit_chunk(run_id, result, batch_size)` is one transaction:

1. A `chunk_id` already in `ingest_chunks` returns its earlier count. This is what makes a retry safe.
2. It locks the run and file rows with `FOR UPDATE`.
3. It rejects a stale cursor, a failed result that carries data, a result with no progress, and a result above the remaining quota.
4. It inserts articles in batches of `DB_BATCH_SIZE`.
5. It sets the file status: `failed`, `eof`, `pending`, or `quota_reached`.
6. It updates the counters and writes the `ingest_chunks` receipt.
7. On success, it updates the shared checkpoint in `ingest_files`.

**Bad rows.** `_insert_isolated` wraps each batch in a savepoint. On a `DataError`,
`IntegrityError`, or a `ValueError` for a bad URL, it splits the batch into single records.
A record that still fails is written to `dead_letter/` as JSONL, and the chunk continues.
Any other error, such as a missing column, fails the whole commit.

**New rows only.** `_insert_new_records` uses `ON CONFLICT (warc_record_id) DO NOTHING`,
and only new pages get `metadata` and `content` rows. Quotas count these new pages.

## 4. Connections

| Function | Behavior |
|---|---|
| `get_connection` | New `psycopg2` connection from the `PG*` variables ([07 §3](../07-configuration.md#3-database-settings)) |
| `connection_scope` | Decorator: reuses a passed connection, or opens and closes its own. It always rolls back open work at the end |
| `streaming_cursor` | A named server-side cursor on its own connection, so writer commits do not invalidate it |

Readers use `streaming_cursor` so that a run never holds all pending rows in memory.

## 5. Locks

Locks are PostgreSQL advisory locks. Their keys are in
[03 §8](../03-cluster-design.md#8-concurrency-control). `acquire_coordinator_lock` and
`acquire_lock` use `pg_try_advisory_lock`, so a second driver fails at once instead of
waiting. The lock belongs to the connection, so it is released when that connection
closes.

## 6. Migrations

`init_db` runs at the start of every stage. It uses a transaction-level advisory lock and
the table `ingest_schema_versions`:

| Version | Applies | Skipped when |
|---|---|---|
| 2 | The main DDL: core and ingestion tables | Version 2 is recorded |
| 3 | Chunks, materializations, analyses | Version 3 is recorded |
| 4 | `vector` extension, `chunk_embeddings`, HNSW index | Version 4 is recorded |
| 5 | Nothing: the query runs but its result is not used | Always |

Two consequences matter on an existing database:

- The newer `pages` and `metadata` columns are only in the version 2 DDL. See [14 §7](../14-article-metadata-and-deduplication.md#7-known-issues).
- `UPGRADE_V5` in `db/vector_schema.py` is never called. A database at version 4 from before `embedding_version` existed would not get that column. See [11](../11-roadmap-and-open-questions.md).

## 7. Stage reads

| Function | Selects | Batch size |
|---|---|---|
| `chunks.iter_pending_documents` | Articles with text and no receipt for this `chunking_version` | 25 |
| `analyses.iter_pending` | Eligible chunks with no analysis, plus `ERROR` rows with `--retry-errors` | 30 |
| `embeddings.iter_pending` | Eligible chunks with no vector for this identity | 25 |
| `documents.iter_documents` | All articles with non-empty text, as LangChain documents | `RAG_FETCH_BATCH_SIZE` |

Each stage also has `counts`, which reports totals as JSON at the end of a run.

## 8. Using documents from Python

```python
from db.documents import iter_documents
from chunking.splitters import iter_document_chunks

for chunk in iter_document_chunks(iter_documents()):
    print(chunk.metadata["doc_id"])
```

`load_documents_distributed` loads into memory, so it needs `limit=` or
`RAG_DOCUMENT_LIMIT`. The `publisher` in document metadata is the site domain.

## 9. Compatibility helpers

`save_batch_records`, `load_ingest_progress`, and `upsert_ingest_progress` are not part of
the Spark pipeline. Run `python -m db.db_handler` to apply the schema alone.
