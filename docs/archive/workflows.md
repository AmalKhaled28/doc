# Workflows

Every data workflow in the project as a graph: the Spark ingestion lifecycle, the
three post-ingestion processing stages, the RAG question flow, the launcher chain,
the operational tools, and the persisted state machines. Each diagram is transcribed
from the code; node labels name the actual functions involved.

Function signatures live in the [module map](module_map.md); runtime behavior rules
(limits, locks, recovery) in [architecture.md](architecture.md); the module-level
import graph in [architecture.md](architecture.md#module-interaction-graph).

---

## 1. End-to-end data flow

The four phases run independently against PostgreSQL; only versioned keys
(`CHUNKING_VERSION`, `EMBEDDING_MODEL`/`EMBEDDING_VERSION`) connect them.

```mermaid
flowchart LR
    subgraph DB["PostgreSQL"]
        PAGES["pages / metadata / content / websites / authors"]
        CK["article_chunks (versioned)"]
        ANLY["chunk_analyses (versioned)"]
        VEC["chunk_embeddings (pgvector, versioned)"]
    end
    CC["CC-NEWS WARC files (HTTP)"] --> ING["ingestion (Spark)"]
    ING --> PAGES
    PAGES --> CHP["chunking (Spark)"] --> CK
    CK --> TGP["tagging (Spark, LLM backend)"] --> ANLY
    CK --> EMP["embeddings (Spark, BGE-M3 ONNX)"] --> VEC
    PAGES --> EXP["export_csv.sh"] --> CSV["exports/ CSVs"]
    CK & VEC --> QA["retrieval (local Python)"] --> ANS["scripts/ask.py answer"]
```

Scheduling order is free: all three processing stages can run in any order and
interleaved; each reads only the rows it depends on (`tagging` and `embeddings`
both read `article_chunks` eligible/materialized rows).

---

## 2. Ingestion run lifecycle (`ingestion.pipeline`)

```mermaid
flowchart TD
    A["load_config(argv)"] --> B{"dry_run?"}
    B -- yes --> B2["_print_config → exit"] --> DONE
    B -- no --> C["init_db()"]
    C --> D["acquire_coordinator_lock(manifest_id)"]
    D --> E{"RESUME_RUN_ID?"}
    E -- new run --> F["fetch_manifest(config)"]
    F --> G["create_run(...): persist quotas + config"]
    E -- resume --> H["load_run + restore_run_config + resume_run"]
    H --> H2["load_run_files"]
    G --> LOOP
    H2 --> LOOP

    subgraph LOOP["wave loop"]
        LOOP1["load_run + load_run_files (fresh persisted state)"] --> LOOP2{"target reached?"}
        LOOP2 -- yes --> NOGO["tasks = []"]
        LOOP2 -- no --> LOOP3["_allocate_tasks(config, run, files, wave)"]
        LOOP3 --> LOOP4{"any tasks?"}
        LOOP4 -- "no (exhausted)" --> FINISH
        LOOP4 -- yes --> LOOP5["get_spark_session() on first wave"]
        LOOP5 --> LOOP6["execute_batch(spark, tasks, process_file_chunk)"]
        LOOP6 --> LOOP7["for each result: commit_chunk(run_id, result,\n with ≤ max_retries commit attempts)"]
        LOOP7 --> LOOP8{"commit exception &\n retries left?"}
        LOOP8 -- yes --> LOOP9["close lock conn → backoff →\n reacquire_coordinator_lock"] --> LOOP7
        LOOP8 -- no/exhausted --> LOOP10["progress bar + wave summary\n(peak RSS per result)"]
        LOOP10 --> LOOP11{"worker error result?"}
        LOOP11 -- yes --> FAIL["finish_run('failed') → summary → exit 1"]
        LOOP11 -- no --> LOOP1
    end

    FINISH{"status decision"} --> S1["complete → 0"]
    FINISH --> S2["shortfall → 2"]
    FINISH --> S3["failed (any failed file) → 1"]
    S1 & S2 & S3 --> DONE["finish_run(status) → _summary → stop spark"]
```

Worker side (inside `process_file_chunk` → `_process_file_once`, executor):

```mermaid
flowchart LR
    T["task dict\n(allocated chunk_id + budgets)"] --> R["open HTTP Range from start_offset\n(ValidatedGzipStream integrity checks)"]
    R --> W1["parse_warc_records_streaming"]
    W1 --> W2["decode_and_validate: encoding + size limits"]
    W2 --> W3["extract_html_fields:\ntrafilatura → selectolax fallback,\nauthor/date/JSON-LD metadata"]
    W3 --> W4["detect_languages_batch (cached Lingua): ar/en/fr only"]
    W4 --> W5{"filter gates:\nmin_words, script checks,\nsize, byte ceilings"}
    W5 -- pass --> W6["candidate: provenance + bounded serialized article"]
    W5 -- fail / oversize --> W7["rejection counter; oversized skipped"]
    W6 --> W8{"limits hit?\n(candidate_limit | max_input_bytes\n| max_seconds | EOF)"}
    W7 --> W8
    W8 -- no --> W4
    W8 -- yes --> W9["result: candidates + next_offset + eof + stats\n(blob bounded by max_result_bytes)"]
```

### Commit transaction (driver, `db.db_handler.commit_chunk`)

```mermaid
flowchart TD
    A["commit_chunk(run_id, result)"] --> B{"chunk receipt already\nin ingest_chunks?"}
    B -- yes --> C["idempotent return: nothing re-inserted"]
    B -- no --> D["transaction: expunge previous wave state"]
    D --> E["insert article rows batched by DB_BATCH_SIZE"]
    E -- malformed row --> F["savepoint absorbs it → _write_diagnostic\n(JSONL into DEAD_LETTER_DIR) → continue"]
    E -- OK --> G["update quota counters, file state, chunk receipt"]
    F --> G
    G --> H["commit; return inserted count (quota counted on success only)"]
    G -- "- persistent SQL failure" --> I["rollback → persistent failures stop the run;\nfailed chunks never advance the cursor"]
```

---

## 3. Bounding wave engine (`jobs.batching.run_waves` — shared by all processing stages)

```mermaid
flowchart TD
    A["batch_tasks(source, cfg, make_task)"] -- "lazy: pull ≤ items-per-task rows" --> B["task dicts (byte/count bounded)"]
    B --> C{"wave = next parallelism tasks"}
    C -- "none" --> R1["return (completed, quota=False)"]
    C --> D["ThreadPool: _execute → spark job, one partition per task"]
    D --> E["as_completed: each future"]
    E -- success --> F["commit(result) → completed += 1"]
    F --> G{"result.quota_exhausted?"}
    E -- exception --> H["record failure; keep draining siblings\n(paid responses are not discarded)"]
    G -- yes --> I["stop flag"]
    G -- no --> C
    H --> J{"failure?"}
    I --> J
    J -- yes --> R2["raise"]
    J -- no --> C
    F2{"KeyboardInterrupt"} -- "first interrupt: finish active wave (commit all) then re-raise" --> D
```

---

## 4. Chunking workflow (`scripts/chunk.py` → `chunking.pipeline.run`)

```mermaid
flowchart TD
    A["config('chunk', argv): dry_run?"] -- yes --> R0["report(cfg) → exit"]
    A -- no --> B["processing_session('chunk'): init_db → chunking stage lock → SparkSession"]
    B --> C["db.chunks.iter_pending_documents(conn, CHUNKING_VERSION, document_limit)\n(raw stored article text, keyed by source_hash)"]
    C --> D["batch_tasks: pack rows ≤ items-per-task / result-bytes"]
    D --> E["run_waves(spark, tasks, process_batch, commit, parallel_tasks)"]
    subgraph WORKER["executor: process_batch"]
        W1["clean_article_text (deterministic boilerplate strip)"]
        W1 --> W2["iter_document_chunks\n(RecursiveCharacterTextSplitter 800/100)"]
        W2 --> W3["noise_reason(chunk): eligibility + filter_reason"]
        W3 --> W4["sha256 text_hash per chunk"]
        W4 --> W5["check_result_size → return documents"]
    end
    E --> WORKER
    WORKER --> F["commit: chunks.commit_document per document\n(idempotent; text_hash drift rejected)"]
    F -- "last wave done" --> G["report(chunks.counts)"]
    G --> DONE["close source cursor → stop spark"]
```

Output rows: `article_chunks` gains versioned `(chunk_index, chunk_text, text_hash,
eligible, filter_reason)` rows, and `chunk_materializations` records the
materialization receipt that makes re-runs idempotent and detects `source_hash`
text drift.

---

## 5. Tagging workflow (`scripts/tag.py` → `tagging.pipeline.run`)

```mermaid
flowchart TD
    A["config('tag', argv): dry_run?"] -- yes --> R0["report(cfg) → exit"]
    A -- no --> B["processing_session('tag'): init_db → tagging stage lock → SparkSession"]
    B --> C["analyses.identity(cfg) = (chunking_version, backend, model)\n→ analyses.iter_pending(limit, retry_errors)"]
    C --> D["batch_tasks: items + fresh batch_id (uuid4) + backend/model keys"]
    D --> E["run_waves(spark, tasks, process_batch, commit, parallel_tasks)"]
    subgraph WORKER["executor: process_batch"]
        W1["cached backend per (backend, model)\natexit-registered close"]
        W1 --> W2["GroqBackend.analyze_batch:\nrate shaping (interval/RPM/TPM),\n≤5 attempts, quota waits ≤ MAX_AUTO_WAIT"]
        W2 --> W3["validate: every request answered exactly once,\nstatus ∈ SUCCESS|NOISE|ERROR,\nno omissions without quota_exhausted"]
        W3 --> W4["check_result_size → BatchResult"]
    end
    E --> WORKER
    WORKER --> F["commit: analyses.commit_batch\n(ON CONFLICT upsert per batch_id)"]
    F --> G{"backend quota exhausted?"}
    G -- yes --> Q["exit code 2 after final report"]
    G -- no --> H["report(analyses.counts)"]
    H --> I{"--export-json?"}
    I -- yes --> J["analyses.export_json (atomic streaming)"]
    J & Q --> DONE
```

Status model per chunk: `SUCCESS` (summarized), `NOISE` (ruled not-article via
`is_noise_summary`), `ERROR` (attempt_count recorded; re-picked by
`iter_pending(..., retry_errors=True)` on the next run).

---

## 6. Embeddings workflow (`scripts/embed.py` → `embeddings.pipeline.run`)

```mermaid
flowchart TD
    A["config('embed', argv): dry_run?"] -- yes --> R0["report(cfg) → exit"]
    A -- no --> B["pin embedding_version = model_version(max_length)\n(content-hash fingerprint of model assets)"]
    B --> C["processing_session('embed'): init_db → embedding stage lock → SparkSession"]
    C --> D["embedding_store.iter_pending(cfg, limit)\nstreams eligible chunks, re-verifying text hash\nagainst the materialized text"]
    D --> E["batch_tasks: items + max_length + result_bytes"]
    E --> F["run_waves(spark, tasks, process_batch, commit, parallel_tasks)"]
    subgraph WORKER["executor: process_batch"]
        W1["set EMBEDDING_MAX_LENGTH from task"]
        W1 --> W2["generate_embeddings(texts)\nBGE-M3 INT8 ONNX: CLS pooling → L2-normalized 1024-dim"]
        W2 --> W3["count check: vectors == items"]
        W3 --> W4["check_result_size → float32 row lists"]
    end
    F --> WORKER
    WORKER --> G["commit: embedding_store.commit_batch\nvalidates 1024 dims, finite, L2-normalized"]
    G -- "last wave done" --> H["report(db.embeddings.counts)"]
```

---

## 7. RAG question flow (`scripts/ask.py` → `retrieval`)

```mermaid
sequenceDiagram
    autonumber
    participant U as ask.py (CLI)
    participant A as RagAgent.answer
    participant P as planner chain
    participant R as VectorRetriever.retrieve
    participant DB as PostgreSQL (pgvector)
    participant RK as OnnxReranker
    participant EV as assemble_evidence
    participant D as decider chain
    participant F as finalizer chain

    U->>A: question
    A->>P: plan prompt with question
    P-->>A: QueryPlan (max 3 unique queries)
    A->>R: retrieve(queries)
    R->>R: embed all queries (BGE-M3, one batch)\nvalidate (n, 1024), finite
    par per query (up to RAG_DB_CONCURRENCY threads)
        R->>DB: exact cosine top-K (candidate_k=20)\njoin source metadata
        DB-->>R: rows become SearchHit list
    end
    R->>RK: rerank_query_groups(query, chunk pairs)
    RK-->>R: sigmoid scores
    R->>R: sort_and_limit_hits, top_k=5 per query
    R-->>A: flattened SearchHit list
    A->>EV: assemble_evidence(hits)
    EV-->>A: Evidence list (dedup + merge contiguous chunk runs)
    A->>D: decision prompt (evidence, answer_language)
    alt action == search_more (one corrective round)
        D-->>A: AgentDecision(search_more, new queries)
        A->>R: retrieve(follow-up queries) rounds = 2
        A->>EV: re-assemble evidence
        A->>F: final answer prompt (grounded)
        F-->>A: AgentDecision / GroundedAnswer
    else action == insufficient and evidence exists
        A->>F: finalize anyway (final decision commit)
    else action == answer
        D-->>A: AgentDecision(answer, citations)
    end
    A->>A: citation validation: chunk-id regex (p99:c12)\nkept only if chunk id exists in evidence
    A-->>U: RagResult with sufficient flag
    U-->>U: print answer + diagnostic evidence\nexit code = result.sufficient
```

Concurrency notes: `retrieve` runs on a dedicated coordinator thread (deterministic
shutdown on Python 3.13); search queries fan out with
`max_concurrent_searches`; missing/corrupt reranker model files abort — there is
no silent cosine-only fallback.

---

## 8. Launcher / deployment chain

```mermaid
flowchart TD
    U["user: ./scripts/run_ingestion.sh --flag..."] --> E["jobs/environment.main\nload .env → os.execvpe into launcher\n(records SPARK_WARC_EXPORTED_ENV_KEYS)"]
    E --> L["run_ingestion.sh / run_processing.sh"]
    L --> LC["jobs.launch_config.main → resolve(stage, argv)\n(executor count, memory, dry-run)"]
    LC --> LC2{"dry-run?"}
    LC2 -- yes --> OUT["print resolution table → stop"]
    LC2 -- no --> PK["Package python deps + project modules\n(ingestion/, db/, chunking/, tagging/, jobs/, …)\ninto .build-tmp/; env+jar fingerprints select\nreusable archives on HDFS (USE_HDFS_CACHE)"]
    PK --> SUB["spark-submit --py-files archive\nscripts/ingest.py | chunk.py | tag.py | embed.py"]
    SUB --> RT["jobs.runtime.start_spark"]
    RT --> APP["stage entry point (run / run_streaming_pipeline)"]
    APP --> SES["processing_session: init_db → stage advisory lock\n→ SparkSession (secret redaction, executor env\nfor Groq keys / model paths)"]
```

Applies to all four launchers: `run_ingestion.sh` uses ingestion's own scheduling;
`run_chunking.sh` / `run_tagging.sh` / `run_embeddings.sh` share
`run_processing.sh` and `jobs.batching.run_waves`.

---

## 9. Operational tools

```mermaid
flowchart LR
    subgraph INSPECT["inspect_db.sh → db.inspect.main"]
        I1["print_counts"] & I2["print_progress"] & I3["print_previews"] --> I4["stdout tables (SAMPLE_SIZE rows per table)"]
    end
    subgraph EXPORT["export_csv.sh"]
        X1["psycopg COPY of nine tables\n(pages, content, metadata, analyses,\nchunks, embeddings incl. full pgvector…)"] --> X2["exports/ (project root) + copy to /mnt/hgfs/copy_path/warcdb_csv/"]
    end
    subgraph WATCH["watch_embeddings.py"]
        W1["read_counts(chunking_version, model, embedding_version)"] --> W2["remaining(eligible, embedded, run_limit…)"] --> W3{"--once?"}
        W3 -- no --> W1
    end
    subgraph VST["vector_search_test.py"]
        V1["resolve_config(conn, versions)"] --> V2["search(query, top_k)\ngenerate_embeddings → raw cosine top-k\n(no reranker, no agent)"] --> V3["printed hits"]
    end
    DB[("PostgreSQL")] --> INSPECT & EXPORT & WATCH & VST
    ONNX[(".models embedding assets")] --> WATCH & VST
```

---

## 10. Persisted state machines

### Ingestion files (`ingest_files` / `ingest_run_files`)

```mermaid
stateDiagram-v2
    [*] --> pending : create_run
    pending --> quota_reached : per-file quota consumed
    pending --> eof : validated EOF checkpoint
    pending --> failed : persistent error
    quota_reached --> pending : next run / resume
    failed --> pending : resume_run retry eligibility
    eof --> [*] : exhausted permanently
```

### Ingestion runs (`ingest_runs`)

```mermaid
stateDiagram-v2
    [*] --> running : create_run / resume_run
    running --> complete : target reached or sources exhausted
    running --> shortfall : quota not fulfillable from selected sources
    running --> failed : worker error / persistent commit failure
    complete --> [*]
    shortfall --> running : resume may still commit more
    failed --> running : resume_run retry eligibility
```

### Tagging chunk statuses (`chunk_analyses`)

```mermaid
stateDiagram-v2
    [*] --> SUCCESS : summary + category + tags
    [*] --> NOISE : is_noise_summary rules it out
    [*] --> ERROR : backend/retry failure
    ERROR --> SUCCESS : retry run
    ERROR --> NOISE : retry run
    ERROR --> ERROR : retry failed again
```

### Chunk versioning (`article_chunks` ↔ `chunk_materializations`)

```mermaid
flowchart LR
    A["new CHUNKING_VERSION\n(or new-stage version key)"] --> B["same rows become 'pending'\nfor that version"]
    B --> C["commit_document per version:\nreceipt prevents double-materialization,\ntext_hash guard rejects drift"]
    C --> D["per-version embeddings & analyses\nkeyed by identity(config)"]
```

---

## Diagram index

| § | Workflow | Entry point |
|---|---|---|
| 1 | End-to-end phases | — |
| 2 | Ingestion lifecycle, worker, commit txn | `scripts/run_ingestion.sh` |
| 3 | Shared wave engine | `jobs.batching.run_waves` |
| 4 | Chunking | `scripts/run_chunking.sh` |
| 5 | Tagging | `scripts/run_tagging.sh` |
| 6 | Embeddings | `scripts/run_embeddings.sh` |
| 7 | RAG question flow | `scripts/ask.py` |
| 8 | Launcher / deployment | shell launchers |
| 9 | Operational tools | `inspect_db.sh`, `export_csv.sh`, `watch_embeddings.py`, `vector_search_test.py` |
| 10 | State machines | persisted tables |
