# Module map

Every module's purpose, public function signatures, and inter-module dependencies.
Signatures are transcribed from the source; when docstrings or annotations are
absent there, they are absent here too. [Architecture](architecture.md) describes
runtime behavior; this page is the API and wiring reference.

The interaction graph lives in
[architecture.md](architecture.md#module-interaction-graph); per-workflow data-flow
diagrams in [workflows.md](workflows.md).

Import-style flags used below:

- **direct** — module-level project import
- **lazy** — imported inside a function (deliberate cycle breaker; see caveats)
- **worker-lazy** — imported inside the Spark worker function so the driver never loads the dependency
- **type-only** — `TYPE_CHECKING` block only

---

## jobs/ — shared Spark runtime and validation

Shared infrastructure; no domain knowledge of ingestion, chunking, tagging, or embeddings.

```python
# jobs/validation.py
integer(value, *, name, minimum=0)            # validate an int config value
env_int(name, default, *, minimum=0)          # env-var int with minimum, for config dicts
positive_int(value)                           # argparse type: int > 0

# jobs/environment.py
load_environment(path)                        # optional dotenv load, no overwrite
main()                                        # records exported keys, loads .env, execvpe into launcher

# jobs/launch_config.py
resolve(stage, argv) -> tuple                 # (executor_count, dry_run, driver_mem, executor_mem, overhead)
main()                                        # tab-separated stdout resolver for shell launchers

# jobs/batching.py
serialized_size(value) -> int
check_result_size(result, max_bytes, message) # raise on oversize serialized Spark result
batch_tasks(source, cfg, make_task) -> iterator   # lazily pack items into bounded task dicts
batches(items, max_items, max_bytes) -> iterator  # byte/count-bounded sub-batching
run_waves(spark, tasks, worker, commit, parallelism)   # ThreadPool wave executor, drain-before-fail
execute_batch(spark, tasks, worker) -> list           # one partition per task, in-order collect

# jobs/runtime.py
processing_session(stage, cfg) -> contextmanager[(conn, spark)]
config(stage, argv=None) -> dict              # stage argparse (chunk/tag/embed) with env defaults
start_spark(stage, cfg) -> SparkSession       # builder with secret redaction + executor env
cli(run)                                      # error/KeyboardInterrupt handler wrapper
report(values)                                # JSON stdout
positive = positive_int                       # compatibility alias
```

- imports: `jobs.validation` (direct); `db.chunks` (lazy), `db.db_handler` (lazy),
  `chunking.CHUNKING_VERSION` (lazy) — see caveats
- imported by: `ingestion.pipeline`, `chunking.pipeline`, `tagging.pipeline`,
  `embeddings.pipeline`, `retrieval.store`, `retrieval.reranker`, `scripts/watch_embeddings`
- caveats: `processing_session` lazily imports `db.chunks.acquire_lock` and
  `db.db_handler.init_db`, while stage pipelines import `jobs.runtime`. Moving those
  imports to module level would create real import cycles. This lazy contract is
  load-bearing; do not hoist. `jobs.launch_config` has the same arrangement with
  `ingestion.pipeline` (lazy) and `jobs.runtime` (lazy).

---

## ingestion/ — WARC → article rows (Spark ingestion phase)

```python
# ingestion/warc.py
class ValidatedGzipStream(source, expected_bytes=None)   # .read(size=65536), .tell() — CRC/length-checked
parse_warc_records_streaming(stream, max_html_bytes=2*1024*1024, base_offset=0)
    # yields record dicts; non-response boundaries surface as {"skip": ...}
parse_warc_stream_fastwarc(stream, **kwargs)             # alias preserving the old entry point

# ingestion/text.py
is_target_language(lang_code: str) -> bool
is_clean_encoding(text: str) -> bool
is_valid_arabic_text(text: str) -> bool
decode_and_validate(raw_bytes: bytes, declared_encoding: Optional[str] = None) -> Tuple[Optional[str], str]

# ingestion/extraction.py
get_lingua_detector()                                    # lazy 20-language detector singleton
detect_languages_batch(texts: List[str], min_confidence=None) -> List[str]
extract_publication_date(tree: HTMLParser) -> str
extract_date_from_text_fallback(tree: HTMLParser) -> str
extract_author(tree: HTMLParser) -> str
extract_html_fields(record: Dict[str, Any], min_words: int = 80, use_trafilatura=None) -> Dict[str, Any]

# ingestion/pipeline.py
load_config(argv=None) -> dict
get_spark_session() -> SparkSession
fetch_manifest(config) -> list[str]                      # CC-NEWS warc.paths.gz with retries
process_file_chunk(task) -> dict                         # Spark worker entry point
restore_run_config(config, run) -> dict                  # reconcile persisted run on resume
run_streaming_pipeline(argv=None) -> int                 # driver loop; returns exit code
main()                                                   # sys.exit wrapper
SEMANTIC_ENV                                             # env-var ↔ config-key map
```

- imports: `ingestion.text` ← `ingestion.extraction` (direct); `jobs.runtime`,
  `jobs.batching`, `jobs.validation`, `db.db_handler` (direct, `pipeline.py` only)
- imported by: `scripts/ingest.py`, `jobs.launch_config` (lazy)
- caveats: the largest, least-annotated module (618 lines). `_process_file_once` is
  the actual worker logic behind `process_file_chunk`; `_allocate_tasks` implements
  quota wave allocation. Both are private but load-bearing.

### Driver ↔ worker contract (Spark-serialized dicts)

Task dict (`_allocate_tasks` → `process_file_chunk`):

```python
{"chunk_id": str, "file_url": str, "start_offset": int, "etag": str|None,
 "last_modified": str|None, "candidate_limit": int, "max_result_bytes": int,
 "max_input_bytes": int, "max_seconds": int, "max_html_bytes": int,
 "max_article_bytes": int, "min_word_count": int, "use_trafilatura": bool,
 "lingua_min_confidence": float, "max_retries": int}
```

Result dict (`process_file_chunk` → `db.db_handler.commit_chunk`):

```python
{"chunk_id": str, "file_url": str, "next_offset": int, "eof": bool,
 "candidates": [{"record_id", "file_url", "warc_date", "raw_html", "source",
                 "author", "published_date", "language", "clean_text", "page_url"}],
 "stats": {...}}
```

---

## db/ — PostgreSQL persistence (driver-only writes)

Five thin stores over one shared `db_handler` core. All connection defaults come
from libpq-style env vars via `DB_CONFIG`.

```python
# db/db_handler.py
get_connection()
streaming_cursor(name, *, itersize, connection_factory=None)   # contextmanager, own read connection
connection_scope(function)                                     # decorator; opens/cleans own connection when conn=None
init_db(conn=None)                                             # additive migrations; applies enrichment + vector DDL
extract_domain(url)
acquire_coordinator_lock(manifest_id)                          # dedicated advisory-lock connection
create_run(manifest_id, urls, target_articles, per_file_target, config, conn=None) -> str
load_run(run_id, conn=None) -> dict
load_run_files(run_id, conn=None) -> list[dict]
resume_run(run_id, conn=None)
commit_chunk(run_id, result, batch_size=25, conn=None)         # idempotent, quota-checked transaction
finish_run(run_id, status, conn=None)
save_batch_records(records_list, conn=None)                    # compatibility helper
load_ingest_progress(manifest_id, conn=None) -> dict
upsert_ingest_progress(manifest_id, file_url, bytes_done, articles_done, complete, conn=None)

# db/documents.py
iter_documents(limit: Optional[int] = None) -> Iterator[Document]
load_documents_distributed(limit: Optional[int] = None) -> List[Document]  # eager; requires limit= or RAG_DOCUMENT_LIMIT

# db/chunks.py            (chunking stage store)
source_hash(document)
acquire_lock(stage) -> contextmanager
iter_pending_documents(conn, version, limit=None) -> iterator
commit_document(conn, version, result)                         # idempotent, hash-drift checked
counts(conn, version) -> dict                                  # {"eligible", "materialized", "pending", ...}

# db/analyses.py          (tagging stage store)
identity(config) -> tuple                                      # (chunking_version, backend, model)
iter_pending(config, limit=None, retry_errors=False) -> iterator
commit_batch(conn, config, batch_id, outcomes)
counts(conn, config) -> dict
export_json(config, destination)                               # atomic streaming JSON export

# db/embeddings.py        (embedding stage store)
iter_pending(config, limit=None) -> iterator                   # verifies text hash while streaming
commit_batch(conn, config, rows)                               # validates 1024-dim, L2-normalized, finite
counts(conn, config) -> dict

# db/inspect.py           (inspection CLI)
print_counts(cur); print_progress(cur); print_previews(cur); main()

# db/enrichment_schema.py, db/vector_schema.py
DDL                                                            # DDL constants; UPGRADE_V5 in vector_schema
```

- imports: every store → `db.db_handler` (direct); `db_handler` → schema modules (lazy)
- imported by: `ingestion.pipeline` (`db_handler`), all four stage pipelines,
  `retrieval.store`, `scripts/vector_search_test`, `scripts/watch_embeddings`, `jobs.runtime` (lazy)
- caveats:
  - **`db/documents.py` is currently orphaned** — nothing in the app imports it; the
    stage pipelines select through `db/chunks.py` instead. It remains the documented
    RAG loader (`README.md` RAG integration example) and is the exemplar of this
    package's documentation style.
  - `db/chunks.py`, `db/analyses.py`, `db/inspect.py` have no docstrings and no
    type annotations.
  - Commit signatures are asymmetric by design: `commit_chunk(run_id, result, ...)`
    takes a run id, while the three stage stores take an open `conn` and a
    stage-specific `config`/`version` key.

---

## chunking/ — text cleaning and splitting (processing phase)

```python
# chunking/__init__.py
CHUNKING_VERSION = "recursive-800-100-clean-v2.4.1-v1"

# chunking/splitters.py
iter_document_chunks(documents: Iterable[Document], chunk_size: int = 800,
                     chunk_overlap: int = 100) -> Iterator[Document]
split_documents_into_chunks(documents: List[Document], chunk_size: int = 800,
                            chunk_overlap: int = 100) -> List[Document]

# chunking/preprocessing.py
clean_article_text(text: str) -> str                 # deterministic header/cut-marker/copyright stripping
noise_reason(chunk) -> Optional[str]                 # "short"|"blocked"|"non_article_url"|"linklist"
                                                     # |"bio"|"headlines"|"garbled"|"aggregator"|None

# chunking/pipeline.py                                (Spark coordinator)
process_batch(task) -> dict                          # worker: clean → split → hash → eligibility rows
run(argv=None) -> int                                # driver: pending docs → waves → chunks.commit_document
main()
```

- imports: `pipeline.py` → `db.chunks`, `jobs.batching`, `jobs.runtime` (direct);
  `preprocessing.py`, `splitters.py`, `langchain_core.documents` (worker-lazy in `process_batch`);
  `splitters.py` → its own package `__init__` for `CHUNKING_VERSION`
- imported by: `scripts/chunk.py`, `retrieval.store` (`CHUNKING_VERSION`),
  `scripts/watch_embeddings`, `jobs.runtime` (lazy)

### Driver ↔ worker contract

Task dict: `{"documents": [source rows from db.chunks], "items_per_task", "result_bytes", "max_length"}`
— worker result: `{"documents": [{"page_id", "source_hash", "chunks": [{"chunk_index",
"chunk_text", "text_hash", "eligible", "filter_reason"}]}]}`.

---

## tagging/ — batch analysis with pluggable backends (processing phase)

```python
# tagging/contracts.py
@dataclass(frozen=True)
AnalysisRequest(page_id: int, chunk_index: int, text: str, language: str, title: str)

@dataclass
AnalysisOutcome(page_id: int, chunk_index: int, status: str, summary: str|None = None,
                category: str|None = None, tags: list[str] = [], error: str|None = None,
                attempt_count: int = 0)

@dataclass
BatchResult(outcomes: list[AnalysisOutcome] = [], quota_exhausted: bool = False)

class AnalyzerBackend(Protocol):
    analyze_batch(self, requests: list[AnalysisRequest]) -> BatchResult: ...
    close(self) -> None: ...

# tagging/pipeline.py                                  (Spark coordinator)
process_batch(task) -> dict                          # worker: cached backend per (backend, model), validates coverage
run(argv=None) -> int                                # driver: db.analyses.commit_batch, optional export_json
main()

# tagging/backends/__init__.py
create_backend(name, model)                          # "groq" or "module:attr" dynamic import

# tagging/backends/groq.py
class GroqBackend:                                   # implements AnalyzerBackend
    __init__(self, model)
    close(self)
    analyze_batch(self, requests)                    # rate-shaping + 5 bounded attempts + quota detection

# tagging/backends/groq_rules.py
is_noise_summary(summary: Optional[str], category: Optional[str]) -> bool
class ChunkAnalysis(BaseModel)                       # pydantic output schema: summary, category: CATEGORIES, tags
DEFAULT_GROQ_MODEL = "qwen/qwen3.8-27b"
```

- imports: `pipeline.py` → `db.analyses`, `jobs.batching`, `jobs.runtime`,
  `tagging.contracts` (direct); `tagging.backends` (worker-lazy);
  `groq.py` → `contracts`, `groq_rules` (direct)
- imported by: `scripts/tag.py`

### Driver ↔ worker contract

Task dict: `{"batch_id", "backend", "model", "items": [AnalysisRequest-shaped dicts], "result_bytes"}`
— worker result: `{"batch_id", "outcomes": [AnalysisOutcome dicts], "quota_exhausted": bool}`.

### Caveats — private helpers used across module boundaries

`groq.py` reaches into `groq_rules.py` underscore-prefixed internals:

```python
groq_rules._summary_problem, _is_daily_quota_error, _quota_wait_seconds,
groq_rules._NOT_ARTICLE_TAG_RE, _cap_tags, _filter_tags, _is_retryable
```

These are effectively public API. A future signature change to any of them must be
treated as a breaking change until they are renamed public.

Also: `is_noise_summary`'s docstring notes that `clean_results.py` holds a synced
copy of these noise rules — the two must be changed together.

---

## embeddings/ — BGE-M3 ONNX encoding (processing phase)

Best-documented module in the codebase (docstrings + `from __future__ import annotations`).

```python
# embeddings/__init__.py
from embeddings.embeddings import generate_embeddings
__all__ = ["generate_embeddings"]

# embeddings/embeddings.py
EMBEDDING_DIMENSION = 1024
DEFAULT_BATCH_SIZE = 1     # env EMBEDDING_BATCH_SIZE
DEFAULT_MAX_LENGTH = 256   # env EMBEDDING_MAX_LENGTH
model_version(max_length: int) -> str          # content-hash fingerprint of model assets
generate_embeddings(texts: list[str]) -> np.ndarray   # 1024-dim, L2-normalized, CLS-pooled

# embeddings/pipeline.py                              (Spark coordinator)
process_batch(task) -> dict
run(argv=None) -> int                          # wires model_version(max_length) as embedding_version
main()
```

- imports: `pipeline.py` → `db.embeddings` (as `embedding_store`), `jobs.batching`,
  `jobs.runtime`, `embeddings.embeddings` (direct); `generate_embeddings` (worker-lazy)
- imported by: `scripts/embed.py`, `retrieval.store`, `scripts/vector_search_test`,
  `scripts/watch_embeddings`

### Driver ↔ worker contract

Task dict: `{"rows": [{"page_id", "chunk_index", "text", "text_hash"}], "max_length", "result_bytes"}`
— worker result: `{"rows": [{"page_id", "chunk_index", "text", "text_hash", "embedding": [1024 floats]}]}`.

---

## retrieval/ — RAG layer (local Python, no Spark)

Fully typed (`from __future__ import annotations` everywhere); docstrings sparse
outside `llm.py` and `evidence.py`.

```python
# retrieval/__init__.py
__all__ = ["RagAgent", "RagResult", "answer"]

# retrieval/models.py        (all pydantic, strict, extra="forbid")
SearchQuery(id: str, query: str)
QueryPlan(queries: list[SearchQuery])
AgentDecision(action: Literal["answer"|"search_more"|"insufficient"], answer: str,
              citations: list[str], reason: str, queries: list[SearchQuery])
GroundedAnswer(answer: str, citations: list[str])
Source(page_id: int, url: str, title: str, publisher: str, author: str,
       language: str, published_date: str)
Evidence(text: str, chunk_ids: list[str], query_ids: list[str], reranker_score: float,
         similarity: float, sources: list[Source])
RagResult(question: str, answer: str, sufficient: bool, citations: list[str],
          sources: list[Source], queries: list[SearchQuery], evidence: list[Evidence],
          retrieval_rounds: int)

# retrieval/store.py
@dataclass(frozen=True)
RetrievalConfig(chunking_version, embedding_model, embedding_version,
                top_k=5, candidate_k=20, max_concurrent_searches=3)
RetrievalConfig.from_environment() -> RetrievalConfig
@dataclass
SearchHit(page_id, chunk_index, text_hash, text, similarity, query_id, source,
          reranker_score=None);  .chunk_id -> str
class VectorRetriever:
    __init__(self, config=None, embedder=generate_embeddings, reranker=None)
    async retrieve(self, queries: list[SearchQuery]) -> list[SearchHit]

# retrieval/ranking.py
build_reranker_inputs(queries: list[SearchQuery],
                      candidate_groups: list[list[SearchHit]]) -> list[tuple[str, str]]
validate_reranker_scores(scores, expected_count: int) -> np.ndarray
attach_reranker_scores(candidate_groups, scores) -> None
sort_and_limit_hits(groups: list[list[SearchHit]], top_k: int) -> list[list[SearchHit]]
rerank_query_groups(queries, candidate_groups, reranker: OnnxReranker, top_k) -> list[list[SearchHit]]

# retrieval/reranker.py
@dataclass(frozen=True)
RerankerConfig(model_dir: Path, max_length=512, batch_size=4, intra_op_threads=1) + from_environment()
sigmoid(logits: np.ndarray) -> np.ndarray
validate_logits(logits: np.ndarray, expected_count: int) -> np.ndarray
class OnnxReranker:                                  # thread-safe lazy load
    __init__(self, config: RerankerConfig | None = None)
    ensure_loaded(self) -> None
    score(self, pairs: list[tuple[str, str]]) -> np.ndarray

# retrieval/evidence.py
assemble_evidence(hits: list[SearchHit]) -> list[Evidence]   # dedup → merge contiguous runs → sort

# retrieval/llm.py
resolve_provider(model: str | None = None) -> str            # "groq" | "gemini"
resolve_model(provider: str, model: str | None = None) -> str
structured_output_kwargs(provider: str) -> dict
build_chat_model(provider: str, model: str)
retryable_errors() -> tuple[type[Exception], ...]

# retrieval/agent.py
class RagAgent:
    __init__(self, retriever=None, planner=None, decider=None, finalizer=None)
    async answer(self, question: str) -> RagResult   # plan → retrieve → decide → one corrective round → finalize
async def answer(question: str) -> RagResult         # module-level convenience wrapper
```

- imports: `store.py` → `chunking`, `db.db_handler`, `embeddings.embeddings`,
  `jobs.validation`, `models`, `ranking`, `reranker` (all direct);
  `ranking.py` → `reranker`, `store` (**type-only**); `evidence.py` → `models`, `store`;
  `agent.py` → `evidence`, `llm`, `models`, `store`; `llm.py`/`models.py` → external only
- imported by: `retrieval/__init__.py` ← `scripts/ask.py` (lazy)
- caveats: `ranking.py` mentioning `store`/`reranker` only under `TYPE_CHECKING` is a
  deliberate cycle breaker (`store` imports `ranking` at module level). The exact
  cosine search intentionally bypasses the HNSW index so a retriever can be replaced
  without touching the agent. `RerankerConfig`/`OnnxReranker` formatting style
  differs from the rest of the package.

---

## scripts/ — operational entry points

```python
# scripts/ingest.py / chunk.py / tag.py / embed.py    (4–6 lines each)
main()   # re-exported from the corresponding stage pipeline; ingest.py is the Spark entry point

# scripts/ask.py
main(argv=None) -> int    # loads .env, lazy-imports retrieval.answer, prints answer + evidence

# scripts/watch_embeddings.py
read_counts(chunking_version, model, embedding_version) -> dict
remaining(eligible, embedded, run_limit=None, starting_embedded=None) -> ...
main(argv=None)

# scripts/vector_search_test.py
resolve_config(conn, chunking_version, embedding_model, embedding_version) -> ...
search(query: str, top_k: int, chunking_version=None, embedding_model=None, ...) -> list
main()
```

None of the script functions carry docstrings or annotations except
`vector_search_test.resolve_config`. Shell launchers (`run_ingestion.sh`,
`run_*_processing.sh`, `inspect_db.sh`, `export_csv.sh`, `watch_embeddings.sh`) pair
with `jobs/environment.py` + `jobs/launch_config.py` as the launcher chain.

---

## query_service/ — placeholder

`__init__.py` is empty (0 lines) and imported by nothing. Reserved for the planned
API service (see `docs/rag-api-plan.md`). Treat as dead scaffolding, not API.

---

## Orphans and hygienic debt (summary)

| Item | Kind | Notes |
|---|---|---|
| `jobs.runtime` ⇄ `db.chunks`/`db.db_handler`/`chunking` | lazy-import cycle | load-bearing lazy imports; comment-contract recommended |
| `jobs.launch_config` ⇄ `ingestion.pipeline` | lazy-import cycle | same pattern |
| `retrieval.ranking` ┄ `retrieval.store`/`reranker` | type-only cycle | clean by design |
| `tagging.groq_rules` private helpers | cross-module private use | 7 `_` names imported by `groq.py` |
| `db/documents.py` | orphaned module | documented public loader, imported by nothing |
| `query_service/__init__.py` | empty placeholder | planned service |
| noise-rule duplication | logic copied in `clean_results.py` | "keep both in sync" contract |
