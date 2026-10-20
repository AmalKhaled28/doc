# News RAG Pipeline — Documentation

Baseline technical documentation for the **sparc_warc** project: a multilingual
(Arabic, English, French) news retrieval-augmented generation (RAG) system built on
Common Crawl CC-NEWS archives, Apache Spark on YARN, and PostgreSQL with pgvector.

This documentation set is the team's reference baseline. Every statement is traceable
to a file in this repository or is explicitly labelled as inferred, proposed, or
awaiting confirmation (see the [confidence legend](#3-confidence-legend)).

---

## 1. What the system does

The system turns raw web-archive files into a searchable, citable news knowledge base:

1. **Ingestion** streams CC-NEWS WARC files over HTTP, extracts article text and
   metadata, keeps Arabic, English, and French articles, and stores them in PostgreSQL.
2. **Chunking** cleans each article and splits it into overlapping text chunks, marking
   noise chunks as ineligible.
3. **Tagging** asks an LLM backend (Groq by default) for a summary, category, and tags
   per eligible chunk.
4. **Embeddings** encodes each eligible chunk with a local BGE-M3 INT8 ONNX model into a
   1024-dimensional vector stored with pgvector.
5. **Retrieval** answers natural-language questions: an LLM plans searches, pgvector
   returns candidates, a local cross-encoder reranks them, and the LLM writes an answer
   that cites chunk IDs.

All stages run as bounded, resumable batch commands today. See
[01-system-overview.md](01-system-overview.md) for the architecture.

## 2. Goals

| Goal | How the code meets it | Confidence |
|---|---|---|
| Collect multilingual news at a controlled volume | Global and per-file quotas count only newly committed articles ([ingestion/pipeline.py](../ingestion/pipeline.py), [db/db_handler.py](../db/db_handler.py)) | Verified in code |
| Never lose or double-count work | Transactional commits, chunk receipts, resumable checkpoints, idempotent stage keys | Verified in code |
| Keep worker memory bounded | Count and byte budgets on every Spark task and result ([jobs/batching.py](../jobs/batching.py)) | Verified in code |
| Allow reprocessing without data loss | Every derived table is keyed by a version (`chunking_version`, `analysis_version`, `embedding_version`) | Verified in code |
| Answer questions only from evidence | Citation validation against retrieved chunk IDs ([retrieval/agent.py](../retrieval/agent.py)) | Verified in code |
| Run continuously in production | Not implemented. Proposed design: an hourly cycle, with embeddings on Modal; see [01 §9](01-system-overview.md#9-prototype-and-production-at-a-glance) and [03 §11](03-cluster-design.md#11-prototype-vs-production-cluster) | Proposed (not implemented) |

## 3. Confidence legend

Statements, table cells, and diagram notes carry one of these labels where relevant.

| Label | Meaning |
|---|---|
| **Verified in code** | Read directly from a source, script, or config file in this repository. A file path is given. |
| **Recorded** | A measurement or observation written in [archive/validation.md](archive/validation.md) from a past run. It is not reproducible from code alone and may be out of date. |
| **Inferred** | A reasonable conclusion from code or config, or a standard default of a third-party tool that the repository does not set explicitly. Verify before relying on it. |
| **Recommended, not measured** | A sizing or configuration value derived from code settings. No benchmark in the repository backs it. |
| **Proposed (not implemented)** | Part of the agreed production target design. No code exists for it yet. |
| **\[\[FILL: \<what is missing\>\]\]** | Unknown. The team must answer it. Never treat it as fact. Every blank is listed, with the command that answers it, in [FILL-IN.md](FILL-IN.md). |

## 4. Quick start (prototype)

This is a condensed path for an engineer who already has a working Spark/YARN cluster
and a PostgreSQL database with pgvector. A full procedure from bare hardware is planned
for document 08 (not written yet).

**Prerequisites**

- Linux on the Spark master node. The code is Linux-only: [ingestion/pipeline.py](../ingestion/pipeline.py) imports the `resource` module, and every launcher is a Bash script. *(Verified in code)*
- `SPARK_HOME` pointing to the cluster's Spark installation, with `spark-submit`, `hdfs`, `zip`, `curl`, `tar`, and `sha256sum` on `PATH`. *(Verified in code: [scripts/run_processing.sh](../scripts/run_processing.sh))*
- An existing PostgreSQL database and role that can create tables and the `vector` extension. *(Verified in code: [db/vector_schema.py](../db/vector_schema.py))*

**Steps** (all commands appear in the repository's README or scripts)

```bash
# 1. Ingestion environment (also bootstraps every Spark launcher)
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements/base.txt
export SPARK_HOME=/opt/spark            # your Spark path
export PATH="$SPARK_HOME/bin:$PATH"

# 2. Database connection (or put these in the project-root .env file)
export PGHOST=localhost PGPORT=5432 PGDATABASE=warcdb PGUSER=warc_user PGPASSWORD='<secret>'

# 3. Validate configuration without contacting the cluster, then run a smoke ingestion
./scripts/run_ingestion.sh --dry-run --max-accepted-articles 1000
./scripts/run_ingestion.sh --num-files 1 --max-accepted-articles 10 --max-accepted-per-file 10

# 4. Chunking
.venv/bin/python -m pip install -r requirements/rag.txt
./scripts/run_chunking.sh --document-limit 100

# 5. Embeddings (dedicated environment + local model). The download script needs the base
#    model files in .models/bge-m3 first: config.json, modules.json, tokenizer.json,
#    tokenizer_config.json, sentencepiece.bpe.model, 1_Pooling/config.json.
#    Source of those files: [[FILL: how to obtain the base .models/bge-m3 files]]
bash scripts/setup_embedding_onnx_env.sh
bash scripts/download_quantized_embedding_model.sh
./scripts/run_embeddings.sh --embedding-limit 100

# 6. Ask a question
python3 -m venv .venv-rag
.venv-rag/bin/python -m pip install -r requirements/rag-agent.txt
bash scripts/download_quantized_reranker_model.sh
export GROQ_API_KEY='<secret>'
.venv-rag/bin/python scripts/ask.py "What happened, and why did it matter?"
```

**Expected output** (step 3, abridged; format from `_summary` in [ingestion/pipeline.py](../ingestion/pipeline.py))

```text
Created ingestion run 7dfd5c9d-deaa-41c6-b9fe-831216cf82c8
Resume command: RESUME_RUN_ID=7dfd5c9d-... ./scripts/run_ingestion.sh
Run 7dfd5c9d-...: complete; committed 10/10 new articles
  https://data.commoncrawl.org/crawl-data/CC-NEWS/2026/05/CC-NEWS-...warc.gz: 10 new; quota_reached
Counters: {"accepted_candidates": ..., "language_rejected": ..., "non_html": ..., "records": ...}
```

Exit codes: `0` complete, `1` operational failure, `2` the selected sources could not
meet the target (shortfall). *(Verified in code)*

**Troubleshooting**

| Symptom | Likely cause | Action |
|---|---|---|
| `Missing virtual environment: ... install requirements/base.txt in .venv or set VENV_DIR.` | `.venv` not created | Run step 1, or set `VENV_DIR` |
| `another ingestion coordinator is active for 2026/05` | Another ingestion driver holds the advisory lock | Wait for it to finish, or stop it |
| `Missing INT8 embedding model or tokenizer in ...` | Model not downloaded | Run `bash scripts/setup_embedding_onnx_env.sh`, then `bash scripts/download_quantized_embedding_model.sh` (step 5) |
| `Base model metadata is incomplete: .../.models/bge-m3/...` | Base model files missing | Put the six base files listed in step 5 into `.models/bge-m3` |
| `GROQ_API_KEY is required ...` | Key not exported or not in `.env` | Export `GROQ_API_KEY` |

## 5. Documentation map

| Document | Contents | Status |
|---|---|---|
| [01-system-overview.md](01-system-overview.md) | Purpose, scope, context and component diagrams, component map, tech stack | Batch 1 |
| [02-hardware-and-infrastructure.md](02-hardware-and-infrastructure.md) | Hardware evidence, sizing per node role, network and storage needs, estimation method | Batch 1 |
| [03-cluster-design.md](03-cluster-design.md) | Node roles, workload placement, ports, artifact distribution, scaling, failure handling | Batch 1 |
| [FILL-IN.md](FILL-IN.md) | Every open question for the team, with the place it belongs and how to answer it | Living list |
| 04-modules/ | One file per module: ingestion, chunking, tagging, embeddings, retrieval, db, jobs, scripts, query_service | Planned (not written yet) |
| 05-data-flow.md | End-to-end flow with payload formats and schemas | Planned (not written yet) |
| 06-database.md | Tables, keys, indexes, ER diagram, retention, read/write ownership | Planned (not written yet) |
| 07-configuration.md | Every environment variable, flag, config file, and secret | Planned (not written yet) |
| 08-deployment-and-running.md | Bare hardware to a running system, step by step | Planned (not written yet) |
| 09-operations.md | Monitoring, logs, troubleshooting, backup and recovery | Planned (not written yet) |
| 10-prototype-vs-production.md | Per-module comparison and migration path | Planned (not written yet) |
| 11-roadmap-and-open-questions.md | Known issues and limitations | Planned (not written yet) |

### Diagrams

Documents show each diagram once, as an SVG image from [diagrams/](diagrams/), so it
renders in any Markdown viewer. The Mermaid source of each diagram is a separate file
in [diagrams/src/](diagrams/src/), linked in the italic "Source:" note under the image.
The source file is the source of truth. After editing one, regenerate the SVGs from this
documentation folder:

```bash
bash diagrams/build.sh
```

**Prerequisites:** Node.js with `npx`. The first run downloads `@mermaid-js/mermaid-cli`
(11.12.0) and a headless Chromium. Set `MMDC=/path/to/mmdc` to use an existing
installation.

**Expected output:** one `built   diagrams/<name>.svg` line per diagram, then
`N diagram(s) built, 0 failed, 0 broken link(s).` The script also checks that every
diagram image and source link in the docs points to an existing file, and exits
non-zero if any is missing.

**Adding a diagram:**

1. Create `diagrams/src/<name>.mmd`, named `<doc number>-<topic>`.
2. Run the build script.
3. In the document, add the image link `![Title](diagrams/<name>.svg)` (use `../diagrams/` from `04-modules/`), the italic source note, and a 2–3 sentence explanation.

Do not put Mermaid code blocks in the `.md` files.

### Detailed legacy references

The documents below predate this baseline and were moved, unchanged in content, to
[archive/](archive/). They hold useful detail, but some statements are outdated. A
Known Issues list is planned for document 11 (not written yet). Where this baseline and
an archived document disagree, this baseline wins.

| Legacy document | Topic |
|---|---|
| [archive/architecture.md](archive/architecture.md) | Ingestion internals, transactions, memory bounds, module interaction graph |
| [archive/module_map.md](archive/module_map.md) | Function signatures and driver/worker payload contracts |
| [archive/workflows.md](archive/workflows.md) | Workflow and state-machine diagrams |
| [archive/processing.md](archive/processing.md) | Chunking, tagging, and embedding job behavior |
| [archive/validation.md](archive/validation.md) | Recorded local and YARN validation runs (2026-09-30 and 2026-10-01) |
| [archive/rag-api-plan.md](archive/rag-api-plan.md) | Plan for the RAG evaluation baseline and FastAPI service |
| [archive/retrieval-chatbot-design.md](archive/retrieval-chatbot-design.md) | Retrieval and chatbot design decisions |
| [archive/embedding-runtime-consistency.md](archive/embedding-runtime-consistency.md) | Embedding drift across CPU instruction sets, the proposed move of embeddings to Modal with an AVX2-only ONNX Runtime wheel, and the hourly-cycle plan (2026-10-20; copied unchanged from the team's `docs/`) |

## 6. Glossary

Terms are used with exactly these meanings across all documents.

| Term | Definition |
|---|---|
| **CC-NEWS** | Common Crawl's news crawl. A monthly manifest (`warc.paths.gz`) lists its WARC files. |
| **WARC file / record** | Web ARChive file (gzip-compressed). Each record is one captured HTTP response. Ingestion requires one record per gzip member. |
| **Manifest ID** | `YYYY/MM` string identifying the monthly CC-NEWS manifest, for example `2026/05`. |
| **Stage** | One of the four Spark jobs: `ingest`, `chunk`, `tag`, `embed`. Retrieval is not a stage; it is a local query path. |
| **Driver** | The Spark driver process. It runs on the machine that invoked the launcher (YARN client mode) and owns every PostgreSQL read and write. |
| **Executor / worker** | A Spark executor in a YARN container on a worker node. It runs pure transforms and never connects to PostgreSQL. |
| **Task** | A bounded unit of work sent to one executor: a dict with items and byte or count limits. |
| **Wave** | A set of up to N tasks submitted concurrently. The driver commits results as each completes, then submits the next wave. |
| **Ingestion run** | One logical ingestion job with its own quotas, identified by `run_id` (UUID) in `ingest_runs`. It can be resumed. |
| **Ingestion chunk** | One ingestion task's result (candidate articles, next offset, statistics) and its commit receipt in `ingest_chunks`. Not related to text chunks. |
| **Text chunk** | A fragment of an article produced by chunking, stored in `article_chunks`. Identified by `(page_id, chunking_version, chunk_index)`. |
| **Chunk ID** | Citation form of a text chunk: `p<page_id>:c<chunk_index>`, for example `p12:c3`. |
| **Candidate** | An article that passed extraction, size, and language checks on an executor but is not yet committed. |
| **Checkpoint** | A saved compressed byte offset at a WARC record boundary (`ingest_run_files.next_offset`, `ingest_files.bytes_done`). |
| **Quota** | Target count of newly committed articles: global (`MAX_ACCEPTED_ARTICLES`) and per file (`MAX_ACCEPTED_PER_FILE`). `0` means unlimited. |
| **Materialization** | Writing all chunks of one article for one `chunking_version`, recorded in `chunk_materializations`. |
| **Eligible chunk** | A text chunk with `eligible = true` (no `filter_reason`). Only eligible chunks are tagged and embedded. |
| **Analysis identity** | `(chunking_version, backend, model, analysis_version)`, the key under which tagging results are stored. |
| **Embedding identity** | `(embedding_model, embedding_version)`. `embedding_version` is a fingerprint of the model files and `max_length`. |
| **Coordinator lock** | A PostgreSQL session advisory lock that allows one active driver per stage. |
| **Dead letter** | JSONL file in `dead_letter/` (or `DEAD_LETTER_DIR`) holding article rows the database rejected. |
| **Evidence** | Retrieved, reranked, deduplicated chunk text with its sources, passed to the answering LLM. |
| **Prototype mode** | How the system runs today: manually launched, bounded batch commands. |
| **Production mode** | The proposed target: continuous ingestion, scheduled processing, an HTTP query service, and full operations tooling. Not implemented. |
