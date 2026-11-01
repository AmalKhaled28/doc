# Module: `retrieval/`

[Modules](README.md) · [Docs home](../README.md) · Query diagram: [01 §4.2](../01-system-overview.md#42-query-plane)

---

> **Code baseline.** `master` at `d794b8c`. Statements are *Verified in code* unless
> labelled otherwise.

## 1. Purpose

Answers one question from stored articles, with citations. It runs as a local process in
`.venv-rag`, not on Spark. It only reads the database.

## 2. Files

| File | Responsibility |
|---|---|
| [retrieval/agent.py](../../retrieval/agent.py) | `RagAgent`, the prompts, citation validation, `answer()` |
| [retrieval/store.py](../../retrieval/store.py) | `RetrievalConfig`, `VectorRetriever`, the search SQL |
| [retrieval/ranking.py](../../retrieval/ranking.py) | Reranker inputs, score checks, sorting |
| [retrieval/reranker.py](../../retrieval/reranker.py) | `OnnxReranker`, the local cross-encoder |
| [retrieval/evidence.py](../../retrieval/evidence.py) | Deduplication and merging of hits |
| [retrieval/llm.py](../../retrieval/llm.py) | Provider and model selection |
| [retrieval/models.py](../../retrieval/models.py) | Pydantic contracts |

Entry points: `scripts/ask.py`, and `from retrieval import answer`, which is asynchronous.
Settings are in [07 §6](../07-configuration.md#6-retrieval-settings).

## 3. Flow

1. **Plan.** The LLM returns one search query, or two or three when separate evidence is needed. Duplicates are removed, ignoring case, and at most 3 are kept. No query is an error.
2. **Search.** Queries are embedded in one batch. Each one runs an exact cosine search for `RERANKER_CANDIDATE_K` candidates, with up to `RAG_DB_CONCURRENCY` searches at a time.
3. **Rerank.** The cross-encoder scores every (query, chunk) pair. The best `TOP_K` per query stay.
4. **Assemble evidence.** See [§6](#6-evidence-assembly).
5. **Decide.** The LLM returns `answer`, `search_more`, or `insufficient`.
6. **Follow up.** `search_more` runs one more search round with new queries, then a final answer step. `insufficient` with evidence also goes to the final answer step.
7. **Validate.** Citations are checked and the result is built.

There are at most two search rounds.

## 4. LLM providers

| Provider | Default model | Key | Settings |
|---|---|---|---|
| `groq` | `openai/gpt-oss-120b` | `GROQ_API_KEY` | `temperature=0`, `max_tokens=2400`, `max_retries=2`, `reasoning_effort` from `RAG_REASONING_EFFORT` (default `medium`), strict `json_schema` |
| `gemini` | `gemini-3.5-flash-lite` | `GEMINI_API_KEY`, then `GOOGLE_API_KEY` | `max_output_tokens=8192`, `max_retries=2`, `json_schema` without `strict` |

`RAG_PROVIDER` forces a provider. Otherwise a `RAG_MODEL` starting with `gemini`, `gemma`,
`models/gemini`, or `models/gemma` selects Gemini, and anything else selects Groq. A blank
`RAG_MODEL` uses the provider default.

**Keep `RAG_REASONING_EFFORT` at `medium` or higher for `gpt-oss-120b`.** At `low`, the
model ignores the answer-language rule and follows the language of the evidence.

A failed structured output gets one retry. Reasoning effort is set only for Groq.

## 5. Search and ranking

The search SQL filters on `chunking_version`, `embedding_model`, `embedding_version`, and
`eligible`, and orders by cosine distance, then page and chunk. It scans every vector of
that version. The HNSW index uses `vector_ip_ops`, so this query does not use it. See
[03 §9](../03-cluster-design.md#9-scaling-considerations).

**Cross-encoder.** `bge-reranker-v2-m3` INT8 ONNX. Pairs are tokenized together, truncated
at `RERANKER_MAX_LENGTH`, and scored in batches of `RERANKER_BATCH_SIZE`. Logits pass
through a sigmoid, giving a score from 0 to 1.

Hits within a query sort by reranker score, then cosine similarity, then page and chunk.

**No fallback.** A missing model, a wrong output shape, or a non-finite score stops the
request. There is no fall back to cosine-only ranking.

## 6. Evidence assembly

`assemble_evidence` turns hits into evidence blocks:

1. The same chunk found by several queries becomes one hit, keeping every query ID and the highest scores.
2. Hits next to each other in one article merge into one block. The largest exact overlap between them is removed.
3. Blocks with identical text merge, which also joins articles republished word for word, and keep all their sources.
4. Blocks sort by reranker score, then cosine similarity.

Only retrieved chunks are joined: neighbours that were not retrieved are never added.

## 7. Answer rules

Both answer prompts require:

- **Language:** the language of the question, never the language of the evidence. If the question is in a dialect, match it. The rule is also repeated at the end of the prompt.
- **Length:** at most 180 words.
- **Citations:** inline chunk IDs such as `[p12:c3]`, copied into `citations`.
- **Evidence only:** no outside facts or invented citations.
- **Partial answers:** a broad question can be answered with one example, stating that the archive does not establish a complete list.

## 8. Citation validation

`_result` accepts a citation only if it appears in the retrieved evidence. It reads both
the `citations` list and the IDs in the answer text.

A result is `sufficient` only with action `answer`, non-empty text, and at least one valid
citation. Otherwise the answer becomes `Insufficient evidence: <reason>`. Invalid IDs in
the answer text stay in the text, but they are left out of `citations` and `sources`.

## 9. Result

| `RagResult` field | Meaning |
|---|---|
| `question`, `answer` | The trimmed question and the final answer |
| `sufficient` | See [§8](#8-citation-validation) |
| `citations` | Valid chunk IDs |
| `sources` | Sources of the cited pages, each with `page_id`, `url`, `title`, `publisher`, `author`, `language`, `published_date` |
| `queries` | All search queries used |
| `evidence` | All evidence blocks, cited or not |
| `retrieval_rounds` | 1 or 2 |

`publisher` here is the site domain. The richer `metadata.publisher` field is not read by
this query.

`scripts/ask.py` prints the answer, the sources, and all evidence with its scores. It
exits 0 when the answer is sufficient and 1 otherwise.

## 10. Memory

The process holds the BGE-M3 graph and the reranker graph, each about 570 MB, plus their
tokenizers. *(Recorded in the project README)*
