# Retrieval and Chatbot Design

This document describes the bounded retrieval and answering workflow implemented
in `retrieval/` and exposed by `scripts/ask.py`.

## Decisions

- Retrieval is vector-only. Do not add keyword search, BM25, PostgreSQL full-text
  search, `LIKE`, `ILIKE`, `tsvector`, or keyword-derived score adjustments.
- The retrieval service returns ranked evidence and provenance. The separate
  chatbot module generates answers.
- Do not put an LLM intent router in front of every search.
- Do not implement rule-based query decomposition. It becomes brittle across
  Arabic, English, and French and attempts to decide what evidence is needed before
  seeing the first retrieval result.
- The chatbot may produce one to three self-contained semantic search queries. It
  can decompose a multi-part question when useful.
- The chatbot may request one additional retrieval round after identifying the
  specific evidence that is missing.
- Use LangChain Core prompt pipelines plus ordinary Python and `asyncio` for the
  bounded workflow. LangGraph is deferred until durable checkpoints, human
  approval, multiple tools, or more complicated loops justify it.
- Do not import the sibling fake retriever or its current retry loop. Repeating the
  same deterministic search without changing the query or search parameters is not
  corrective retrieval.

## Component responsibilities

### Retrieval service

The retrieval service is deterministic and does not decide user intent. It:

1. Accepts one to three search queries.
2. Embeds all queries in one batch with the existing BGE-M3 implementation.
3. Searches the versioned `chunk_embeddings` rows through exact pgvector cosine
   distance.
4. Returns the top results for each query, preserving the query-to-result mapping.
5. Deduplicates repeated chunks while retaining every query that matched them.
6. Returns chunk text, similarity, stable chunk identity, URL, title, language,
   publisher, author, and publication date.

Every search must use the same model files, maximum token length, pooling,
normalization, `embedding_model`, and computed `embedding_version` used to create
the stored chunk vectors. Searches must also select the configured
`chunking_version` and eligible chunks only.

### Chatbot module

The chatbot owns semantic planning and answering. It:

1. Keeps the original user question unchanged in its state.
2. Produces one query for a simple question or up to three self-contained queries
   when separate evidence is required.
3. Examines the grouped evidence and either answers or names the missing aspects.
4. May request one targeted follow-up search round.
5. Generates an answer using returned evidence and cites stable chunk identifiers.
6. Reports insufficient evidence when the allowed retry still does not provide the
   required support.

The chatbot must not answer from an unsupported draft and then use its own prose as
the retrieval query. Follow-up queries must describe evidence missing from the
original question.

## Request flow

```mermaid
flowchart TD
    U[User question] --> P[Chatbot creates 1-3 search queries]
    P --> E[BGE-M3 embeds the query batch]
    E --> V[Parallel pgvector searches]
    V --> G[Grouped and deduplicated evidence]
    G --> A{Chatbot evidence assessment}
    A -->|Sufficient| R[Answer with citations]
    A -->|Specific evidence missing| Q[Create targeted follow-up queries]
    Q --> E2[BGE-M3 embeds follow-up batch]
    E2 --> V2[Parallel pgvector searches]
    V2 --> G2[Merge evidence]
    G2 --> F{Final assessment}
    F -->|Sufficient| R
    F -->|Still insufficient| S[Return insufficient evidence]
```

Keep results grouped by search query until the chatbot has checked coverage. A
single global ranking can allow an easy sub-question to consume every result and
leave another part without evidence.

## Model responsibilities

| Task | Mechanism |
|---|---|
| Query and chunk embeddings | Local BGE-M3 bi-encoder |
| Corpus candidate ranking | pgvector inner-product similarity |
| Query decomposition | Chatbot LLM, only when it chooses multiple searches |
| Missing-evidence analysis | Chatbot LLM after seeing retrieved chunks |
| Follow-up query generation | Chatbot LLM, at most one round |
| Answer generation | Separate chatbot LLM |
| Candidate reranking | Multilingual BGE cross-encoder over the top pgvector candidates |
| Answer grounding | Optional multilingual NLI cross-encoder in the chatbot module |

A bi-encoder embeds queries and chunks independently, which makes corpus search
efficient. A reranking cross-encoder reads a query and candidate chunk together and
produces a more precise relevance score, but it requires one inference per pair,
so it is applied only to a small pgvector candidate set (`RERANKER_CANDIDATE_K`,
default 20) and the best `TOP_K` per query are kept.

A grounding cross-encoder performs a different task. It reads an answer claim and
its cited evidence together and predicts entailment, neutrality, or contradiction.
It can determine whether the evidence supports the claim; it cannot establish that
the source itself is factually true. Reranking and grounding models are deferred
until labeled evaluations demonstrate a need.

## Structured agent contract

The chatbot's search decisions are Pydantic contracts produced through LangChain
structured output. Application code validates the query and round limits before
calling retrieval.

```json
{
  "queries": [
    {
      "id": "comparison",
      "query": "differences between policy A and policy B"
    },
    {
      "id": "replacement_reason",
      "query": "why policy B replaced policy A"
    }
  ]
}
```

After retrieval, the chatbot returns either an answer action or a targeted retry:

```json
{
  "action": "search_more",
  "answer": "",
  "citations": [],
  "reason": "The comparison is supported, but the replacement reason is missing.",
  "queries": [
    {
      "id": "replacement_reason_retry",
      "query": "stated reasons policy B replaced policy A"
    }
  ]
}
```

These actions are validated Pydantic models. The application invokes retrieval and
asks the chatbot for the next structured action. The model never receives database
credentials or executes code.

## Concurrency

For a batch of decomposed queries:

1. Call `generate_embeddings()` once with the full list. This is more efficient
   than starting one ONNX invocation per query.
2. Run the synchronous ONNX batch in an owned coordinator worker so it does not
   block callers and command-line shutdown does not depend on asyncio's global
   executor.
3. Start one pgvector search per query with `asyncio.gather()`.
4. Because the project uses synchronous `psycopg2`, run each search in that
   executor and open a separate connection for it. Never share a connection or
   cursor between concurrent searches.
5. Bound database search concurrency with the executor's worker count.

Initial operational limits are:

```text
maximum queries per round:       3
maximum retrieval rounds:        2
maximum concurrent DB searches:  3
embedding inference concurrency: 1 batch per process
```

The initial implementation uses exhaustive `vector <=> query_vector` ordering.
The schema's existing `vector_ip_ops` HNSW index remains untouched but is not used
by this exact cosine query. Switching to approximate indexed retrieval is confined
to the retrieval store.

These limits must be enforced by application code rather than accepted from model
output. The service also controls `top_k`, timeouts, database filters, and permitted
metadata fields.

## Sibling implementation disposition

The sibling `query-ret-optimization` work is a prototype rather than a production
retrieval implementation:

- Its query service demonstrates LLM rewrite, expansion, and decomposition, but a
  separate LLM intent router is unnecessary in the agreed design.
- Its retrieval service returns hard-coded chunks through `FakeRetriever`.
- Its CRAG evaluator judges the combined context as sufficient or insufficient; it
  does not rank or rerank chunks.
- Its retry is simulated and does not perform a meaningfully different search.

Do not copy `fake_retriever`, the existing CRAG service loop, the separate query
HTTP API, generated evaluation results, manual test scripts, or reports. Individual
prompts and structured-output utilities may be used as references, but the new
retrieval module should be implemented around the current PostgreSQL schema,
BGE-M3 embedding code, and pgvector index.

## Deferred features and evaluation gates

The BGE cross-encoder reranker is implemented as the second retrieval stage:
pgvector returns `RERANKER_CANDIDATE_K` candidates per generated query, the
cross-encoder scores each `(query, chunk)` pair, and the best `TOP_K` per query are
kept. Its scores are not stored in PostgreSQL.

Before treating reranking as production-ready, create a multilingual labeled set of
real questions and expected chunks and record at least Recall@20, reranked MRR@5
and Recall@5, per-language results, reranking latency per candidate set, end-to-end
latency, and peak process RSS.

Add a grounding checker only when unsupported chatbot claims remain a measured
problem. Add LangGraph only when the bounded Python state machine can no longer
express the required workflow cleanly.
