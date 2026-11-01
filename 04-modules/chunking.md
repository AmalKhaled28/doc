# Module: `chunking/`

[Modules](README.md) · [Docs home](../README.md)

---

> **Code baseline.** `master` at `d794b8c`. Statements are *Verified in code* unless
> labelled otherwise.

## 1. Purpose

Cleans each article, splits it into overlapping chunks, and marks noise chunks as
ineligible. Ineligible chunks stay in the database with a reason, but tagging, embedding,
and search skip them.

## 2. Files

| File | Responsibility |
|---|---|
| [chunking/__init__.py](../../chunking/__init__.py) | `CHUNKING_VERSION` = `recursive-800-100-clean-v2.4.1-v1` |
| [chunking/pipeline.py](../../chunking/pipeline.py) | Spark job: `run` on the driver, `process_batch` on executors |
| [chunking/preprocessing.py](../../chunking/preprocessing.py) | `clean_article_text` and `noise_reason` |
| [chunking/splitters.py](../../chunking/splitters.py) | `iter_document_chunks`, `split_documents_into_chunks` |
| `chunking/requirements.txt` | Points to `requirements/rag.txt`, for compatibility only |

Entry point: `scripts/chunk.py`. Settings are in
[07 §5](../07-configuration.md#5-processing-stage-settings).

## 3. Flow

1. The driver streams articles with no `chunk_materializations` row for this version.
2. Tasks hold up to 25 documents and 2 MiB of input.
3. The executor cleans, splits, flags, and hashes each document.
4. The driver commits each document in its own transaction.

## 4. Cleaning

`clean_article_text` does three things:

1. **Removes a site header.** If `Advertisement` appears in the first 2,500 characters, and the text before it has `Privacy Policy`, `Se connecter`, or the Arabic login text, everything up to and including that word is dropped.
2. **Cuts at a link list.** The text ends at the first of `Read more »`, `Similar News:`, `United States Latest News`, or one Arabic newspaper marker.
3. **Removes a trailing copyright line**, such as `© 2026 …`.

## 5. Splitting

`RecursiveCharacterTextSplitter` with `chunk_size=800`, `chunk_overlap=100`, and the
separators `\n\n`, `\n`, `.`, space, and the empty string. Sizes are in characters.
`chunk_index` starts at 0 for each article.

Changing these values, the cleaning rules, or the filters means a new
`CHUNKING_VERSION`. A changed result under the old version raises a drift error
([§7](#7-commit-and-drift)).

## 6. Noise filters, in order

`noise_reason` returns the first reason that applies, and `None` means the chunk is
eligible.

| Reason | Rule |
|---|---|
| `short` | Under 250 characters (`MIN_CHUNK_CHARS`), or a boilerplate phrase in the first 160 characters (legal disclaimers, "skip to content", "all rights reserved", and their French forms) |
| `blocked` | Domain is `sixactualites.fr`, `lg.com`, `realting.com`, or `kinoafisha.info` |
| `non_article_url` | URL contains `/publi-`, `/sponsored`, `advertorial`, `/partner-content`, `/forum/`, or `/forums/` |
| `linklist` | Chunk still contains a cut marker from [§4](#4-cleaning) |
| `bio` | Reads like an author biography, or has NPR's "See stories by" or copyright line |
| `headlines` | Three or more `°C` values; or five or more `(123)` counters; or at least 500 characters with at most 1 sentence ending and at most 2 commas |
| `garbled` | See below |
| `aggregator` | Repeated timestamp patterns in the first 500 characters, or a repeated `Lebanon 24` label |

**`garbled`** checks the first 600 characters, for samples of at least 40 characters. A
chunk fails if mojibake matches exceed 3% of the sample, or if too few letters fit the
article's language: under 50% for Arabic, under 70% for English and French.

The current snapshot has no `linklist` rows
([02 §6.1](../02-hardware-and-infrastructure.md#61-prototype-snapshot-baseline-units)):
cleaning removes those markers first *(Inferred)*.

## 7. Commit and drift

`commit_document` validates and then writes:

- Chunk indexes must be contiguous from 0.
- Every `text_hash` must match the SHA-256 of `chunk_text`.
- If a receipt or rows exist, they must match the new result. A difference raises `chunking drift for page N; use a new version`.
- Otherwise it inserts the chunks and the `chunk_materializations` receipt. An article with no chunks still gets a receipt with `chunk_count = 0`.

`source_hash` is a SHA-256 of the whole source dictionary: page ID, text, URL, title, and
language.

## 8. Failure behavior

`spark.task.maxFailures` is 1. A failed task fails the job after finished sibling tasks
are committed, and the driver exits 1. Rerunning continues with the articles that have no
receipt.
