# Module: `ingestion/`

[Modules](README.md) · [Docs home](../README.md) · Related: [14 — Article metadata and deduplication](../14-article-metadata-and-deduplication.md) · [13 — Ingestion operations](../13-ingestion-operations.md)

---

> **Code baseline.** `master` at `d794b8c`. Differences on `airflow_migration` at
> `2769828` are marked **Branch**. Statements are *Verified in code* unless labelled
> otherwise.

## 1. Purpose

Turns one CC-NEWS monthly manifest into rows in PostgreSQL. Executors download, parse,
extract, and classify. The driver is the only transaction coordinator, and it counts
only rows that PostgreSQL really inserted.

## 2. Files

| File | Responsibility |
|---|---|
| [ingestion/pipeline.py](../../ingestion/pipeline.py) | Configuration, manifest selection, task allocation, the wave loop, the worker entry point, run summary |
| [ingestion/warc.py](../../ingestion/warc.py) | FastWARC parsing with a bounded payload, and gzip validation (`ValidatedGzipStream`) |
| [ingestion/extraction.py](../../ingestion/extraction.py) | HTML field extraction, language detection, URL normalization |
| [ingestion/text.py](../../ingestion/text.py) | Decoding, encoding checks, Arabic validation, the target language rule |

Entry point: `scripts/ingest.py` calls `ingestion.pipeline.main`. All settings are in
[07 §4](../07-configuration.md#4-ingestion-settings).

## 3. Run lifecycle

1. `load_config` reads flags, then the environment, then defaults. A set `MAX_EXTRACTED_PER_ROUND` is an error.
2. With a dry run, the configuration is printed and nothing else happens.
3. `init_db` applies the schema, and `acquire_coordinator_lock` takes the global ingestion lock.
4. A new run calls `fetch_manifest` and `create_run`. A resumed run calls `load_run`, `restore_run_config`, and `resume_run`.
5. The wave loop allocates tasks, runs them, commits each result, and repeats.
6. When no task is left, the run gets a final status and `_summary` prints the totals.

| Run status | Meaning | Exit code |
|---|---|---|
| `running` | In progress, or the driver died | — |
| `complete` | Target reached, or no target and no failed files | 0 |
| `shortfall` | Sources ended before the target | 2 |
| `failed` | Failed files remain, or 3 blank failed waves in a row | 1 |

No WARC file selected also exits with 2, and no run is created.

| File status (`ingest_run_files.status`) | Meaning |
|---|---|
| `pending` | More data to read |
| `eof` | Read to the end |
| `quota_reached` | The per-file target was met; the file stays available to later runs |
| `failed` | The last task for this file failed |

## 4. Manifest and file selection

`fetch_manifest` downloads
`https://data.commoncrawl.org/crawl-data/CC-NEWS/<YYYY>/<MM>/warc.paths.gz`, skips
`START_FILE_OFFSET` entries, and keeps `NUM_FILES` of the rest. `NUM_FILES` values `all`,
`none`, `0`, and an empty string select everything.

**Branch:** the `NUM_FILES` default is `all`. On `master` it is `15`.

`create_run` copies earlier progress for the same manifest from `ingest_files`, so a new
run skips finished files and continues from saved offsets. Offsets are reused only when
`checkpoint_version` is 2 or higher.

## 5. Task allocation

`_allocate_tasks` takes the first `MAX_FILES_PER_ROUND` files that are `pending` and not
at EOF. Each task gets a candidate allowance:

- With no global target: `WORKER_MAX_CANDIDATES`.
- With a global target: `min(WORKER_MAX_CANDIDATES, ceil(remaining ÷ slots left))`, and at least 1.
- With a per-file target: also capped by what that file may still add. An allowance of 0 skips the file.

The `chunk_id` is a SHA-256 of the run ID, file URL, offset, and wave. It is the receipt
that stops a retried commit from inserting twice.

## 6. Worker behavior

`process_file_chunk` runs on an executor. It never touches the database.

**Download.** It opens the URL with a `Range` header when the offset is not 0, using a
120-second timeout and the user agent `spark-warc/2`. It accepts HTTP 200 and 206.

| Check | Result on failure |
|---|---|
| `Content-Range` matches the saved offset and runs to the end of the file | `IOError` |
| `ETag` and `Last-Modified` match the values saved for the file | `IOError` |
| The saved offset is inside the file | `IOError` |
| Each record is its own gzip member | `IOError`: offsets could not be resumed |
| The offset is a real record boundary | `IOError` |
| The gzip stream is complete, and its size matches `Content-Length` | `IOError` from `ValidatedGzipStream` |

If a server ignores `Range`, the worker replays from byte 0 and skips records before the
saved offset.

**Stop conditions.** A task ends at the first of these, checked between records, so the
input and time limits can overshoot by one record:

- `candidate_limit` candidates collected
- `WORKER_MAX_RESULT_BYTES` of serialized candidates
- `WORKER_MAX_INPUT_BYTES` of compressed input
- `WORKER_MAX_SECONDS` elapsed
- End of file

If the next candidate would overflow the result budget, the task ends before that record
and the next task starts on it.

**Retries.** Transient errors retry up to `MAX_RETRIES` attempts with a backoff of
`min(30, 2^(n+1))` seconds. Transient means HTTP 408, 429, or 5xx, a timeout, or a
connection or OS error. After that, the worker returns a result with `error` set, no
candidates, and an unchanged offset.

## 7. Filters, in order

| Step | Rule | Counter |
|---|---|---|
| Record type | Must be a WARC `response` with `text/html` or `application/xhtml` | `non_html` |
| HTML size | At most `MAX_HTML_BYTES` of decoded payload | `oversized_html` |
| Decoding | See [§8](#8-decoding-and-encoding-checks) | `too_short_or_corrupt` |
| Length | At least `MIN_WORD_COUNT` words of extracted text | `too_short_or_corrupt` |
| Language | Must be `ar`, `en`, or `fr` ([§9](#9-language-detection)) | `language_rejected` |
| Article size | Serialized candidate at most `MAX_ARTICLE_BYTES` | `oversized_article` |
| Duplicates | Same `record_id` within one task is skipped | — |

Other counters: `records`, `accepted_candidates`, `result_bytes`, `elapsed_seconds`,
`compressed_bytes_read`, `python_peak_rss_mib`, `worker_host`, `worker_pid`, and
`attempts`. They are stored in `ingest_chunks.stats`.

## 8. Decoding and encoding checks

`decode_and_validate` tries these in order and stops at the first success:

1. The declared charset, strict.
2. UTF-8, strict.
3. `charset_normalizer`, rejecting a Latin-1 guess with coherence below 0.8.
4. `windows-1256`, strict.

`is_clean_encoding` rejects text with U+FFFD or NUL, with at least 3 mojibake sequences
that cover more than 1% of the text, or with more than 0.1% control characters. It runs on
the decoded HTML and again on the extracted text.

## 9. Language detection

`detect_languages_batch` uses Lingua with 20 preloaded languages.

| Step | Rule | Result |
|---|---|---|
| Sample | Whole text up to 2,000 characters; otherwise 800 from the start, 600 from the middle, and 600 from the end | — |
| Too short | Under 20 characters | `unknown` |
| No known script | No Arabic, Latin, Cyrillic, or CJK letters in the sample | `ignored` |
| Confidence | Below `LINGUA_MIN_CONFIDENCE` | `other` |
| Script share | For `ar`, `en`, `fr`: over 10% of letters in unsupported scripts | `other` |
| Arabic validity | Fails `is_valid_arabic_text` | `corrupted` |

`is_valid_arabic_text` reads the first 500 characters. At least 40% of the letters must be
Arabic. Two or more Persian, Urdu, or Sorani marker letters, at over 2% of the Arabic
letters, fail the check.

## 10. Field extraction

`extract_html_fields` reads metadata first, because later steps remove elements.

| Field | Source, in order |
|---|---|
| Body text | Trafilatura without comments or tables, when `USE_TRAFILATURA=1` and it gives at least `MIN_WORD_COUNT` words. Otherwise selectolax body text after removing `script`, `style`, `noscript`, `header`, `footer`, `svg`, and `nav` |
| `extraction` | `trafilatura` or `selectolax` |
| Title | `<title>`, then JSON-LD `headline`, then `N/A` |
| Published date | Meta tags `article:published_time`, `pubdate`, `publishdate`, `date`, `DC.date.issued`, `parsely-pub-date`; JSON-LD `datePublished` or `dateCreated`; `<time>`; a date found in the body text; `N/A` |
| Author | Meta tags `author`, `article:author`, `parsely-author`, `dc.creator`, `author_name`; JSON-LD `author`; `N/A` |
| `record_id` | The WARC record ID. Otherwise `gen-` plus a SHA-256 of the URL and the first 500 characters |

The remaining fields are in [14 §3](../14-article-metadata-and-deduplication.md#3-new-database-columns),
and the `canonical_key` rules are in [14 §5](../14-article-metadata-and-deduplication.md#5-canonical-url-rules).
`arabic_dialect` is always `NULL`.

## 11. Commit

`commit_chunk` runs in one transaction. The full contract is in
[db.md](db.md#3-ingestion-writes). In short: a repeated `chunk_id` returns the earlier
count, and a chunk that would exceed its quota is an error. Rows the database rejects go
to `dead_letter/`.

A failed commit retries up to `MAX_RETRIES` attempts. Each retry reconnects and takes the
lock again. After the last attempt the run is marked `failed` and the driver exits 1.

## 12. Resume

`--resume-run-id` restores these saved settings: `MAX_ACCEPTED_ARTICLES`,
`MAX_ACCEPTED_PER_FILE`, `YEAR`, `MONTH`, `NUM_FILES`, `START_FILE_OFFSET`,
`MIN_WORD_COUNT`, `USE_TRAFILATURA`, `LINGUA_MIN_CONFIDENCE`, `MAX_HTML_BYTES`, and
`MAX_ARTICLE_BYTES`.

A flag or exported variable that differs from a saved value is an error. A changed `.env`
value does not block a resume, because `SPARK_WARC_EXPORTED_ENV_KEYS` records what was
set before `.env` loaded. Resource budgets and concurrency can change. Resuming also
returns `failed` files to `pending`.

Failed-file recycling inside one run is in
[13 §3](../13-ingestion-operations.md#3-failed-file-recycling-in-the-ingestion-driver).
