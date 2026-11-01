# Module: `tagging/`

[Modules](README.md) · [Docs home](../README.md) · Cost and duration: [02 §6.4](../02-hardware-and-infrastructure.md#64-llm-tagging-cost-and-duration)

---

> **Code baseline.** `master` at `d794b8c`. Statements are *Verified in code* unless
> labelled otherwise.

## 1. Purpose

Asks an LLM for a summary, a category, and tags for each eligible chunk. Results are
stored per analysis identity: `(chunking_version, backend, model, analysis_version)`.

## 2. Files

| File | Responsibility |
|---|---|
| [tagging/pipeline.py](../../tagging/pipeline.py) | Spark job and outcome validation |
| [tagging/contracts.py](../../tagging/contracts.py) | `AnalysisRequest`, `AnalysisOutcome`, `BatchResult`, and the `AnalyzerBackend` protocol |
| [tagging/backends/__init__.py](../../tagging/backends/__init__.py) | `create_backend` |
| [tagging/backends/groq.py](../../tagging/backends/groq.py) | The Groq adapter: pacing, retries, classification |
| [tagging/backends/groq_rules.py](../../tagging/backends/groq_rules.py) | The prompt, the output schema, and validation rules |

Entry point: `scripts/tag.py`. Settings are in
[07 §5](../07-configuration.md#5-processing-stage-settings).

## 3. Backends

A backend is any class with `analyze_batch(requests)` and `close()`, taking the model name
in its constructor. `create_backend` accepts:

- `groq`: the included adapter.
- `module:attribute`: any importable class. The tests use `tests.fake_backend:FakeBackend`.

A backend other than `groq` must be given a model (`--model` or `TAGGING_MODEL`). Executors
keep one backend per `(backend, model)` for the life of the Python worker.

The driver checks each batch. It rejects unknown, duplicate, or invalid outcomes, and a
missing outcome is allowed only when the batch reports `quota_exhausted`.

## 4. Output schema

| Field | Rule |
|---|---|
| `summary` | 1–2 sentences, in the language of the chunk |
| `category` | One of the 16 categories below |
| `tags` | 3–6 specific terms from the chunk, in its language |

| Category | Covers |
|---|---|
| `Politics` | Government and diplomacy, including government positions on a war |
| `Economy` | Economic topics |
| `Sports` | Sports |
| `Technology` | Technology |
| `Science` | Space, physics, astronomy, research findings |
| `Environment` | Climate, pollution, forests, rivers, wildlife |
| `Health` | Medicine, public health, nutrition science |
| `Culture` | Arts, entertainment, celebrities, books, festivals |
| `Religion` | Prayer, fiqh, religious holidays, sermons |
| `Society` | Social issues that are not mainly crime |
| `Crime` | Police, arrests, courts, fraud |
| `Security` | Military operations, battlefield events, weapons, terrorism, cyber security |
| `Lifestyle` | Beauty, horoscopes, fashion, relationship advice, consumer tips, deals |
| `Food` | Recipes, dishes, food labeling, restaurant rules |
| `Weather` | Weather |
| `Other` | Used only if nothing fits |

The prompt tells the model to use only the chunk, to treat the title as context only, and
to add no names, dates, or numbers. Non-article text must start with `NOT_ARTICLE:`, use
`Other`, and take no tags from menus.

## 5. Processing one chunk

For each request, the adapter makes up to 5 attempts:

1. Wait for the pacing interval ([§6](#6-pacing)).
2. Call the model with `max_tokens=600`. The first attempt uses temperature 0 and retries use 0.3.
3. Reject a bad summary, which counts as a retryable error.
4. Classify the result as `NOISE` or `SUCCESS`.

**Bad summaries:**

| Problem | Rule |
|---|---|
| Empty | No text |
| Leaked syntax | Contains `<summary>`, `<parameter`, `"category":`, `"tags":`, `"summary":`, `` ```json ``, `<|`, or similar |
| Foreign script | Contains Cyrillic, Thai, Korean, Japanese, or Chinese letters |
| Arabic in English or French | Arabic letters in a summary for an `en` or `fr` chunk |
| Truncated | Does not end in sentence punctuation |

**Outcome status:**

| Status | Meaning | Stored |
|---|---|---|
| `SUCCESS` | Usable analysis | Summary, category, filtered tags |
| `NOISE` | The model says this is not article content | Summary without the `NOT_ARTICLE` prefix; no category or tags |
| `ERROR` | Attempts ran out, or the error was not retryable | Error text, up to 2,000 characters |

A summary is noise when it starts with `NOT_ARTICLE`; or when the category is `Other` and
the summary says so in its own words; or when it opens by describing "the text" and calls
it non-article content.

**Tag filtering.** A tag stays if one of its words of 3 or more characters appears in the
chunk. The Arabic prefix `ال` is ignored. If fewer than 2 tags survive, the original list
is kept. The list is capped at 6.

## 6. Pacing

Each executor waits for the largest of these between requests:

- `TAG_REQUEST_INTERVAL_SECONDS`, default 1.5
- `60 × TAG_PARALLELISM ÷ TAG_REQUESTS_PER_MINUTE`, when set
- `60 × TAG_PARALLELISM × estimated tokens ÷ TAG_TOKENS_PER_MINUTE`, when set

The estimate is the UTF-8 byte length of the prompt plus 600. This shapes traffic only;
it is not a shared quota counter, so a 429 from Groq remains the real limit.

## 7. Error handling

| Error | Behavior |
|---|---|
| HTTP 401, 403, 404 | `RuntimeError`: the job fails, because the setup is wrong |
| HTTP 429, 500, 502, 503, 504, or a retryable message | Wait, then retry. The wait is the time in the message plus 5 seconds, otherwise `5 × 2^attempt`, and never less than `retry-after` |
| Wait over 15 minutes | `quota_exhausted`: the batch stops |
| Message has "tokens per day" or "(TPD)" | `quota_exhausted`: the batch stops |
| Any other error | Recorded as `ERROR`; no retry |

With `quota_exhausted`, completed outcomes are committed, later waves stop, and the driver
exits 2. API keys are removed from stored error text.

## 8. Commit and retry

`commit_batch` inserts outcomes with `ON CONFLICT`. An existing row changes only if it is
`ERROR` and comes from a different `batch_id`, so `SUCCESS` and `NOISE` are final for an
analysis identity. `attempt_count` adds up across batches.

`ERROR` rows are not retried by default. Add `--retry-errors` to select them again.

`--export-json <path>` writes the analyses for this identity to a JSON array, through a
temporary file and an atomic rename.

## 9. Differences from the constants

`RETRY_TEMPERATURE` and `CHUNK_MAX_TOKENS` in `groq_rules.py` are unused, because
`groq.py` repeats those values. The docstring of `is_noise_summary` mentions a
`clean_results.py` copy of the rules, which is not in the repository. See
[11](../11-roadmap-and-open-questions.md).
