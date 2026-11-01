# Module: `embeddings/`

[Modules](README.md) · [Docs home](../README.md) · Version keys: [01 §7](../01-system-overview.md#7-version-keys-connecting-the-stages)

---

> **Code baseline.** `master` at `d794b8c`. Statements are *Verified in code* unless
> labelled otherwise.

## 1. Purpose

Encodes each eligible chunk into a 1024-dimensional, L2-normalized vector with a local
BGE-M3 INT8 ONNX model. The same code embeds questions at query time.

## 2. Files

| File | Responsibility |
|---|---|
| [embeddings/embeddings.py](../../embeddings/embeddings.py) | Model loading, `generate_embeddings`, `model_version` |
| [embeddings/pipeline.py](../../embeddings/pipeline.py) | Spark job |
| [embeddings/__init__.py](../../embeddings/__init__.py) | Exports `generate_embeddings` |

Entry point: `scripts/embed.py`. Settings are in
[07 §5](../07-configuration.md#5-processing-stage-settings).

## 3. Model loading

`_load_model` runs once per Python process and refuses a model it cannot prove is right:

| Check | Error when it fails |
|---|---|
| `onnx/model_int8.onnx` exists | `FileNotFoundError` |
| `1_Pooling/config.json` selects CLS pooling, not mean pooling | `RuntimeError` |
| The model has a `last_hidden_state` output of dimension 1024 | `RuntimeError` |
| The model accepts `input_ids` and `attention_mask` | `RuntimeError` |

The session uses `CPUExecutionProvider`, sequential execution, one inter-op thread, and
`EMBEDDING_INTRA_OP_THREADS` intra-op threads. The tokenizer loads local files only.

## 4. Encoding

`generate_embeddings(texts)` tokenizes with padding and truncation at
`EMBEDDING_MAX_LENGTH`, runs the model, takes the first token (CLS), and normalizes to
unit length. Text longer than `max_length` tokens is cut, with no warning. It returns a
`float32` array of shape `(n, 1024)`.

## 5. Identity

`embedding_version` fingerprints the model files and `max_length`. Its contents are in
[01 §7](../01-system-overview.md#7-version-keys-connecting-the-stages). A changed model
file or `max_length` gives a new version, and new rows.

The fingerprint does not cover the CPU instruction set. See the warning in
[02 §3.1](../02-hardware-and-infrastructure.md#31-known-and-unknown-specifications).

## 6. Flow

1. The driver fixes `embedding_version` once, at the start of the run.
2. It streams eligible chunks with no vector for this identity. Each chunk's hash is checked as it is read.
3. Tasks hold up to 10 chunks and 2 MiB of input. Each result may be up to 1 MiB.
4. The executor returns the vector with the original text and hash.
5. The driver validates and commits each batch.

## 7. Commit checks

`commit_batch` in [db/embeddings.py](../../db/embeddings.py) raises an error on any of
these, and writes nothing from that batch:

- Duplicate chunk identities in the batch
- A `text_hash` that does not match the text
- A vector that is not 1024 numbers, has a non-finite value, or has a norm outside `1 ± 0.001`
- A source chunk that changed or is no longer eligible
- An existing vector row with a different `text_hash`

Inserts use `ON CONFLICT DO NOTHING`, so a repeated batch is harmless.

## 8. Environment

The stage uses its own `.venv-embedding-onnx`, without PyTorch or Sentence Transformers.
`scripts/setup_embedding_onnx_env.sh` fails if either package is present. The model
archive travels through the HDFS cache and unpacks in each container as
`./embedding-model`. See [03 §5](../03-cluster-design.md#5-code-environment-and-model-distribution).

The Modal investigation tools are in
[scripts.md](scripts.md#6-modal-tools). They do not change this stage: embedding still
runs on Spark.
