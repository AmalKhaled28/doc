# Embedding Runtime Consistency on Modal

**Status:** findings and decision record  
**Date:** 2026-10-20  
**Scope:** BGE-M3 INT8 embeddings for the continuous WARC news pipeline  
**Code state:** no runtime/pipeline changes were applied. This document is the
hand-off for the next model; it describes what was measured, what was decided,
and what still needs to be implemented.

---

## 1. Purpose

The project will run continuously: every hour it checks for new WARC data,
queues new links, and runs ingestion, chunking, tagging, and embedding. Modal is
intended to be the permanent embedding tier.

The original plan was to use stock ONNX Runtime on Modal and pin one warm
container to an AVX2-only host. That is adequate for a one-off re-embed, but it
is not a safe permanent design. This document records why, what the drift
actually looks like, and the agreed direction: **use an AVX2-only ONNX Runtime
wheel in the Modal image instead of pinning hosts.**

No code changes were made as part of this investigation. The next model should
implement the wheel integration and versioning changes described below.

---

## 2. TL;DR

- Stock ONNX Runtime dispatches different MLAS kernels on AVX2 and
  AVX-512/VNNI hosts. ONNX Runtime does not guarantee cross-machine
  reproducibility.
- Measured same-text drift for the current BGE-M3 INT8 model:
  - cosine mean `0.98452276`
  - cosine min `0.98068875`
  - cosine max `0.98820132`
  - max absolute component difference up to `2.059e-02`
- The drift is **not catastrophic semantically**. The same text still ranks
  itself first about 99.7% of the time in a read-only simulation. Top-5 results
  are mostly stable, but near-ties do move.
- The real problem is **index consistency**, not a single query’s quality. If
  one hourly batch lands on AVX-512 while another lands on AVX2, the database
  silently contains vectors from two kernel paths under the same
  `embedding_version`.
- Modal exposes CPU cores, memory, region, cloud, and GPU type, but **no CPU
  model or ISA selector**. Host pinning is a lottery; `modal container stop`
  can produce another AVX-512 host. A permanent hourly pipeline cannot rely on
  winning that lottery.
- Full-precision ONNX, PyTorch FP32, or a different quantized runtime does not
  guarantee a fix. They can reduce the magnitude, but the ISA-dependent kernel
  dispatch remains. Only a single-ISA build or fixed hardware gives a real
  guarantee.
- **Decision:** use the AVX2-only ONNX Runtime wheel on Modal. Scale normally;
  do not pin hosts. Use the same effective runtime identity for corpus and
  query embeddings.

---

## 3. Current project context

- Model: BGE-M3 INT8 ONNX.
- Preprocessing: CLS pooling, `max_length=256`, batch size `1`, L2
  normalization.
- Embedding dimension: `1024`.
- Database: PostgreSQL with pgvector on `localhost`; Modal cannot write to it
  directly.
- Architecture: the driver owns DB reads/writes; Spark executors perform
  inference and return vectors. The same pattern must hold for Modal: the
  local driver reads pending chunks and commits vectors; Modal stays stateless.
- Local query service: `retrieval/store.py` calls the local
  `generate_embeddings()` by default.
- Local workspace host: Intel i5-9500, AVX2 only (no `avx512f` /
  `avx512_vnni` flags). Verify every Spark worker with `lscpu`; do not assume a
  heterogeneous cluster is AVX2-only.
- As of the measurement date, the local database contained `911` stored
  embeddings and `10,145` eligible chunks. The full corpus is therefore larger
  than the simulation sample.

### Existing versioning gap

`embeddings/embeddings.py::model_version()` currently hashes:

- the model file
- the tokenizer files
- the pooling configuration
- the preprocessing label (`cls-l2-raw-text-v1`)
- `max_length`

It does **not** include:

- ONNX Runtime version or wheel build
- effective CPU kernel/ISA path
- batch size
- intra-op thread count

`embeddings/pipeline.py` always computes `model_version()` and ignores an
explicit `EMBEDDING_VERSION`. `retrieval/store.py` *does* honor
`EMBEDDING_VERSION`. That mismatch must be fixed before continuous operation,
otherwise the corpus job and the query service can use different vector
identities, or a runtime change can be written under the same identity.

---

## 4. What was measured

### 4.1 Cross-host drift

The comparison used stock ONNX Runtime `1.30.0`, the same model/tokenizer, and
the canonical preprocessing.

| Metric | Value |
|---|---|
| Cosine mean | `0.98452276` |
| Cosine min | `0.98068875` |
| Cosine max | `0.98820132` |
| Max absolute component difference | `2.059e-02` |
| Same-text parity on an AVX2 host | exact: `cos=1.0`, `max_abs=0` |

Hosts observed:

- Passing AVX2 host: AMD EPYC family 25 model 1.
- Failing stock hosts: AMD EPYC family 25 model 17 and Intel family 6 model 85,
  both `avx512_vnni`.

The cause is that the CPU execution provider selects different MLAS kernels.
INT8 U8S8/VNNI paths also differ in saturation and accumulation behavior.
ONNX Runtime’s position is that cross-machine reproducibility is not promised
([ORT #12086](https://github.com/microsoft/onnxruntime/issues/12086)).

### 4.2 Retrieval sensitivity simulation

A read-only simulation used the `911` stored vectors and `300` sampled queries.
Isotropic noise was calibrated to a same-text cosine of `0.9845`; this is a
sensitivity estimate, not a replay of real AVX-512 vectors.

| Scenario | Top-1 retained | Top-5 slots unchanged | Queries with any top-5 change |
|---|---:|---:|---:|
| Query drift only | `~99.7%` | `~97.3%` | `~12.7%` |
| Corpus drift only | `~99.7%` | `~96.5%` | `~17.7%` |
| Both sides drift | `~99.7%` | `~95.2%` | `~23.3%` |

Top-20 candidate sets were more sensitive than top-5 sets, but the reranker
only sees candidates that survive retrieval. A relevant document can still
drop out before reranking.

Score-gap observations from the same sample:

- Top-1 vs top-2 gap: median `0.2419`.
- Top-5 vs top-6 gap: median `0.0112`.
- `58.7%` of top-5/top-6 gaps were below `0.015`.
- Median pairwise similarity in the sample was about `0.35`; top-20 median was
  about `0.49`.

This explains the apparent contradiction: `0.9845` is a small semantic
perturbation relative to clear relevance gaps, but it is comparable to the
score gaps at the top-5 boundary. Most changes are near-ties; a minority can be
real retrieval changes.

### 4.3 Interpretation

- `max_abs = 0.02` is a per-coordinate difference on normalized vectors. It
  should not be read as “2% semantic difference”; cosine aggregates over 1024
  coordinates.
- The same text remains nearest to itself almost always; coarse retrieval is
  robust.
- The corpus is only partly populated in the sample. A denser full corpus will
  have more near-ties, so top-k churn could be somewhat higher.
- The risk that matters for a permanent pipeline is **silent mixing of kernel
  paths**, not the absolute size of one vector difference.

---

## 5. Options considered

| Option | Fixes cross-host drift? | Notes |
|---|---|---|
| **AVX2-only ONNX Runtime wheel** | **Yes** | Forces the same AVX2/FMA3 kernels on every host. Works with scale-to-zero and multiple containers. |
| Fixed AVX2 hardware | **Yes** | Dedicated VM, Kubernetes node affinity, or a fixed local worker. Reliable but not serverless. |
| Stock ORT + host pinning | **No guarantee** | `min_containers=max_containers=1`, 24/7 warm, watchdog, fail-closed, and full re-embed if the host changes. Replacement is a lottery. |
| Stock ORT + multiple hosts | **No** | Explicitly mixes kernels. Not acceptable for one vector identity. |
| ONNX FP32 | Reduces, probably a lot; not guaranteed | Same MLAS dispatch, no integer quantization. AVX2/AVX-512 still differ in FMA width and reduction order. |
| PyTorch FP32 | Reduces, not guaranteed | ATen/MKL/oneDNN still dispatch by ISA. `ATEN_CPU_CAPABILITY=avx2` was insufficient in the PyTorch report; CPUID masking was needed. |
| PyTorch/other INT8 quantization | **No** | Different quantized kernels, same ISA problem. Could be better or worse than ONNX INT8. |
| FP16/BF16 on CPU | **No, often worse** | Native support is ISA-dependent; some hosts emulate. |
| GPU | **No** | Different nondeterminism and hardware paths; also expensive. |
| `ONEDNN_MAX_CPU_ISA=AVX2` | Partial | Only covers oneDNN kernels. Stock ORT CPU EP uses MLAS; oneDNN EP requires a custom build. |
| `MKL_CBWR=AVX2` / `MKL_ENABLE_INSTRUCTIONS=AVX2` | Partial | MKL-only; does not cover MLAS. |
| CPUID masking / VM CPU model | **Yes** | `libvirtcpuid` or QEMU/KVM can hide AVX-512. Reliable but requires controlling the VM; Modal does not expose this. |
| Accept drift + strict versioning | Quality may be acceptable | Still requires one vector space per `embedding_version`; does not allow arbitrary host mixing. |

### Why full precision is not the fix

The root cause is not quantization alone. It is that different CPUs execute
different vectorized kernels and accumulate floating-point operations in
different orders. Quantization amplifies the difference; removing it reduces
the magnitude but does not remove the ISA-dependent path.

Practical costs of switching to FP32 or PyTorch:

- BGE-M3 FP32 is roughly `2.3 GB` versus `~570 MB` for the INT8 ONNX graph.
- Modal image size, cold start, and memory requirements increase; a 4 GB
  container may be tight.
- CPU inference is slower, likely several times slower than INT8.
- PyTorch adds a large runtime and breaks the deliberately lean ONNX embedding
  environment (`torch`/`sentence-transformers` are intentionally absent).
- Switching precision changes the vector space; the corpus and queries must be
  re-embedded.

**Do not switch to FP32 just for consistency.** If FP32 is chosen for quality,
still use a single-ISA build or fixed hardware, and measure it.

---

## 6. Decision

**Use the AVX2-only ONNX Runtime wheel in the Modal image. Do not use host
pinning for the permanent pipeline.**

Reasons:

1. It makes vector generation independent of the CPU model Modal schedules.
2. It allows scale-to-zero between hourly runs.
3. It allows `max_containers > 1` when throughput requires it.
4. It survives container eviction and replacement without a watchdog or manual
   re-pin.
5. It makes the embedding version honest: model + preprocessing + effective
   runtime can be fingerprinted together.

The pin script was useful for the one-off investigation, but it should not be
part of the continuous production path.

---

## 7. Implementation plan (not applied)

### 7.1 Wheel

- Candidate wheel:
  `.local-wheels/onnxruntime-1.30.0-cp312-cp312-linux_x86_64.whl`
- SHA256:
  `3410d300f5a3916465b919f82f34437a53a9c83c2f30940bd94200bef1ef67ca`
- Built from ONNX Runtime `v1.30.0` with MLAS AVX-VNNI, AVX-512, and AMX
  dispatch blocks disabled.
- Build script: `scripts/modal_ort_build.py` (writes to the `warc-ort-build`
  Modal Volume).
- The wheel is `cp312`; the Modal image uses Python `3.12`, so it fits.
- Local `.venv`, `.venv-embedding-onnx`, and `.venv-rag` are Python `3.13.5`,
  so this wheel will **not** install locally. Local stock ONNX Runtime on an
  AVX2-only host is equivalent for query use, but validate parity. If a local
  wheel is needed, build a `cp313` wheel or use a Python `3.12` embedding venv.
- The wheel has not yet been validated on both AVX2 and AVX-512 Modal hosts.
  Do that before writing corpus vectors.

Possible install shape for `scripts/modal_embedding_app.py`:

```python
image = (
    modal.Image.debian_slim(python_version="3.12")
    .apt_install("libgomp1")
    .uv_pip_install(
        "fastapi[standard]>=0.115,<1",
        "numpy==2.5.3",
        "transformers==5.18.0",
        "tokenizers==0.23.2",
        "sentencepiece==0.2.0",
        "py-cpuinfo==9.0.0",
    )
    .add_local_file(
        ".local-wheels/onnxruntime-1.30.0-cp312-cp312-linux_x86_64.whl",
        "/tmp/onnxruntime-avx2.whl",
        copy=True,
    )
    .pip_install("/tmp/onnxruntime-avx2.whl")
    .add_local_dir(".models/bge-m3-int8", remote_path="/model", copy=True)
)
```

The app should expose a stable runtime identity, for example:

- `ORT_BUILD = "avx2-only-1.30.0"`
- `RUNTIME_ID = "ort-1.30.0-avx2"`
- `/health` and `/debug` return both.

The current app name is `warc-embedding-parity`; a production name such as
`warc-embeddings` would be clearer, but renaming changes the endpoint URL.

### 7.2 Version identity

Add the effective runtime to the fingerprint. Suggested identity components:

- preprocessing label / pooling
- model and tokenizer hashes
- `max_length`
- batch size (`1`)
- intra-op threads (`1`)
- ONNX Runtime version
- wheel hash or logical runtime ID

Suggested environment/config:

```bash
EMBEDDING_BACKEND=modal
EMBEDDING_RUNTIME_ID=ort-1.30.0-avx2
# Optional explicit override; leave blank to compute from files + runtime.
EMBEDDING_VERSION=
```

Requirements:

- `embeddings/pipeline.py` must honor an explicit `EMBEDDING_VERSION`, not only
  `model_version()`.
- `retrieval/store.py` and `scripts/watch_embeddings.py` must use the same
  resolution helper.
- The corpus job and query service must use the same `EMBEDDING_RUNTIME_ID`.
- If the runtime changes, the embedding version changes and the corpus is
  re-embedded under the new identity. Never mix kernels under one version.
- Include batch size and thread count, or enforce `1` for both. The current
  Spark launcher sets `EMBEDDING_INTRA_OP_THREADS=1`; local query settings must
  match.

### 7.3 Modal client

The current pipeline calls `generate_embeddings()` in Spark executors and never
calls the Modal endpoint. A production integration needs a small client, e.g.
`embeddings/backend.py`:

- `EMBEDDING_BACKEND=local|modal`
- Modal client posts to `/v1/embeddings`
- proxy bearer token from `MODAL_PROXY_TOKEN`
- bounded retries for 429/5xx/network errors
- validate count, shape `(n, 1024)`, finite values, L2 norms
- call `/debug` once per worker and fail closed if:
  - `ort_build` is not the expected AVX2 build
  - `runtime_id` is not the expected runtime
  - model/tokenizer SHA256 do not match the pinned values
- commit behavior stays idempotent; retries may repeat a batch but must not
  duplicate rows

If called from Spark executors, pass the endpoint and token through executor
environment configuration. The token is a secret; follow the existing Groq
pattern and Spark redaction. If hourly volume is small, calling Modal from the
driver or a small local worker avoids distributing the token.

### 7.4 DB ownership

Keep the existing driver-owned pattern:

1. local driver reads pending chunks from PostgreSQL
2. driver/executor sends text to Modal
3. Modal returns vectors
4. local driver commits to `chunk_embeddings`

Do not try to connect Modal to `localhost` PostgreSQL. Do not run the DB reembed
job inside the Modal web container. The Modal endpoint should be stateless.

### 7.5 Hourly operation

- The existing `db/chunks.py::acquire_lock` is a global per-stage advisory
  lock. A second embedding run fails with “another embedding coordinator is
  active.” Ensure the hourly run finishes under an hour, or make the scheduler
  wait/skip with an alert.
- A full backfill under a new `embedding_version` will also hold the embedding
  lock. Schedule it separately, or change the lock key to include the version.
- A long-lived local worker polling pending rows may be simpler and cheaper
  than launching Spark every hour; it also keeps the model/HTTP client warm.
- If using Modal scale-to-zero, expect a cold start each hour. With the wheel,
  cold starts are safe. Use `min_containers=1` only if latency justifies the
  cost.
- Start with `max_containers=1` and measure throughput. The current endpoint
  serializes `_embed()` with a lock and sends one text at a time; multiple
  containers or a revised concurrency model may be needed for volume.

### 7.6 Backfill and cutover

1. Freeze the old embedding identity.
2. Build and validate the wheel.
3. Deploy the wheel-backed Modal app.
4. Set the new `EMBEDDING_RUNTIME_ID` / optional `EMBEDDING_VERSION`.
5. Backfill the whole corpus under the new identity.
6. Keep serving the old identity for retrieval while backfill runs.
7. Switch the query service to the new identity only after the backfill is
   complete and recall checks pass.
8. Do not mix old and new vectors in one query.

---

## 8. Validation plan

### 8.1 Wheel parity

Before any corpus write:

1. Deploy the wheel-backed app on an AVX2 host.
2. Deploy/restart until an AVX-512 host is selected.
3. Call `/debug` on both and confirm:
   - `ort_build` is the AVX2-only build
   - `runtime_id` is stable
   - model/tokenizer hashes match
4. Embed a fixed text set on both hosts.
5. Compare against the local AVX2 baseline:
   - expected exact or near-exact vectors
   - if drift remains, the wheel did not fully constrain the kernel path;
     stop and investigate
6. Run `scripts/modal_embedding_parity.py` and
   `scripts/modal_embedding_compare.py` read-only.

### 8.2 Retrieval A/B

The simulation above is directional, not a benchmark. For a definitive answer:

- choose a fixed query set with known relevant chunks
- embed with AVX2 stock, AVX-512 stock, and AVX2-only wheel
- run the actual retriever and reranker
- compare top-5 hits, citations, and recall@k
- decide whether any remaining drift is acceptable for the product

This can be done read-only; no DB writes are required.

---

## 9. Current repo state and cleanup notes

- No runtime/pipeline code changes were applied for this investigation.
- The four one-off probe scripts were removed after the investigation:
  - `scripts/local_ort_options_probe.py`
  - `scripts/modal_build_probe.py`
  - `scripts/modal_gpu_probe.py`
  - `scripts/modal_ort_options_probe.py`
- The remaining new files are:
  - `scripts/modal_embedding_app.py` (production candidate)
  - `scripts/modal_embedding_compare.py` (useful validation)
  - `scripts/modal_embedding_parity.py` (useful validation)
  - `scripts/modal_ort_build.py` (required to rebuild the wheel)
  - `scripts/modal_pin_avx2.py` (obsolete for production; keep only while
    validating the wheel)
- All Modal apps observed during the investigation were stopped.
- No DB writes were made.
- `.local-wheels/` and `.models/` are gitignored.
- Current Modal app name: `warc-embedding-parity`.
- Current endpoint placeholder:
  `https://<workspace>--warc-embedding-parity-web.<region>.modal.run`.
- Current Modal app still uses stock `onnxruntime==1.30.0`; it has not yet been
  switched to the wheel.
- The current app has no runtime ID field; `/health` reports `ort_build` and
  model metadata only.

Remaining cleanup after the wheel integration is implemented:

- remove `modal_pin_avx2.py` once the wheel has been validated on both AVX2 and
  AVX-512 hosts; update the docstring reference in
  `scripts/modal_embedding_app.py` first
- keep `modal_ort_build.py` for reproducible wheel rebuilds
- keep `modal_embedding_parity.py` and `modal_embedding_compare.py` as
  read-only validation tools; `modal_embedding_compare.py` can be removed after
  cutover if it is no longer useful
- rename the app from `warc-embedding-parity` to a production name if the
  endpoint change is acceptable

---

## 10. Open questions for the next model

1. Is the local query service going to keep using stock ONNX Runtime on the
   AVX2-only host, or will it also call Modal? If local, validate that local
   stock AVX2 exactly matches the wheel before sharing a runtime ID.
2. What is the expected hourly chunk volume? This determines whether one
   container is enough and whether `min_containers=0` cold starts are
   acceptable.
3. Should the embedding lock be per-version so a full backfill can run
   alongside hourly ingestion?
4. Is an explicit `EMBEDDING_VERSION` preferred over automatic model/runtime
   fingerprinting for operational control?
5. Does the product need bitwise reproducibility for tests, or is a
   recall-based tolerance sufficient? The current evidence suggests the latter
   is reasonable, but a full retriever/reranker A/B should confirm it.

---

## 11. References

- ONNX Runtime reproducibility issue:
  https://github.com/microsoft/onnxruntime/issues/12086
- PyTorch AVX-512 reproducibility / `ATEN_CPU_CAPABILITY`:
  https://github.com/pytorch/pytorch/issues/155552
- `libvirtcpuid` CPU feature masking:
  https://github.com/twosigma/libvirtcpuid
- oneDNN CPU dispatcher control:
  https://uxlfoundation.github.io/oneDNN/dev_guide_cpu_dispatcher_control.html
- ONNX Runtime build options (`onnxruntime_USE_AVX512`):
  https://github.com/microsoft/onnxruntime/blob/main/cmake/CMakeLists.txt
- ONNX Runtime quantization / U8S8 / VNNI notes:
  https://onnxruntime.ai/docs/performance/model-optimizations/quantization.html
- Modal resource configuration:
  https://modal.com/docs/guide/resources
- Modal region selection:
  https://modal.com/docs/guide/region-selection
- Embedding drift / versioned re-embedding:
  https://tianpan.co/blog/2026/04/16/embedding-drift-silent-semantic-search-degradation
- Valori deterministic memory substrate (research; does not solve embedding
  inference nondeterminism):
  https://arxiv.org/html/2512.22280v1
