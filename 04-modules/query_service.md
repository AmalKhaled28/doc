# Module: `query_service/`

[Modules](README.md) · [Docs home](../README.md)

---

`query_service/` holds one empty `__init__.py` on `master` and on `airflow_migration`. It
has no code. *(Verified in code)*

It is reserved for the HTTP API, which is **Proposed (not implemented)**. The plan is a
FastAPI service that wraps `retrieval.answer()`. See
[archive/rag-api-plan.md](../archive/rag-api-plan.md) and
[10 — Prototype vs production](../10-prototype-vs-production.md).

Until then, ask questions with `scripts/ask.py`, or call `retrieval.answer()` from Python.
See [retrieval.md](retrieval.md).
