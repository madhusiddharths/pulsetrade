# TODOS

Deliberately deferred work, captured with enough context to pick up cold.

## Offline / sample-data demo mode

**What:** a seeded sample-data path so `docker compose up` + one `curl` produces a
real investigation with zero cloud accounts (no Gemini / Databricks / Tavily keys).

**Why:** today a keyless evaluator can only watch the README demo, not run it —
the config fails fast (by design) without real keys. An offline mode is the
Stripe-tier hello world: it works forever, even after free-tier credits die.

**How (pickup context):**
- Entry point: a `DEMO_MODE=1` env flag read in `api/config.py` (relax the
  required `Field(...)`s when set).
- Fixtures: canned gold-window rows + a recorded investigation response; store
  under `api/tests/fixtures/` so they double as unit-test data.
- Pattern to imitate: `drift/gold_data.py` already has an `inject_drift=True`
  demo mode.
- Label stubbed LLM output clearly in the dashboard (e.g. "demo replay") so it
  never reads as fake proof.

**Depends on:** nothing — independent of the day-13 GKE work.
**Effort:** human ~2 days / with a coding agent ~1-2 hours.
