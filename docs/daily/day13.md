# Day 13 — Ingress + CI/CD + the completion session

**Goal:** finish the build. Verify the split-ingress fix on a fresh ephemeral
GKE cluster, ship the deferred GitHub Actions CI/CD, capture the evidence the
repo has been promising, and tear everything down to a verified $0.

## Blocks 1-2 (done earlier): ingress in the Helm chart

Added `templates/ingress.yaml` — one public entry point routing by path:
`/api/*` → api:8000 with the prefix rewritten away, `/` → dashboard:8501.

## Block 3: the websocket-rewrite bug → two Ingresses

First deploy of the single-Ingress version served a **blank Streamlit page**.
Root cause: `nginx.ingress.kubernetes.io/rewrite-target` is a *per-Ingress*
annotation — it applies to every path rule in the same Ingress. The `/$2`
rewrite that strips `/api` was also mangling the dashboard's websocket path
(`/_stcore/stream`), so the browser's live connection died.

Fix: **two Ingress resources sharing one ingressClass** (one controller, one
load balancer, one IP). The rewrite lives only on the API Ingress; the
dashboard Ingress passes paths through untouched, with long proxy read/send
timeouts for the persistent websocket.

- [ ] _Verified live on cluster (Phase B of the completion session)_

## Block 4: CI/CD (`.github/workflows/`)

- `ci.yml` (badged): ruff → offline pytest unit set → helm lint + render guard
  → amd64 image builds; on main with `GCP_SA_KEY` configured, pushes
  `:sha`/`:latest` to Artifact Registry. Steps self-skip when the key is absent
  so the badge survives teardown.
- `deploy.yml` (manual only, badge-free): `helm upgrade` against the ephemeral
  cluster — demonstrated once, live, during the session.
- New offline unit tests (`api/tests/unit/`, `drift/tests/`) stub the heavy
  integrations (mlflow/sklearn/evidently) and test the pure logic: z-score
  labeling, chronological split, drift-dict extraction, config env contract.
- Lint pass turned up real debt: dead `_get_llm()` in `agent/nodes.py`
  referenced a function that no longer exists (pre-refactor leftover) — deleted.

## Block 5: the live session (evidence + teardown)

_To be filled during the session — see [runbook-gke.md](../runbook-gke.md) for
the exact commands._

- [ ] Cluster up, 3 pods `1/1`
- [ ] `http://<IP>/api/health` + `/api/ready` OK through the rewrite
- [ ] Dashboard at `http://<IP>/` with live websocket (no blank page)
- [ ] Investigation end-to-end via the public URL
- [ ] `deploy.yml` run green against the live cluster
- [ ] Screenshots + investigation GIF captured
- [ ] Teardown + **verify-$0 checklist** screenshots

## Cost

_TBD — target ≤ $0.50 for the session._
