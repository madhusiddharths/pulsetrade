# GKE Runbook — the ephemeral session

One session = **create → deploy → verify → capture evidence → destroy → prove $0**.
The cluster never idles (see [ADR-009](adr/ADR-009-pod-vs-cluster.md) and the
[day 12](daily/day12.md) / [day 13](daily/day13.md) journals for the reasoning
and war stories). Budget: 1-2 h, ~$0.10-0.50 per session.

Everything below is copy-paste runnable, in order.

## 0. Preflight ($0 — do all of this BEFORE creating the cluster)

```bash
# Authed as the right account, right project?
gcloud auth list
gcloud config set project gen-lang-client-0874026413

# Images present in Artifact Registry? (pushed by CI on main, or manually)
gcloud artifacts docker images list \
  us-central1-docker.pkg.dev/gen-lang-client-0874026413/pulsetrade

# Databricks awake + gold data present? Fail fast HERE, not on the cluster.
# (Free Edition hibernates — this also warms it.)
cd api && .venv/bin/python tests/test_agent_e2e.py && cd ..

# CI deploy needs GitHub → GCP auth: scoped service account + key as a secret.
gcloud iam service-accounts create pulsetrade-ci --display-name "pulsetrade CI (ephemeral)"
gcloud projects add-iam-policy-binding gen-lang-client-0874026413 \
  --member serviceAccount:pulsetrade-ci@gen-lang-client-0874026413.iam.gserviceaccount.com \
  --role roles/artifactregistry.writer
gcloud projects add-iam-policy-binding gen-lang-client-0874026413 \
  --member serviceAccount:pulsetrade-ci@gen-lang-client-0874026413.iam.gserviceaccount.com \
  --role roles/container.developer
gcloud iam service-accounts keys create /tmp/pulsetrade-ci-key.json \
  --iam-account pulsetrade-ci@gen-lang-client-0874026413.iam.gserviceaccount.com
gh secret set GCP_SA_KEY < /tmp/pulsetrade-ci-key.json && rm /tmp/pulsetrade-ci-key.json
# Production path: Workload Identity Federation instead of a key — a key is
# used here only because the whole setup lives for one session (see teardown).
```

## 1. Cluster (~5 min)

```bash
gcloud container clusters create pulsetrade \
  --zone us-central1-a --num-nodes 2 --machine-type e2-small --disk-size 32
kubectl get nodes   # 2 × Ready
```

Zonal = control plane covered by the GKE free tier; the 2 e2-small nodes are
the only real charge (~$0.034/hr total).

## 2. Ingress controller (~3-5 min for the LB IP)

```bash
helm upgrade --install ingress-nginx ingress-nginx \
  --repo https://kubernetes.github.io/ingress-nginx \
  --namespace ingress-nginx --create-namespace
# Wait for the external IP (this is the one load balancer both Ingresses share):
kubectl get svc -n ingress-nginx ingress-nginx-controller -w
```

## 3. Secrets (FILTERED — day-12 lesson)

`envFrom` applies the Secret AFTER the ConfigMap, so any `POSTGRES_*` /
`PULSETRADE_*` keys in the Secret would silently clobber the ConfigMap's
in-cluster DB wiring. Filter them out — API keys only:

```bash
kubectl create secret generic pulsetrade-secrets \
  --from-env-file=<(grep -viE '^(POSTGRES_|PULSETRADE_)' .env)
# If the secret already exists and won't update: delete + recreate beats apply.
```

## 4. Deploy

```bash
helm upgrade --install pulsetrade infra/helm/pulsetrade
kubectl get pods -w   # api, dashboard, postgres → 1/1 Running, 0 restarts
```

Or demonstrate the pipeline instead: GitHub → Actions → "Deploy (manual,
ephemeral cluster)" → Run workflow (tags: `latest` = what CI just built).

## 5. Verify the split ingress (the day-13 fix)

```bash
IP=$(kubectl get svc -n ingress-nginx ingress-nginx-controller \
     -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
curl -s  http://$IP/api/health   # rewrite strips /api → FastAPI answers
curl -s  http://$IP/api/ready
open     http://$IP/             # dashboard at root — websocket must be LIVE
```

The dashboard page must render fully (no blank Streamlit = the websocket
`/_stcore/stream` is flowing — the exact bug the two-Ingress split fixes).
Then trigger an investigation from the dashboard via the public URL.

**Timebox:** if the split can't be verified in ~30 min of debugging, fall back
to `kubectl port-forward svc/dashboard 8501:8501` (proven day 12), journal the
ingress findings, and STILL tear down on schedule.

## 6. Capture evidence

Immediately before recording, warm Databricks again (hibernation between
preflight and now is the classic demo-killer):

```bash
cd api && .venv/bin/python -c "from data.databricks import healthcheck; print(healthcheck())" && cd ..
```

Then capture: pods `1/1`, `kubectl get ingress`, dashboard on the public IP,
a completed investigation, the green Actions run — and screen-record the
investigation for the README GIF.

## 7. Teardown (non-negotiable) → verified $0

```bash
helm uninstall pulsetrade
kubectl delete pvc --all                       # PVC first — see disk note below
gcloud container clusters delete pulsetrade --zone us-central1-a --quiet
```

Then kill the CI credential (the SA key must not outlive the session):

```bash
gh secret delete GCP_SA_KEY
for K in $(gcloud iam service-accounts keys list \
    --iam-account pulsetrade-ci@gen-lang-client-0874026413.iam.gserviceaccount.com \
    --managed-by user --format 'value(name)'); do
  gcloud iam service-accounts keys delete "$K" --quiet \
    --iam-account pulsetrade-ci@gen-lang-client-0874026413.iam.gserviceaccount.com
done
```

### The verify-$0 checklist (screenshot this)

Deleting a cluster does **not** delete PVC-provisioned persistent disks, and
load-balancer artifacts can orphan. Every listing below must come back empty:

```bash
gcloud container clusters list
gcloud compute disks list
gcloud compute forwarding-rules list
gcloud compute addresses list
gcloud compute target-pools list
```

Still billing (deliberately, ≈ pennies/month): the two images in Artifact
Registry — kept so the next session skips the build+push.
