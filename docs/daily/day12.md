# Day 12 — Deploy to GKE with Helm (ephemeral, cost-disciplined)

**Goal:** take the containers from "run together locally" (Day 11 compose) to
"run on a managed Kubernetes cluster in the cloud." One focused session:
provision → deploy → verify → tear down, to stay inside the ~$18 Developer
Program credit.

## Outcome
Full stack deployed and working on GKE: 3 pods (api, dashboard, postgres) all
`1/1 Running`, `0` restarts. Investigation triggered from the (port-forwarded)
dashboard completed end-to-end on the cluster — dashboard pod → api pod (by
Service name) → co-located MCP subprocess → postgres pod (by Service name) →
result rendered. Clean first deploy, no ImagePull/CrashLoop. Cluster torn down
after capturing evidence; total cost ~$0.15.

## Decisions
- **GKE Standard, single zonal cluster, 2× e2-small** (not Autopilot). Standard
  is cheaper for a tiny short-lived workload and teaches the node layer. Zonal so
  the $74.40/mo free-tier credit covers the control plane; nodes are the only
  real charge (~$0.034/hr total). Region us-central1 (Chicago-local).
- **Postgres as in-cluster pod, not Cloud SQL** — see ADR-009 (cost + demo scope;
  Cloud SQL documented as the production choice when data must persist).
- **Ephemeral cluster** — create/deploy/demo/destroy in one session. No idle
  infra. Teardown is non-negotiable (`gcloud container clusters delete`).

## Phase sequence (what was actually done)
1. **Prereqs** — gcloud 553, kubectl 1.34, authed as madhusiddharths1@gmail.com.
   Installed `gke-gcloud-auth-plugin` (needed PATH fix: SDK bin at
   `/opt/homebrew/share/google-cloud-sdk/bin` wasn't on PATH; added to .zshrc).
   Confirmed billing enabled on project gen-lang-client-0874026413
   (billingAccount 017DB2-5D4AEF-05A562).
2. **Artifact Registry** — enabled artifactregistry + container APIs; created
   `pulsetrade` docker repo in us-central1; `gcloud auth configure-docker`.
3. **amd64 image rebuild + push** — THE arm64 trap. Mac builds arm64; GKE nodes
   are amd64 → arm64 image would crash-loop with `exec format error`. Rebuilt
   both images with `docker buildx --platform linux/amd64 ... --push`. Verified
   in registry as linux/amd64.
4. **Cluster** — `gcloud container clusters create pulsetrade --zone
   us-central1-a --num-nodes 2 --machine-type e2-small --disk-size 32`. ~5 min.
   `kubectl get nodes` → 2 nodes Ready (auth plugin worked).
5. **Helm chart** — `infra/helm/pulsetrade/`: Chart.yaml, values.yaml, and
   templates for postgres (Deployment+Service+PVC), api (Deployment+Service),
   dashboard (Deployment+Service), configmap. Service-name networking from Day 11
   maps directly onto K8s Services (api → postgres:5432, dashboard → api:8000).
6. **Secret** — `kubectl create secret generic pulsetrade-secrets
   --from-env-file` of a FILTERED .env (stripped POSTGRES_*/PULSETRADE_API_URL so
   they don't override the ConfigMap — see bug below). API keys only.
7. **Deploy** — `helm install pulsetrade`. All 3 pods Ready in ~50s.
8. **Verify** — `kubectl port-forward svc/dashboard 8501:8501`, triggered an
   investigation, it completed on the cluster.
9. **Teardown** — `helm uninstall` + `gcloud container clusters delete`. Images
   left in GAR (pennies, reused Day 13).

## Bugs / lessons

### The Secret vs ConfigMap override (the time-sink today)
`envFrom` applies configMapRef THEN secretRef, so secret keys override configMap
keys of the same name. The raw `.env` contained `POSTGRES_*` and a stray
`POSTGRES_URL` (unused by the code, leftover). If loaded into the Secret, those
would clobber the ConfigMap's correct `POSTGRES_HOST=postgres` with a
localhost-y value → the Day-11 "can't reach postgres" failure, but hidden.
Fix: filter them out of the secret (`grep -viE '^POSTGRES_|^PULSETRADE_API_URL'`)
so DB wiring lives ONLY in the ConfigMap. Confirmed the app builds its DSN from
individual `postgres_host/port/db` via a `postgres_url` @property (config.py:46),
so removing the env var `POSTGRES_URL` was safe — it was never read.

Sub-lesson: `kubectl apply` reported "unchanged" and refused to update the stale
secret; had to `kubectl delete secret` then recreate. When a secret won't update,
delete-and-recreate beats fighting apply.

### Key K8s concepts locked in
- **pod** = one running instance of a container (1 container/pod here). Same
  three containers as compose, now wrapped in pods.
- **Deployment** manages pods (recreates on death); **Service** gives pods a
  stable DNS name so they find each other despite pods being ephemeral.
- cluster → nodes (VMs) → pods → containers; Services front the pods.
- Free tier covers the CONTROL PLANE, not nodes. Nodes bill the whole time they
  exist → teardown is what stops the cost.

## Cost
~$0.15 for the session (2 e2-small × ~30 min). Control plane free (zonal, free
tier). GAR storage for 2 images ≈ pennies/month (left in place). Against ~$18
Developer Program credit: negligible.

## Deferred to Day 13
- Ingress-nginx: public routing `/api/*` → api, `/dashboard/*` → dashboard behind
  one load balancer (replaces today's port-forward).
- GitHub Actions CI/CD: push to main → lint → test → build → push GAR →
  helm upgrade. (Honest caveat: auto-deploy targets an ephemeral cluster, so the
  deploy stage is demonstrated, not continuously live.)
- Re-provision the cluster for Day 13 (same create command) — another ephemeral
  session.