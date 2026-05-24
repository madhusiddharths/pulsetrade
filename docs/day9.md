## Day 9 — Data Drift Detection (Evidently + Airflow DockerOperator)

The third observability layer. LangSmith traces the agent (Day 5), Prometheus
tracks the system (Day 8), and now Evidently watches the *data* — catching the
failure mode where nothing crashes but the input distribution silently shifts.

**Built:**
- Standalone `drift/` package, self-contained (own venv, own deps), so it can
  run as an isolated container:
  - `gold_data.py` — pulls real gold via the Databricks SQL connector; swappable
    reference/current window functions; `inject_drift` flag for demo mode.
  - `drift_detector.py` — Evidently 0.7.x DataDriftPreset; result extraction
    written against the exact 0.7.21 result-dict structure (no boolean in the
    output — compute `drifted = p_value < threshold` ourselves).
  - `drift_alert.py` — `drift_alerts` table + SQLAlchemy writer (matches the
    pulsetrade.py DAG helper pattern); both share-based and key-feature alert
    conditions.
  - `run_drift.py` — entrypoint: pull → detect → save timestamped HTML →
    persist alert row.
- `pulsetrade-drift` Docker image (the same unit that becomes a K8s pod later).
- `pulsetrade_data_quality` Airflow DAG — daily, DockerOperator spawns the
  drift container (Docker-out-of-Docker via mounted socket).
- Proven end-to-end: Airflow-triggered run wrote drift_alerts row #2 and a
  fresh HTML report independently of the standalone test (row #1).

**Decisions (see ADR-007):**
- DockerOperator (separate container) over PythonOperator — dependency
  isolation + dev/prod parity with the eventual K8s pod. Chose the harder
  local path (DoD socket) deliberately to test the deploy model.
- Demo mode with injected sentiment drift, because ~440 rows over 10
  intermittent days isn't enough for clean week-over-week. Swappable seam built
  in (`inject_drift=False`) for when real history accumulates.
- Daily schedule to conserve Databricks Free Edition compute.

**Gotchas hit:**
- Evidently API rewrite at 0.7 — old tutorials use `evidently.report.Report` /
  `evidently.metric_preset`; new API is top-level `Report` + `evidently.presets`,
  and `.run(current, reference)` is current-FIRST. Pinned 0.7.x and verified the
  real `.dict()` structure before writing extraction code.
- Class-sorted iris split made ALL columns "drift" — the windowing-comparability
  lesson. Fixed by shuffling before split. Directly informs how real gold
  windows must be chosen.
- Databricks catalog/schema aren't in .env — they're defaulted in api/config.py.
  Hardcoded in the drift package (self-contained) rather than requiring them in
  env.
- `DATABRICKS_*` creds were BLANK in the Airflow worker — docker compose reads
  `airflow/.env` (which only had AIRFLOW_UID), not the project-root `.env` that
  has the creds. This silently gave the spawned drift container empty Databricks
  credentials → task failure. Fixed by appending the creds to `airflow/.env`.
  Likely also affected earlier Databricks-using DAGs without us noticing.
- DockerOperator on macOS DoD: `mount_tmp_dir=False` needed (the default tmp
  mount fails), and volume `source` is a HOST path (sibling container, resolved
  by host daemon), not a worker-relative path.

**Verified working:**
- Evidently catches injected sentiment drift on the real gold schema
  (mean_news_sentiment p≈3.6e-10), leaves the 9 untouched features clean.
- drift_alerts rows persist; HTML reports land on the host volume.
- Full pipeline runs through Airflow, not just standalone.

**Deferred:**
- Real week-over-week drift — needs more cleanly-windowed production history.
  Keep running producers daily; flip `inject_drift=False` and swap window
  queries when there's enough.
- In K8s (later days): DockerOperator → KubernetesPodOperator; drop the socket
  mount in favor of native pod scheduling.

**Recurring constraint:** Databricks Free Edition daily compute cap again shaped
the day (had to wait for resets). Worth a README note as a known limitation of
the Free Edition tier.