# api/metrics.py
"""
Prometheus metric definitions for the PulseTrade agent service.

Lives in its own module to avoid circular imports — both main.py and the
agent nodes (which need to increment metrics inside the LangChain callback)
import from here.

Module-level definitions are created exactly once at first import.
The Prometheus client uses a global registry; defining a metric twice raises
"Duplicated timeseries". Importing this module multiple times is safe
because Python caches module objects in sys.modules.
"""
from prometheus_client import Counter, Histogram


investigations_total = Counter(
    "investigations_total",
    "Total number of agent investigations completed",
    labelnames=["ticker", "anomaly_type", "status"],  # status: success | partial | error
)


investigation_duration_seconds = Histogram(
    "investigation_duration_seconds",
    "End-to-end latency of an agent investigation",
    labelnames=["ticker"],
    # Buckets sized for the 3-15s typical range, plus headroom for slow
    # Gemini days or when one of the upstream data sources is degraded.
    buckets=(1, 2, 5, 10, 15, 20, 30, 45, 60, 90, 120),
)


gemini_tokens_used_total = Counter(
    "gemini_tokens_used_total",
    "Total tokens consumed by Gemini calls",
    labelnames=["model", "node", "token_type"],  # token_type: input | output
)