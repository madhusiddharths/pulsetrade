# api/tests/test_agent_e2e.py
"""
End-to-end smoke test for the Day 5 ReAct agent.

Runs the full pipeline against real data:
    1. fetch_context  — reads gold_5min_features (baseline)
    2. investigate    — ReAct loop with MCP tools (gold/news/web search)
    3. write_report   — inserts into Postgres
    4. (verify)       — re-reads the row to confirm

Usage:
    cd api && python tests/test_agent_e2e.py
    cd api && python tests/test_agent_e2e.py --ticker NVDA
    cd api && python tests/test_agent_e2e.py \
        --ticker AAPL \
        --lookback-minutes 360 \
        --window-start "2026-05-07T20:35:00"
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

# Make sure we can import from api/ regardless of cwd
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

print(f"[debug] running with: {sys.executable}", flush=True)

from agent.graph import agent
from agent.state import make_initial_state
from data.postgres import get_investigation, init_schema


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ticker",
        default="AAPL",
        help="ticker to investigate (default: AAPL)",
    )
    parser.add_argument(
        "--anomaly-type",
        default="price_spike",
        help="anomaly type label stored on the investigation",
    )
    parser.add_argument(
        "--lookback-minutes",
        type=int,
        default=30,
        help="how far back fetch_context looks in gold_5min_features "
             "(default 30; use 360 for ~6h, 10080 for a week)",
    )
    parser.add_argument(
        "--window-start",
        default=None,
        help="ISO timestamp for the anomaly window center (e.g. "
             "'2026-05-07T20:35:00'); defaults to now. The agent searches "
             "news in window_start +/- 30 min via tool calls.",
    )
    args = parser.parse_args()

    # Resolve window_start: explicit ISO arg, or now()
    if args.window_start:
        ws = datetime.fromisoformat(args.window_start)
        if ws.tzinfo is None:
            ws = ws.replace(tzinfo=timezone.utc)
    else:
        ws = datetime.now(timezone.utc)

    init_schema()  # idempotent — safe to call

    initial = make_initial_state(
        ticker=args.ticker,
        anomaly_type=args.anomaly_type,
        window_start=ws,
        lookback_minutes=args.lookback_minutes,
    )

    print(f"\n=== INVOKING AGENT ===")
    print(f"ticker={initial['ticker']} type={initial['anomaly_type']}")
    print(f"window_start={initial['window_start']}")
    print(f"lookback_minutes={initial['lookback_minutes']}\n")

    final = agent.invoke(initial)

    print("\n=== FINAL STATE ===")
    print(f"  gold rows:       {len(final.get('gold_context', []))}")
    print(f"  iterations:      {final.get('iterations', 0)}")
    print(f"  reasoning chars: {len(final.get('reasoning', ''))}")
    print(f"  report_id:       {final.get('report_id')}")
    print(f"  errors:          {final.get('errors', [])}")

    # Surface what tools the agent actually called.
    # If this is empty, the ReAct loop didn't run — Gemini answered from
    # baseline alone, OR (more likely if baseline was empty) MCP tools
    # failed to load and Gemini hallucinated a one-shot response.
    print("\n=== AGENT TOOL CALLS ===")
    tool_call_count = 0
    for m in final.get("messages", []):
        if type(m).__name__ == "AIMessage":
            for tc in (getattr(m, "tool_calls", None) or []):
                tool_call_count += 1
                # Show args compactly — first 100 chars of repr
                args_repr = repr(tc.get("args", {}))[:100]
                print(f"  → {tc['name']}({args_repr})")
    if tool_call_count == 0:
        print("  (none — agent answered from baseline alone)")
    print(f"\n  total tool calls: {tool_call_count}")

    rid = final.get("report_id")
    if not rid:
        print("\n❌ no report_id — investigation failed before reaching write_report")
        sys.exit(1)

    # Read it back from Postgres to confirm the write actually persisted
    row = get_investigation(rid)
    if not row:
        print(f"\n❌ report_id {rid} not found in Postgres")
        sys.exit(1)

    print(f"\n=== POSTGRES ROW #{rid} ===")
    print(f"  ticker:       {row['ticker']}")
    print(f"  anomaly_type: {row['anomaly_type']}")
    print(f"  created_at:   {row['created_at']}")
    print(f"\n--- report_markdown (first 800 chars) ---")
    print(row["report_markdown"][:800])

    # ── Validation ───────────────────────────────────────────────────────
    failed = False

    # Catches the "MCP tools didn't load" failure mode: when there's no
    # baseline data AND the agent made zero tool calls, the ReAct loop
    # didn't really happen — Gemini just hallucinated a response.
    has_no_baseline = len(final.get("gold_context", [])) == 0
    if has_no_baseline and tool_call_count == 0:
        print(
            "\n❌ no baseline gold AND zero tool calls — agent didn't "
            "actually investigate. Likely MCP tools failed to load."
        )
        failed = True

    if final.get("errors"):
        print(f"\n❌ agent recorded errors:")
        for e in final["errors"]:
            print(f"   - {e}")
        failed = True

    if len(final.get("reasoning", "")) < 100:
        print(
            f"\n❌ reasoning is empty or too short "
            f"({len(final.get('reasoning', ''))} chars) — Gemini likely failed"
        )
        failed = True

    if final.get("report_markdown", "").startswith("# Investigation failed"):
        print("\n❌ report_markdown is the failure stub, not real analysis")
        failed = True

    if failed:
        print("\n❌ AGENT END-TO-END TEST FAILED")
        sys.exit(1)

    print("\n✅ AGENT END-TO-END TEST PASSED")


if __name__ == "__main__":
    main()