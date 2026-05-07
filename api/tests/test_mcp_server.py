# api/tests/test_agent_e2e.py
"""
End-to-end smoke test for the Day 5 ReAct agent.

Runs the full pipeline and surfaces the agent's tool-calling behavior:
    1. fetch_context  — baseline (hardcoded query)
    2. investigate    — ReAct loop with MCP tools
    3. write_report   — Postgres insert

Usage:
    cd api && python tests/test_agent_e2e.py
    cd api && python tests/test_agent_e2e.py --ticker AAPL
    cd api && python tests/test_agent_e2e.py \\
        --ticker AAPL \\
        --lookback-minutes 1500 \\
        --window-start "2026-05-04T21:15:00"
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

print(f"[debug] running with: {sys.executable}", flush=True)

from agent.graph import agent
from agent.state import make_initial_state
from data.postgres import get_investigation, init_schema


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", default="AAPL")
    parser.add_argument("--anomaly-type", default="price_spike")
    parser.add_argument("--lookback-minutes", type=int, default=30)
    parser.add_argument("--window-start", default=None)
    args = parser.parse_args()

    if args.window_start:
        ws = datetime.fromisoformat(args.window_start)
        if ws.tzinfo is None:
            ws = ws.replace(tzinfo=timezone.utc)
    else:
        ws = datetime.now(timezone.utc)

    init_schema()

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

    # Surface what tools the agent actually called
    print("\n=== AGENT TOOL CALLS ===")
    tool_call_count = 0
    for m in final.get("messages", []):
        if type(m).__name__ == "AIMessage":
            for tc in (getattr(m, "tool_calls", None) or []):
                tool_call_count += 1
                print(f"  → {tc['name']}({tc['args']})")
    if tool_call_count == 0:
        print("  (none — agent answered from baseline alone)")
    print(f"\n  total tool calls: {tool_call_count}")

    rid = final.get("report_id")
    if not rid:
        print("\n❌ no report_id — investigation failed before write")
        sys.exit(1)

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

    # Validation
    failed = False
    if final.get("errors"):
        print(f"\n❌ agent recorded errors:")
        for e in final["errors"]:
            print(f"   - {e}")
        failed = True

    if len(final.get("reasoning", "")) < 100:
        print(f"\n❌ reasoning too short ({len(final.get('reasoning', ''))} chars)")
        failed = True

    if final.get("report_markdown", "").startswith("# Investigation failed"):
        print("\n❌ report_markdown is failure stub")
        failed = True

    if failed:
        print("\n❌ AGENT END-TO-END TEST FAILED")
        sys.exit(1)

    print("\n✅ AGENT END-TO-END TEST PASSED")


if __name__ == "__main__":
    main()