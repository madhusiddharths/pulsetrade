# api/tests/test_gemini.py
"""
One-shot smoke test: confirm GEMINI_API_KEY is valid and the model responds.
Run as a script (not pytest) — fast manual verification.

Usage:
    cd api && python tests/test_gemini.py
"""

import os
import sys
from pathlib import Path

# Load .env from project root
from dotenv import load_dotenv
ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

from langchain_google_genai import ChatGoogleGenerativeAI


def main():
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("❌ GOOGLE_API_KEY not found in .env")
        sys.exit(1)

    print(f"✓ GOOGLE_API_KEY loaded (first 8 chars: {api_key[:8]}...)")

    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        google_api_key=api_key,
        temperature=0.2,
    )

    print("→ sending test prompt to Gemini 2.5 Flash...")
    resp = llm.invoke(
        "You are a financial analyst. In one sentence, what does it mean when a "
        "stock's intraday volatility (high-low range) exceeds 3% in a single day?"
    )
    print(f"✓ Gemini response:\n  {resp.content}\n")
    print(f"✓ token usage: {getattr(resp, 'usage_metadata', 'n/a')}")
    print("✅ Gemini smoke test PASSED")


if __name__ == "__main__":
    main()