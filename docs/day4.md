## Day 4 — agent core

Built FastAPI + LangGraph + Gemini + Postgres in one session.

Three debugging stories worth keeping:

1. **gcloud ADC hijacking Gemini auth.** `langchain-google-genai` silently
   prefers Application Default Credentials over GOOGLE_API_KEY when both
   exist. Cached gcloud ADC from a previous session caused 403 with
   "ACCESS_TOKEN_SCOPE_INSUFFICIENT". Fix: pass `google_api_key=` explicitly
   when constructing the LLM.

2. **Venv pip bootstrap on macOS + anaconda.** `python3 -m venv` created
   a venv without pip on disk because anaconda's python doesn't always
   ship pip into venvs cleanly. Fix: `python -m ensurepip --upgrade`
   after creating the venv. Also disabled conda auto-base.

3. **Test passing on a failure stub.** First version of test_agent_e2e
   only checked "did a row land in Postgres?" — even the failure stub
   counted. Fixed validation to check reasoning length, errors array,
   and not-the-stub-prefix.

Notable: agent correctly pushed back on a misclassified `price_spike`
label, citing zero-variance single-o