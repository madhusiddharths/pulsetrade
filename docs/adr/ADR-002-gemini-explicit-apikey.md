# ADR-002: Gemini as the reasoning LLM, with an explicit API key (not ADC)

**Status:** Accepted
**Date:** ~Day 1 / Day 4 (reconstructed 2026-05-24 from project history)
**Context day:** Day 1 (provider choice), Day 4 (auth fix)

> Reconstructed from build notes; the status doc and Day 4 journal already
> referenced this as "ADR-002 — explicit Gemini API key." Two related
> decisions are recorded together because they concern the same component.

## Context

The agent's `reason` node needs an LLM. The original architecture assumed
Anthropic/Claude (a $5 credit was provisioned). Separately, once Gemini was
wired in via `langchain-google-genai`, the first calls failed with a 403.

## Decision

1. **Use Google Gemini 2.5 Flash as the primary reasoning LLM**, with Groq
   (Llama 3.3 70B) as a free fallback. Anthropic dropped.
2. **Authenticate with an explicit API key** passed as `google_api_key=...`,
   not Application Default Credentials (ADC).

## Rationale

**Provider choice (Gemini over Claude):**
- Gemini 2.5 Flash free tier is 1500 requests/day with **no card required**;
  more than enough for development and demo load.
- Keeps total project spend at $0 for the LLM layer — the right budget for a
  student project.
- LangGraph/LangChain is provider-agnostic, so the `reason` node is identical
  regardless of provider. The README frames this as an "LLM-agnostic reasoning
  layer with provider failover (Gemini primary, Groq fallback)" — a real
  production pattern, ~20 lines of code.

**Auth (explicit key over ADC):**
- The initial 403 cost ~45 minutes of debugging; it presented like an API-key
  problem but was actually an **auth-path** problem — the client was trying ADC
  rather than the key. Fix was a single parameter: `google_api_key=...`.
- An explicit key is the right model for a containerized service anyway: the key
  arrives via env/secret at runtime, with no dependency on a gcloud ADC file
  being present in the container.

## Consequences

- LLM cost stays $0 throughout development.
- The explicit-key model maps cleanly onto the later K8s Secret approach — no
  ADC mounting needed in the GKE deployment.
- Lesson captured: a 403 from a Google client library is an auth-*mechanism*
  question first, an API-key-validity question second.

## Related
- Day 4 journal (the ADC debugging story).