"""
Custom LangChain callback that increments the gemini_tokens_used_total
counter every time a Gemini call completes.

Why a callback and not just inspecting the LangGraph state:
- The graph state may or may not surface token counts depending on
  how the underlying model client returns them.
- The on_llm_end callback fires for *every* LLM call regardless of
  which node it happens in — which is what we want for total cost
  attribution.
"""
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.outputs import LLMResult
from typing import Any
import logging

from metrics import gemini_tokens_used_total

logger = logging.getLogger(__name__)


class GeminiTokenCounter(BaseCallbackHandler):
    def __init__(self, node_name: str = "unknown"):
        # We pass the node_name in when constructing the callback
        # so each LangGraph node attributes its tokens correctly.
        self.node_name = node_name
    
    def on_llm_end(self, response: LLMResult, **kwargs: Any) -> None:
        """
        Called by LangChain after every LLM completion.
        
        WARNING: the shape of response.llm_output varies by provider
        and even by langchain-google-genai version. Logging here helps
        debug when token counts don't appear.
        """
        try:
            llm_output = response.llm_output or {}
            
            # langchain-google-genai puts usage under 'usage_metadata' in
            # newer versions and 'token_usage' in older ones. Check both.
            usage = llm_output.get("usage_metadata") or llm_output.get("token_usage") or {}
            
            input_tokens = usage.get("input_tokens") or usage.get("prompt_tokens") or 0
            output_tokens = usage.get("output_tokens") or usage.get("completion_tokens") or 0
            model = llm_output.get("model_name", "gemini-unknown")
            
            if input_tokens:
                gemini_tokens_used_total.labels(
                    model=model, node=self.node_name, token_type="input"
                ).inc(input_tokens)
            
            if output_tokens:
                gemini_tokens_used_total.labels(
                    model=model, node=self.node_name, token_type="output"
                ).inc(output_tokens)
            
            if not input_tokens and not output_tokens:
                logger.warning(
                    f"No tokens captured in on_llm_end. llm_output keys: {list(llm_output.keys())}"
                )
        
        except Exception as e:
            # Never let metric collection crash a real request
            logger.error(f"Token callback failed: {e}", exc_info=True)