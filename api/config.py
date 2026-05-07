# api/config.py
"""
Typed configuration loaded from project-root .env.

All env access in the api/ service goes through this module — no scattered
os.getenv calls. Missing or malformed values fail fast at startup with a
clear pydantic validation error instead of a None dereference at request time.
"""

from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Project root is two levels up from this file: api/config.py → pulsetrade/
PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",  # ignore unrelated keys (KAFKA_*, FINNHUB_*, etc.)
    )

    # ── LLM ──────────────────────────────────────────────────────────────────
    google_api_key: str = Field(..., alias="GOOGLE_API_KEY")
    groq_api_key: str | None = Field(default=None, alias="GROQ_API_KEY")

    # ── Databricks SQL ───────────────────────────────────────────────────────
    databricks_host: str = Field(..., alias="DATABRICKS_HOST")
    databricks_token: str = Field(..., alias="DATABRICKS_TOKEN")
    databricks_http_path: str = Field(..., alias="DATABRICKS_HTTP_PATH")

    databricks_catalog: str = "workspace"
    databricks_schema: str = "pulsetrade"

    # ── Postgres ─────────────────────────────────────────────────────────────
    postgres_host: str = "localhost"
    postgres_port: int = 5433
    postgres_user: str = "pulsetrade"
    postgres_password: str = "pulsetrade_dev"
    postgres_db: str = "pulsetrade"

    @property
    def postgres_url(self) -> str:
        """SQLAlchemy connection URL."""
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    # ── LangSmith (optional) ─────────────────────────────────────────────────
    langsmith_api_key: str | None = Field(default=None, alias="LANGSMITH_API_KEY")
    langsmith_project: str = Field(default="pulsetrade", alias="LANGSMITH_PROJECT")

    # ── Tavily (used in Day 5) ───────────────────────────────────────────────
    tavily_api_key: str | None = Field(default=None, alias="TAVILY_API_KEY")


# Singleton — import this everywhere
settings = Settings()