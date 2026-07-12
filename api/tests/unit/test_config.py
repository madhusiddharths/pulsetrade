"""Unit tests for api/config.py — the env contract the README documents."""
import pytest
from pydantic import ValidationError

from config import Settings


def _kwargs(**overrides):
    base = dict(
        GOOGLE_API_KEY="k",
        DATABRICKS_HOST="h.cloud.databricks.com",
        DATABRICKS_TOKEN="t",
        DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/x",
    )
    base.update(overrides)
    return base


def test_postgres_url_built_from_parts():
    s = Settings(
        **_kwargs(),
        postgres_host="db",
        postgres_port=5555,
        postgres_user="u",
        postgres_password="pw",
        postgres_db="d",
    )
    assert s.postgres_url == "postgresql+psycopg2://u:pw@db:5555/d"


def test_postgres_defaults_target_local_dev():
    s = Settings(**_kwargs())
    assert s.postgres_host == "localhost"
    assert s.postgres_port == 5433


def test_missing_required_key_fails_fast(monkeypatch, tmp_path):
    """The documented behavior: no GOOGLE_API_KEY -> loud pydantic error, not a
    None dereference at request time."""
    for var in (
        "GOOGLE_API_KEY",
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_HTTP_PATH",
    ):
        monkeypatch.delenv(var, raising=False)
    empty_env = tmp_path / "empty.env"
    empty_env.write_text("")
    with pytest.raises(ValidationError) as exc:
        Settings(_env_file=empty_env)
    # The error must NAME the missing fields — that's the DX contract.
    assert "GOOGLE_API_KEY" in str(exc.value)
