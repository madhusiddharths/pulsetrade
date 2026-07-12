"""
Offline unit-test bootstrap.

These tests run with NO .env, NO network, NO cloud credentials — they must be
green in CI on every push. Heavy/credentialed imports (mlflow, sklearn, the
Databricks connector) are stubbed at import time; the functions under test
never touch them. The script-style smoke tests one level up (api/tests/) are
the manual integration checks and stay out of CI.
"""
import os
import sys
import types
from pathlib import Path

API_DIR = Path(__file__).resolve().parents[2]  # .../api
sys.path.insert(0, str(API_DIR))

# Dummy env so api/config.py's module-level `settings = Settings()` never
# explodes at import in a credential-less environment.
os.environ.setdefault("GOOGLE_API_KEY", "test-key")
os.environ.setdefault("DATABRICKS_HOST", "test.cloud.databricks.com")
os.environ.setdefault("DATABRICKS_TOKEN", "test-token")
os.environ.setdefault("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/test")


def _stub(name: str) -> types.ModuleType:
    mod = sys.modules.get(name) or types.ModuleType(name)
    sys.modules.setdefault(name, mod)
    return mod


# model_training.py imports these at module top; the pure data-prep functions
# under test never call into them.
_mlflow = _stub("mlflow")
_mlflow_sk = _stub("mlflow.sklearn")
_mlflow.sklearn = _mlflow_sk

_sk = _stub("sklearn")
_sk_ens = _stub("sklearn.ensemble")
_sk_ens.IsolationForest = object
_sk.ensemble = _sk_ens
_sk_prep = _stub("sklearn.preprocessing")
_sk_prep.StandardScaler = object
_sk.preprocessing = _sk_prep

# data/databricks.py pulls in the Databricks SQL connector; stub the single
# symbol model_training imports from it.
_dbx = _stub("data.databricks")
_dbx.get_gold_window = lambda *a, **k: []
