"""Offline bootstrap: stub evidently so extract_drift_results is testable
without installing the full Evidently/scipy stack in CI."""
import sys
import types
from pathlib import Path

DRIFT_DIR = Path(__file__).resolve().parents[1]  # .../drift
sys.path.insert(0, str(DRIFT_DIR))

for name in ("evidently", "evidently.presets"):
    sys.modules.setdefault(name, types.ModuleType(name))
sys.modules["evidently"].Report = object
sys.modules["evidently.presets"].DataDriftPreset = object
