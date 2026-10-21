import json
import os
import shutil
import threading
from pathlib import Path

import pandas as pd
from catboost import CatBoostClassifier

from preprocessing import load_artifact


MODELS_DIR = Path(os.getenv("MODELS_DIR", "/models"))
BUNDLED_MODEL = Path(os.getenv("BUNDLED_MODEL_PATH", "/app/models/initial.cbm"))
BUNDLED_ARTIFACT = Path(os.getenv("BUNDLED_ARTIFACT_PATH", "/app/artifacts/initial_preprocessing.json"))
FRAUD_THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", "0.98"))
CPU_THREADS = int(os.getenv("CPU_THREADS", "1"))


class ModelRegistry:
    """Reloads a published model and preprocessing artifact when latest.json changes."""

    def __init__(self):
        self.lock = threading.Lock()
        self.version = None
        self.model = None
        self.artifact = None

    def bootstrap(self) -> None:
        MODELS_DIR.mkdir(parents=True, exist_ok=True)
        pointer = MODELS_DIR / "latest.json"
        if pointer.exists():
            return
        if not BUNDLED_MODEL.exists() or not BUNDLED_ARTIFACT.exists():
            raise FileNotFoundError("Initial model files are missing")
        shutil.copyfile(BUNDLED_MODEL, MODELS_DIR / "model_initial.cbm")
        shutil.copyfile(BUNDLED_ARTIFACT, MODELS_DIR / "preprocessing_initial.json")
        self._write_pointer({"version": "initial", "model_file": "model_initial.cbm", "artifact_file": "preprocessing_initial.json"})

    def _write_pointer(self, payload: dict) -> None:
        temporary = MODELS_DIR / ".latest.tmp"
        temporary.write_text(json.dumps(payload), encoding="utf-8")
        os.replace(temporary, MODELS_DIR / "latest.json")

    def _read_pointer(self) -> dict:
        self.bootstrap()
        payload = json.loads((MODELS_DIR / "latest.json").read_text(encoding="utf-8"))
        if not {"version", "model_file", "artifact_file"}.issubset(payload):
            raise ValueError("Invalid model registry pointer")
        return payload

    def current(self):
        with self.lock:
            pointer = self._read_pointer()
            if pointer["version"] != self.version:
                model = CatBoostClassifier()
                model.load_model(str(MODELS_DIR / pointer["model_file"]))
                self.model = model
                self.artifact = load_artifact(MODELS_DIR / pointer["artifact_file"])
                self.version = pointer["version"]
            return self.model, self.artifact, self.version


def predict(model: CatBoostClassifier, frame: pd.DataFrame) -> pd.DataFrame:
    features = model.feature_names_
    missing = [name for name in features if name not in frame.columns]
    if missing:
        raise ValueError("Model features are missing: " + ", ".join(missing))
    probabilities = model.predict_proba(frame.loc[:, features], thread_count=CPU_THREADS)[:, 1]
    return pd.DataFrame({"score": probabilities, "fraud_flag": (probabilities > FRAUD_THRESHOLD).astype(int)})
