# src/annotate.py
import time
import json
import joblib
import os
from pathlib import Path
from datetime import datetime
import pandas as pd
from hashlib import md5

MODEL_PATH = Path("models/current_model.joblib")  # keep this updated by train step
RAW_DIR = Path("data/raw")
ANNOT_DIR = Path("data/annotations")
HUMAN_DIR = Path("data/human_review")
ANNOT_DIR.mkdir(parents=True, exist_ok=True)
HUMAN_DIR.mkdir(parents=True, exist_ok=True)

# small helper: load model (auto-reload if file changed)
class ModelReloader:
    def __init__(self, path: Path):
        self.path = path
        self._mtime = None
        self._model = None
    def get(self):
        if not self.path.exists():
            raise FileNotFoundError(self.path)
        mtime = self.path.stat().st_mtime
        if self._model is None or mtime != self._mtime:
            self._model = joblib.load(self.path)
            self._mtime = mtime
            print(f"Loaded model from {self.path} (mtime={self._mtime})")
        return self._model

def features_from_event(evt: dict):
    """
    Convert raw event to model features (match your pipeline).
    Return a 1-row pandas.DataFrame or feature vector as model expects.
    """
    # TODO: adapt to your feature logic. Example:
    return pd.DataFrame([{
        "price": evt.get("price"),
        "size": evt.get("size"),
        "spread": evt.get("ask") - evt.get("bid") if evt.get("ask") and evt.get("bid") else 0,
    }])

def annotate_file(path: Path, reloader: ModelReloader):
    out_rows = []
    with open(path, "r", encoding="utf8") as f:
        for line in f:
            evt = json.loads(line)
            model = reloader.get()
            X = features_from_event(evt)
            # adapt to your model API: predict or predict_proba
            if hasattr(model, "predict_proba"):
                proba = model.predict_proba(X)[0].tolist()
                pred = int(proba.index(max(proba)))
                conf = max(proba)
            else:
                pred = int(model.predict(X)[0])
                conf = None

            evt_out = {
                "id": md5(json.dumps(evt, sort_keys=True).encode()).hexdigest(),
                "timestamp": evt.get("timestamp", datetime.utcnow().isoformat()),
                "symbol": evt.get("symbol"),
                "prediction": pred,
                "confidence": conf,
                "raw": evt
            }
            # write to annotations file (by day)
            out_rows.append(evt_out)

            # low confidence -> human review
            if conf is not None and conf < 0.6:
                hr_path = HUMAN_DIR / f"human_{evt_out['id']}.json"
                with open(hr_path, "w", encoding="utf8") as hf:
                    json.dump(evt_out, hf)

    if out_rows:
        day = path.name.split("_")[1].split(".")[0]
        out_path = ANNOT_DIR / f"annotations_{day}.ndjson"
        with open(out_path, "a", encoding="utf8") as of:
            for r in out_rows:
                of.write(json.dumps(r, default=str) + "\n")
        print(f"Wrote {len(out_rows)} annotations to {out_path}")

def run_once():
    reloader = ModelReloader(MODEL_PATH)
    # process any raw files that do not have corresponding annotation file
    for raw in sorted(RAW_DIR.glob("raw_*.ndjson")):
        day = raw.name.split("_")[1].split(".")[0]
        out_path = ANNOT_DIR / f"annotations_{day}.ndjson"
        if out_path.exists():
            print(f"Skipping {raw} (already annotated)")
            continue
        print("Annotating", raw)
        annotate_file(raw, reloader)

if __name__ == "__main__":
    # simple runner: iterate repeatedly
    while True:
        try:
            run_once()
        except Exception as e:
            print("Annotator error:", e)
        time.sleep(30)  # poll every 30s