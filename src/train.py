# src/train.py
import joblib
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score
import datetime
import json

LABEL_DIR = Path("data/labelled")
MODEL_DIR = Path("models")
MODEL_DIR.mkdir(exist_ok=True)
CURRENT_META = MODEL_DIR / "current_model.json"

def load_labelled():
    # concat all labelled CSVs/ndjson into DataFrame
    dfs = []
    for p in LABEL_DIR.glob("*.csv"):
        dfs.append(pd.read_csv(p))
    for p in LABEL_DIR.glob("*.ndjson"):
        # quick ndjson load
        rows = [json.loads(l) for l in p.read_text(encoding="utf8").splitlines() if l.strip()]
        dfs.append(pd.DataFrame(rows))
    if not dfs:
        raise RuntimeError("No labelled data found")
    df = pd.concat(dfs, ignore_index=True)
    return df

def feature_target(df):
    # adapt to your feature names
    X = df[["price","size","spread"]].fillna(0)
    y = df["label"]
    return X, y

def train_and_evaluate():
    df = load_labelled()
    X, y = feature_target(df)
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    pred = model.predict(X_val)
    acc = accuracy_score(y_val, pred)
    auc = None
    if hasattr(model, "predict_proba"):
        try:
            auc = roc_auc_score(y_val, model.predict_proba(X_val)[:,1])
        except Exception:
            auc = None
    print("Validation acc:", acc, "auc:", auc)
    # version artifact if improved
    now = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    model_file = MODEL_DIR / f"model_{now}.joblib"
    joblib.dump(model, model_file)
    # read current meta
    if CURRENT_META.exists():
        meta = json.loads(CURRENT_META.read_text())
        prev_acc = meta.get("val_acc", 0)
    else:
        prev_acc = -1
    # promote if improved
    if acc >= prev_acc:
        # update current_model.joblib symlink-ish
        current_target = MODEL_DIR / "current_model.joblib"
        joblib.dump(model, current_target)
        meta = {"model_file": str(model_file.name), "val_acc": acc, "val_auc": auc}
        CURRENT_META.write_text(json.dumps(meta))
        print("Promoted new model:", model_file)
    else:
        print("New model did not improve over existing (prev_acc=%s). Artifact saved as %s" % (prev_acc, model_file))

if __name__ == "__main__":
    train_and_evaluate()