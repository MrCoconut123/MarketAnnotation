# src/collector.py
import json
from pathlib import Path
from datetime import datetime

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

def append_raw_event(event: dict):
    """
    Append a normalized event to file by day.
    event: dict containing timestamp and fields from stream.
    """
    ts = event.get("timestamp") or datetime.utcnow().isoformat()
    day = ts.split("T")[0]
    path = RAW_DIR / f"raw_{day}.ndjson"
    with open(path, "a", encoding="utf8") as f:
        f.write(json.dumps(event, default=str) + "\n")

# Example usage (call this from your stream handler)
# append_raw_event(dict(symbol="AAPL", price=150.3, size=200, timestamp="2026-02-20T15:32:00Z"))