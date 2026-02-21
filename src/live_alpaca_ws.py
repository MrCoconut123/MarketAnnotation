# src/live_alpaca_ws.py
import os
import asyncio
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd

# Use the alpaca-py market data stream for live bars
from alpaca.data.live.stock import StockDataStream

# ---- Load environment variables ----
load_dotenv()

API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")

if not API_KEY or not API_SECRET:
    raise RuntimeError("Set APCA_API_KEY_ID and APCA_API_SECRET_KEY in your environment (e.g. in a .env)")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

TICKER = "AAPL"
OUT_FILE = DATA_DIR / f"alpaca_{TICKER}_1min.csv"

# ---- Create stream (StockDataStream for market data) ----
stream = StockDataStream(API_KEY, API_SECRET)

# ---- Safe timestamp conversion helper ----
def _to_utc_naive_timestamp(ts):
    """
    Convert various timestamp inputs to a timezone-naive UTC pandas.Timestamp.
    Accepts str, datetime, or pandas.Timestamp.
    """
    ts = pd.to_datetime(ts)
    if ts.tzinfo is None and ts.tz is None:
        # naive -> assume UTC
        ts = ts.tz_localize("UTC")
    else:
        # already tz-aware -> convert to UTC
        ts = ts.tz_convert("UTC")
    # make naive
    return ts.tz_localize(None)

# ---- Async handler ----
async def handle_bar(bar):
    """
    Handler for incoming bar messages.
    The bar object fields vary by version; common attributes: timestamp, open, high, low, close, volume
    """
    try:
        # bar.timestamp might be str or datetime or pandas.Timestamp
        ts = _to_utc_naive_timestamp(getattr(bar, "timestamp", None) or getattr(bar, "t", None) or bar)

        row = {
            "timestamp": ts,
            "open": getattr(bar, "open", getattr(bar, "o", None)),
            "high": getattr(bar, "high", getattr(bar, "h", None)),
            "low": getattr(bar, "low", getattr(bar, "l", None)),
            "close": getattr(bar, "close", getattr(bar, "c", None)),
            "volume": getattr(bar, "volume", getattr(bar, "v", None)),
        }

        df = pd.DataFrame([row])

        # write header only if file doesn't exist
        if OUT_FILE.exists():
            df.to_csv(OUT_FILE, mode="a", header=False, index=False)
        else:
            df.to_csv(OUT_FILE, index=False)

        print("Wrote bar:", ts)

    except Exception as e:
        print("Error handling bar:", repr(e))

# src/live_alpaca_ws.py  (only the bottom part changed)
# ... (keep your imports, env loading, StockDataStream setup, handler, etc.)

# ---- Create stream (StockDataStream for market data) ----
stream = StockDataStream(API_KEY, API_SECRET)

def start_stream():
    try:
        # subscribe_bars accepts callback and symbols
        stream.subscribe_bars(handle_bar, TICKER)

        # NOTE: do NOT await stream.run() — call it directly (it manages its own loop).
        # This matches the examples in the alpaca-py docs.
        print("Starting stream (press Ctrl+C to stop)...")
        stream.run()

    except KeyboardInterrupt:
        print("Interrupted by user — closing stream...")
        # close() is async, so run it with asyncio.run when exiting
        try:
            asyncio.run(stream.close())
        except Exception as e:
            print("Error closing stream:", repr(e))

    except Exception as e:
        print("Stream error:", repr(e))
        # If the stream state is bad, attempt a clean close
        try:
            asyncio.run(stream.close())
        except Exception:
            pass

if __name__ == "__main__":
    start_stream()