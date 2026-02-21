# src/fetch.py
from pathlib import Path
import yfinance as yf
import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

def fetch_ticker(ticker: str, period: str = "6mo", interval: str = "1d") -> pd.DataFrame:
    """
    Fetch OHLCV for ticker using yfinance, save to CSV, and return a DataFrame.
    period examples: "1mo", "3mo", "6mo", "1y"
    interval examples: "1d", "1h", "5m"
    """
    t = yf.Ticker(ticker)
    df = t.history(period=period, interval=interval)
    if df.empty:
        raise RuntimeError(f"No data for {ticker} (period={period}, interval={interval})")
    df.index = pd.to_datetime(df.index)
    out_path = DATA_DIR / f"{ticker}_{period}_{interval}.csv"
    df.to_csv(out_path)
    print(f"Saved {out_path} ({len(df)} rows)")
    return df

if __name__ == "__main__":
    # quick smoke-test
    df = fetch_ticker("AAPL", period="3mo", interval="1d")
    print(df.tail(3))