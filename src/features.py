# src/features.py

import pandas as pd
import numpy as np


def add_basic_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add basic technical features to OHLCV dataframe.
    Safely handles timezone-aware datetime index.
    """

    df = df.copy()

    # --- Fix timezone issue ---
    # Convert any tz-aware index to UTC, then remove timezone info
    df.index = pd.to_datetime(df.index, utc=True)
    df.index = df.index.tz_convert("UTC").tz_localize(None)

    # --- Returns ---
    df["return"] = df["Close"].pct_change()
    df["log_return"] = np.log(df["Close"]).diff()

    # --- Moving averages ---
    df["sma_10"] = df["Close"].rolling(10, min_periods=1).mean()
    df["sma_50"] = df["Close"].rolling(50, min_periods=1).mean()

    # --- Volatility ---
    df["volatility_10"] = df["log_return"].rolling(10, min_periods=1).std()

    # --- Volume features ---
    df["volume_avg_20"] = df["Volume"].rolling(20, min_periods=1).mean()
    df["volume_spike"] = df["Volume"] > 2 * df["volume_avg_20"]

    # --- Momentum ---
    df["momentum_5"] = df["Close"].pct_change(5)

    # --- ATR approximation ---
    df["tr"] = (df["High"] - df["Low"]).abs()
    df["atr_14"] = df["tr"].rolling(14, min_periods=1).mean()

    return df.drop(columns=["tr"])