# src/heuristics.py

import pandas as pd
import numpy as np


def label_trend(df: pd.DataFrame) -> pd.Series:
    """
    Simple trend labels:
    - 'uptrend' when sma_10 > sma_50 and short momentum is positive
    - 'downtrend' when sma_10 < sma_50 and short momentum is negative
    - otherwise 'neutral'
    """
    cond_up = (df["sma_10"] > df["sma_50"]) & (df["momentum_5"] > 0)
    cond_down = (df["sma_10"] < df["sma_50"]) & (df["momentum_5"] < 0)

    labels = pd.Series("neutral", index=df.index)
    labels.loc[cond_up] = "uptrend"
    labels.loc[cond_down] = "downtrend"

    return labels


def label_volatility(df: pd.DataFrame) -> pd.Series:
    """
    Mark high-volatility days:
    - 'volatility_spike' when volatility_10 is above the 90th percentile
      of its 50-day rolling distribution OR when volume_spike is True.
    - otherwise 'normal'
    """
    thr = df["volatility_10"].rolling(50, min_periods=1).quantile(0.9)
    cond = (df["volatility_10"] > thr) | (df["volume_spike"])

    return pd.Series(
        np.where(cond, "volatility_spike", "normal"),
        index=df.index
    )


def auto_label(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a DataFrame with automatic labels for each index/date.
    Columns:
        - trend
        - volatility
        - overnight_gap
    """
    df = df.copy()
    out = pd.DataFrame(index=df.index)

    out["trend"] = label_trend(df)
    out["volatility"] = label_volatility(df)

    # Overnight gap detection (open vs previous open percent change)
    if "Open" in df.columns:
        overnight = df["Open"].pct_change()
        out["overnight_gap"] = np.where(
            overnight.abs() > 0.05,
            "large_gap",
            "none"
        )
    else:
        out["overnight_gap"] = "none"

    return out


# -------------------------------
# TEST BLOCK (runs only if file is executed directly)
# -------------------------------
if __name__ == "__main__":
    from pathlib import Path
    from features import add_basic_features

    csv = Path("data") / "AAPL_3mo_1d.csv"

    df = pd.read_csv(csv, index_col=0, parse_dates=True)
    df = add_basic_features(df)

    labels = auto_label(df)

    print(labels.tail(8))