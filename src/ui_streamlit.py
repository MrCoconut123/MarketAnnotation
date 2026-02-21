# src/ui_streamlit.py
import sys
from pathlib import Path
import streamlit as st
import pandas as pd
import datetime

# ensure src/ modules are importable when running "streamlit run src/ui_streamlit.py"
sys.path.append(str(Path(__file__).resolve().parent))

from fetch import fetch_ticker
from features import add_basic_features
from heuristics import auto_label

# Folders
ROOT = Path(__file__).resolve().parent.parent
ANNOT_DIR = ROOT / "annotations"
ANNOT_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR = ROOT / "data"

st.set_page_config(page_title="Market Annotation Tool", layout="wide")
st.title("Market Annotation Tool — Prototype")

# --- Controls ---
with st.sidebar:
    st.header("Load data")
    ticker = st.text_input("Ticker", value="AAPL")
    period = st.selectbox("Period", ["1mo", "3mo", "6mo", "1y"], index=2)
    interval = st.selectbox("Interval", ["1d"], index=0)
    annotator_id = st.text_input("Annotator ID", value="anon")
    use_live = st.checkbox("Use live feed (Alpaca CSV)", value=False)
    if st.button("Load data"):
        try:
            df = fetch_ticker(ticker, period=period, interval=interval)
            st.success(f"Downloaded {len(df)} rows for {ticker}")
        except Exception as e:
            st.error(f"Fetch failed: {e}")
            st.stop()
    else:
        # if user requested live feed, look for alpaca CSV first
        live_csv = DATA_DIR / f"alpaca_{ticker}_1min.csv"
        csv_path = DATA_DIR / f"{ticker}_{period}_{interval}.csv"

        if use_live and live_csv.exists():
            # load the live 1min-bars CSV (we'll resample to daily if you still want daily)
            df = pd.read_csv(live_csv, parse_dates=["timestamp"])
            # make the DataFrame look like yfinance format (index with datetime and OHLCV columns)
            df = df.rename(columns={"timestamp": "Date", "open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"})
            # set index and normalize to daily if your UI expects daily data.
            df = df.set_index("Date").sort_index()
        elif csv_path.exists():
            df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
        else:
            df = None

# require data
if df is None:
    st.info("No data loaded. Use the sidebar to enter a ticker and click 'Load data' (or pre-run src/fetch.py).")
    st.stop()

# compute features and suggested labels
df = add_basic_features(df)
suggested = auto_label(df)

# --- Candlestick chart (Plotly) ---
import plotly.graph_objects as go

# prepare OHLC data for the selected range (show last 90 rows by default)
plot_df = df.copy()
plot_df = plot_df.reset_index().rename(columns={"index": "Date"})
# Plotly expects datetimes without timezone
plot_df["Date"] = pd.to_datetime(plot_df["Date"]).dt.tz_localize(None)

fig = go.Figure(
    data=[
        go.Candlestick(
            x=plot_df["Date"],
            open=plot_df["Open"],
            high=plot_df["High"],
            low=plot_df["Low"],
            close=plot_df["Close"],
            name=ticker
        )
    ]
)
fig.update_layout(
    xaxis_rangeslider_visible=False,
    height=450,
    margin=dict(l=40, r=20, t=30, b=30),
    title=f"{ticker} - Candlestick"
)

st.plotly_chart(fig, use_container_width=True)

# --- Date selector & row lookup (single unified block) ---
st.subheader(f"{ticker} — Data & Annotation")
# date picked by the user (a date, no time)
date = st.date_input("Select date to annotate", value=df.index[-1].date())

# normalize to midnight (no timezone)
date = pd.to_datetime(date).normalize()

# Match by date only (ignore time-of-day and timezone)
df_dates = df.index.normalize()
mask = df_dates == date

if not mask.any():
    st.warning("Selected date not found in the dataset index. Pick another date.")
    st.stop()

# get the exact timestamp from the index that corresponds to the selected date
matched_idx = df.index[mask][0]
row = df.loc[matched_idx]

st.markdown("### Price / Volume")
st.write(row[["Open", "High", "Low", "Close", "Volume"]])

st.markdown("### Computed features")
st.write(df.loc[matched_idx, ["sma_10", "sma_50", "momentum_5", "volatility_10", "volume_spike", "atr_14"]])

st.markdown("### Suggested automatic labels")
st.write(suggested.loc[matched_idx])

# --- Annotation inputs ---
st.markdown("### Final labels (choose and save)")
col1, col2 = st.columns(2)
with col1:
    trend = st.selectbox("Trend label", ["uptrend", "downtrend", "neutral"], index=0)
with col2:
    volatility = st.selectbox("Volatility label", ["volatility_spike", "normal"], index=1)

note = st.text_area("Notes (optional)")

if st.button("Save annotation"):
    out = {
        "ticker": ticker,
        "date": date.strftime("%Y-%m-%d"),
        "trend": trend,
        "volatility": volatility,
        "annotator_id": annotator_id or "anon",
        "annotated_at": datetime.datetime.utcnow().isoformat(),
        "note": note,
        "source_file": str(DATA_DIR / f"{ticker}_{period}_{interval}.csv")
    }

    fname = ANNOT_DIR / f"annotations_{ticker}.csv"

    if fname.exists():
        existing = pd.read_csv(fname)

        # Remove existing row for same date
        existing = existing[existing["date"] != out["date"]]

        # Append new row
        updated = pd.concat([existing, pd.DataFrame([out])], ignore_index=True)
        updated.to_csv(fname, index=False)
    else:
        pd.DataFrame([out]).to_csv(fname, index=False)

    st.success(f"Saved annotation to {fname.name}")

# --- Show recent annotations for this ticker ---
st.markdown("### Recent annotations")
ann_file = ANNOT_DIR / f"annotations_{ticker}.csv"
if ann_file.exists():
    ann_df = pd.read_csv(ann_file, parse_dates=["annotated_at"])
    st.dataframe(ann_df.sort_values("annotated_at", ascending=False).head(20))
else:
    st.info("No annotations yet for this ticker.")