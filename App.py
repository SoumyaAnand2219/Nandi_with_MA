import io
import time
import datetime as dt
import pandas as pd
import numpy as np
import streamlit as st
import pyotp
from SmartApi.smartConnect import SmartConnect

# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(page_title="Nandi Fibonacci Buy Scanner", layout="wide")
st.title("📈 Nandi + Fibonacci Buy Scanner")

# =========================================================
# LOGIN
# =========================================================
api_key = "g5o6vfTl"
client_id = "R59803990"
password = "1234"
totp_secret = "5W4MC6MMLANC3UYOAW2QDUIFEU"


@st.cache_resource
def angel_login():
    if not all([api_key, client_id, password, totp_secret]):
        raise ValueError("Missing Angel credentials in Streamlit secrets.")

    obj = SmartConnect(api_key=api_key)
    totp = pyotp.TOTP(totp_secret).now()
    session = obj.generateSession(client_id, password, totp)

    ok = False
    if isinstance(session, dict):
        ok = session.get("status", session.get("success", False))

    if not ok:
        msg = session.get("message", "Login failed") if isinstance(session, dict) else "Login failed"
        raise ValueError(msg)

    return obj

try:
    obj = angel_login()
    st.success("Angel One login successful")
except Exception as e:
    st.error(f"Login failed: {e}")
    st.stop()

# =========================================================
# STOCK MASTER
# =========================================================
from Stock_tokens import stock_list

# =========================================================
# HARD CODED SETTINGS
# =========================================================
INTERVAL_MAP = {
    "1 Day": "ONE_DAY",
    "1 Hour": "ONE_HOUR",
}

FIB_LENGTHS = [5, 8, 13, 21, 34, 55, 89, 144, 233]

# hardcoded logic values
MA1_LENGTH = 50
MA2_LENGTH = 200
LEN_SMA = 20
MULT = 2.0
CMO_LEN = 9
VOL_MULTIPLIER = 1.5
BUY_WINDOW_BARS = 14
STRETCH_LIMIT = 4.0
USE_VOL_SPIKE = True
USE_PERFECT_BUY = False

# fetch delay to reduce access denied / rate limit issues
FETCH_DELAY_SECONDS = 0.70

# fixed batch size
BATCH_SIZE = 100

# =========================================================
# HELPERS
# =========================================================
def fetch_data(token, interval, from_date, to_date):
    params = {
        "exchange": "NSE",
        "symboltoken": str(token),
        "interval": interval,
        "fromdate": from_date.strftime("%Y-%m-%d 09:15"),
        "todate": to_date.strftime("%Y-%m-%d 15:30"),
    }

    try:
        response = obj.getCandleData(params)
    except Exception:
        return None

    if not response:
        return None

    ok = response.get("status", response.get("success", False))
    data = response.get("data")

    if not ok or not data:
        return None

    df = pd.DataFrame(
        data,
        columns=["timestamp", "open", "high", "low", "close", "volume"]
    )

    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(None)
    num_cols = ["open", "high", "low", "close", "volume"]
    df[num_cols] = df[num_cols].astype(float)
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def ema(series, length):
    return series.ewm(span=length, adjust=False).mean()


def sma(series, length):
    return series.rolling(length).mean()


def stdev(series, length):
    return series.rolling(length).std()


def cmo(series, length):
    diff = series.diff()
    up = diff.clip(lower=0).rolling(length).sum()
    down = (-diff.clip(upper=0)).rolling(length).sum()
    denom = up + down
    out = np.where(denom == 0, 0, 100 * (up - down) / denom)
    return pd.Series(out, index=series.index)

# =========================================================
# PINE LOGIC PORT
# =========================================================
def apply_pine_buy_logic(df):
    df = df.copy()

    high_emas = [ema(df["high"], x) for x in FIB_LENGTHS]
    low_emas = [ema(df["low"], x) for x in FIB_LENGTHS]

    df["fib_high"] = sum(high_emas) / len(high_emas)
    df["fib_low"] = sum(low_emas) / len(low_emas)
    df["ma1"] = sma(df["close"], MA1_LENGTH)
    df["ma2"] = sma(df["close"], MA2_LENGTH)

    df["smaVal"] = sma(df["close"], LEN_SMA)
    df["smaValPrev"] = df["smaVal"].shift(1)

    df["kri"] = df["close"] - df["smaVal"]
    df["kriPrev"] = df["close"].shift(1) - df["smaValPrev"]

    df["absKRI"] = df["kri"].abs()
    df["absKRIprev"] = df["kriPrev"].abs()

    df["dev"] = MULT * stdev((df["close"] - df["smaVal"]).abs(), LEN_SMA)
    df["devPrev"] = df["dev"].shift(1)

    df["changePerc"] = ((df["close"] - df["close"].shift(1)) / df["close"].shift(1)) * 100.0

    df["condition1"] = (
        (df["absKRI"] > df["dev"]) &
        (df["absKRIprev"] <= df["devPrev"]) &
        (df["changePerc"] >= 0)
    )

    df["cmo"] = cmo(df["close"], CMO_LEN)
    df["condition2"] = (df["cmo"] > 0) & (df["cmo"].shift(1) < 0)

    df["white_candle"] = df["condition1"] & df["condition2"]

    df["vol_sma"] = sma(df["volume"], 20)
    df["vol_spike"] = df["volume"] > (df["vol_sma"] * VOL_MULTIPLIER)

    breakout_active = False
    has_triggered_buy = False
    white_candle_high = np.nan
    white_candle_low = np.nan
    white_candle_bar = None

    final_buy_list = []
    raw_breakout_list = []
    normal_buy_list = []
    perfect_buy_list = []
    signal_status_list = []
    white_high_track = []
    white_low_track = []
    bars_since_white_list = []

    for i in range(len(df)):
        row = df.iloc[i]

        if bool(row["white_candle"]):
            white_candle_high = row["high"]
            white_candle_low = row["low"]
            white_candle_bar = i
            breakout_active = True
            has_triggered_buy = False

        bars_since_white = (i - white_candle_bar) if (breakout_active and white_candle_bar is not None) else np.nan

        if breakout_active and not has_triggered_buy and pd.notna(bars_since_white) and bars_since_white > BUY_WINDOW_BARS:
            breakout_active = False
            white_candle_high = np.nan
            white_candle_low = np.nan
            white_candle_bar = None

        raw_breakout = (
            breakout_active and
            (not has_triggered_buy) and
            pd.notna(white_candle_high) and
            (row["close"] > white_candle_high) and
            ((not USE_VOL_SPIKE) or bool(row["vol_spike"]))
        )

        trend_bullish = (
            (row["close"] > row["fib_high"]) and
            (row["fib_high"] > row["fib_low"]) and
            (row["ma1"] > row["ma2"])
        )

        ma2_prev_5 = df["ma2"].iloc[i - 5] if i >= 5 and pd.notna(df["ma2"].iloc[i - 5]) else np.nan
        if pd.notna(ma2_prev_5):
            trend_bullish = trend_bullish and (row["ma2"] > ma2_prev_5)
        else:
            trend_bullish = False

        prev_fib_high = df["fib_high"].iloc[i - 1] if i >= 1 else np.nan
        prev_fib_low = df["fib_low"].iloc[i - 1] if i >= 1 else np.nan
        fib_rising = pd.notna(prev_fib_high) and pd.notna(prev_fib_low) and (row["fib_high"] > prev_fib_high) and (row["fib_low"] > prev_fib_low)

        range_val = row["high"] - row["low"]
        close_near_high = (((row["close"] - row["low"]) / range_val) > 0.60) if range_val > 0 else False
        bullish_body = row["close"] > row["open"]

        stretch_pct = ((row["close"] - row["fib_high"]) / row["fib_high"]) * 100.0 if row["fib_high"] != 0 else 0.0
        not_overstretched = stretch_pct <= STRETCH_LIMIT

        normal_nandi_buy = raw_breakout and (row["close"] > row["fib_high"]) and (row["ma1"] > row["ma2"])
        perfect_nandi_buy = raw_breakout and trend_bullish and fib_rising and bullish_body and close_near_high and not_overstretched

        final_buy = perfect_nandi_buy if USE_PERFECT_BUY else normal_nandi_buy

        if final_buy:
            has_triggered_buy = True
            breakout_active = False

        if final_buy:
            status = "BUY SIGNAL"
        elif breakout_active:
            status = "WAITING BREAKOUT"
        elif bool(row["white_candle"]):
            status = "WHITE CANDLE"
        else:
            status = "NO SIGNAL"

        final_buy_list.append(bool(final_buy))
        raw_breakout_list.append(bool(raw_breakout))
        normal_buy_list.append(bool(normal_nandi_buy))
        perfect_buy_list.append(bool(perfect_nandi_buy))
        signal_status_list.append(status)
        white_high_track.append(white_candle_high if pd.notna(white_candle_high) else np.nan)
        white_low_track.append(white_candle_low if pd.notna(white_candle_low) else np.nan)
        bars_since_white_list.append(bars_since_white)

    df["white_candle_high_track"] = white_high_track
    df["white_candle_low_track"] = white_low_track
    df["bars_since_white"] = bars_since_white_list
    df["raw_breakout"] = raw_breakout_list
    df["normal_nandi_buy"] = normal_buy_list
    df["perfect_nandi_buy"] = perfect_buy_list
    df["buy_signal"] = final_buy_list
    df["signal_status"] = signal_status_list

    return df

# =========================================================
# ANALYZE STOCK
# latest buy signal in selected date range
# =========================================================
def analyze_stock(symbol, token, interval, from_date, to_date):
    df = fetch_data(token, interval, from_date, to_date)
    if df is None or df.empty:
        return None

    df = apply_pine_buy_logic(df)
    if df.empty:
        return None

    buy_rows = df[df["buy_signal"]].copy()

    latest = df.iloc[-1]
    latest_buy_row = buy_rows.iloc[-1] if not buy_rows.empty else None

    result = {
        "symbol": symbol,
        "df": df,
        "buy_rows": buy_rows,
        "latest_row": latest,
        "latest_buy_row": latest_buy_row,
        "latest_close": round(latest["close"], 2),
        "fib_high": round(latest["fib_high"], 2) if pd.notna(latest["fib_high"]) else None,
        "fib_low": round(latest["fib_low"], 2) if pd.notna(latest["fib_low"]) else None,
        "has_buy_in_range": latest_buy_row is not None,
        "total_buy_signals": int(df["buy_signal"].sum()),
    }
    return result

# =========================================================
# SIDEBAR INPUTS
# =========================================================
st.sidebar.header("Scanner Inputs")

timeframe_label = st.sidebar.selectbox("Time Frame", ["1 Day", "1 Hour"], index=0)
interval = INTERVAL_MAP[timeframe_label]

from_date_input = st.sidebar.date_input("From Date", dt.date.today() - dt.timedelta(days=365))
to_date_input = st.sidebar.date_input("To Date", dt.date.today())

if from_date_input > to_date_input:
    st.sidebar.error("From Date cannot be greater than To Date.")
    st.stop()

from_date = dt.datetime.combine(from_date_input, dt.time.min)
to_date = dt.datetime.combine(to_date_input, dt.time.min)

# =========================================================
# TABS
# =========================================================
tab1, tab2 = st.tabs([
    "📦 Buy Signal Stocks",
    "🔍 Stock Specific Search"
])

# =========================================================
# TAB 1
# =========================================================
with tab1:
    st.subheader("Stocks With Latest Buy Signal In Selected Date Range")

    items = list(stock_list.items())
    batches = [items[i:i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]

    batch_no = st.selectbox("Select Batch Number", list(range(1, len(batches) + 1)))
    selected_batch = batches[batch_no - 1]

    run_scan = st.button("Run Buy Signal Scanner")

    if run_scan:
        rows = []
        failed_symbols = []
        progress = st.progress(0.0)

        for i, (symbol, token) in enumerate(selected_batch):
            result = analyze_stock(symbol, token, interval, from_date, to_date)

            if result is None:
                failed_symbols.append(symbol)
            else:
                if result["has_buy_in_range"]:
                    latest_buy = result["latest_buy_row"]
                    rows.append({
                        "Symbol": result["symbol"],
                        "Latest Buy Time": latest_buy["timestamp"],
                        "Buy Close": round(latest_buy["close"], 2),
                        "Fib High": round(latest_buy["fib_high"], 2) if pd.notna(latest_buy["fib_high"]) else None,
                        "Fib Low": round(latest_buy["fib_low"], 2) if pd.notna(latest_buy["fib_low"]) else None,
                        "Current Close": result["latest_close"],
                        "Total Buy Signals In Range": result["total_buy_signals"],
                    })

            progress.progress((i + 1) / len(selected_batch))
            time.sleep(FETCH_DELAY_SECONDS)

        if rows:
            out_df = pd.DataFrame(rows).sort_values(by=["Latest Buy Time", "Symbol"], ascending=[False, True])
            st.success(f"{len(out_df)} stocks found with buy signal in selected date range.")
            st.dataframe(out_df, use_container_width=True)

            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                out_df.to_excel(writer, sheet_name="Latest Buy Signals", index=False)

            st.download_button(
                "Download Buy Signal List",
                data=buffer.getvalue(),
                file_name="nandi_latest_buy_signals.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.warning("No stocks found with buy signal in this batch for selected date range.")

        if failed_symbols:
            st.info(f"Failed / access issue in {len(failed_symbols)} symbols.")

# =========================================================
# TAB 2
# =========================================================
with tab2:
    st.subheader("Single Stock Search")

    stock_names = list(stock_list.keys())
    selected_stock = st.selectbox("Select Stock", stock_names)
    run_single = st.button("Analyze Stock")

    if run_single:
        token = stock_list[selected_stock]
        result = analyze_stock(selected_stock, token, interval, from_date, to_date)

        if result is None:
            st.error("Failed to fetch data or calculate signals.")
        else:
            df = result["df"]
            buy_rows = result["buy_rows"]
            latest_buy_row = result["latest_buy_row"]

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Current Close", result["latest_close"])
            c2.metric("Current Fib High", result["fib_high"] if result["fib_high"] is not None else "NA")
            c3.metric("Current Fib Low", result["fib_low"] if result["fib_low"] is not None else "NA")
            c4.metric("Buy Signals In Range", result["total_buy_signals"])

            if latest_buy_row is not None:
                d1, d2, d3 = st.columns(3)
                d1.metric("Latest Buy Time", str(latest_buy_row["timestamp"]))
                d2.metric("Latest Buy Close", round(latest_buy_row["close"], 2))
                d3.metric("Latest Buy Status", "BUY SIGNAL")
            else:
                st.info("No buy signal found in selected date range.")

            st.markdown("### Buy Signal Rows")
            if not buy_rows.empty:
                signal_view = buy_rows[[
                    "timestamp", "open", "high", "low", "close",
                    "fib_high", "fib_low",
                    "white_candle", "raw_breakout",
                    "normal_nandi_buy", "perfect_nandi_buy", "buy_signal"
                ]].copy()
                st.dataframe(signal_view, use_container_width=True)
            else:
                st.info("No buy signals found in the selected date range.")

            st.markdown("### Latest 30 Candles")
            latest_view = df[[
                "timestamp", "open", "high", "low", "close", "volume",
                "fib_high", "fib_low",
                "white_candle", "vol_spike", "raw_breakout",
                "normal_nandi_buy", "perfect_nandi_buy", "buy_signal", "signal_status"
            ]].tail(30).copy()
            st.dataframe(latest_view, use_container_width=True)

            buffer = io.BytesIO()
            export_df = df[[
                "timestamp", "open", "high", "low", "close", "volume",
                "fib_high", "fib_low",
                "white_candle", "vol_spike", "raw_breakout",
                "normal_nandi_buy", "perfect_nandi_buy", "buy_signal", "signal_status"
            ]].copy()

            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                export_df.to_excel(writer, sheet_name="Stock Analysis", index=False)

            st.download_button(
                "Download Stock Analysis",
                data=buffer.getvalue(),
                file_name=f"{selected_stock}_nandi_analysis.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
