import os
import io
import time
import datetime as dt

import numpy as np
import pandas as pd
import streamlit as st
import pyotp
from SmartApi.smartConnect import SmartConnect

# ================= CONFIG =================
st.set_page_config(page_title="Fibonacci MA Scanner", layout="wide")
st.title("📈 Fibonacci Moving Average Scanner")

# ================= ANGEL LOGIN =================
# Prefer Streamlit secrets first, then environment variables
api_key = "g5o6vfTl"
client_id = "R59803990"
password = "1234"
totp_secret = "5W4MC6MMLANC3UYOAW2QDUIFEU"

@st.cache_resource
def angel_login():
    if not all([api_key, client_id, password, totp_secret]):
        raise ValueError("Missing Angel credentials in secrets or environment variables.")

    obj = SmartConnect(api_key=api_key)
    totp = pyotp.TOTP(totp_secret).now()
    session = obj.generateSession(client_id, password, totp)

    ok = False
    if isinstance(session, dict):
        ok = session.get("status", session.get("success", False))

    if not ok:
        message = session.get("message", "Login failed") if isinstance(session, dict) else "Login failed"
        raise ValueError(message)

    return obj

try:
    obj = angel_login()
    st.success("Angel One login successful")
except Exception as e:
    st.error(f"Login failed: {e}")
    st.stop()

# ================= STOCK LIST =================
# Example import:
# from Stock_tokens import stock_list
# stock_list = {"RELIANCE": "2885", "TCS": "11536"}

from Stock_tokens import stock_list

# ================= DATA FETCH =================
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
    except Exception as e:
        st.warning(f"API error for token {token}: {e}")
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

# ================= FIBONACCI MA LOGIC =================
FIB_LENGTHS = [5, 8, 13, 21, 34, 55, 89, 144, 233]

def add_fibonacci_ma(df, ma1_length=50, ma2_length=200):
    df = df.copy()

    high_emas = []
    low_emas = []

    for length in FIB_LENGTHS:
        high_emas.append(df["high"].ewm(span=length, adjust=False).mean())
        low_emas.append(df["low"].ewm(span=length, adjust=False).mean())

    df["fib_high"] = sum(high_emas) / len(high_emas)
    df["fib_low"] = sum(low_emas) / len(low_emas)

    df["ma1"] = df["close"].rolling(ma1_length).mean()
    df["ma2"] = df["close"].rolling(ma2_length).mean()

    # Fresh buy only on breakout close above upper band
    df["buy_signal"] = (
        (df["close"] > df["fib_high"]) &
        (df["close"].shift(1) <= df["fib_high"].shift(1))
    )

    # General buy zone
    df["in_buy_zone"] = df["close"] > df["fib_high"]

    # SL condition for open trade
    df["sl_signal"] = df["close"] < df["fib_low"]

    return df

# ================= BACKTEST ENGINE =================
def run_fib_strategy(df, target_pct):
    """
    Entry: candle close above fib_high (fresh breakout)
    Exit 1: if high reaches entry * (1 + target_pct/100)
    Exit 2: if candle closes below fib_low
    """
    df = df.copy()
    trades = []

    in_trade = False
    entry_price = None
    entry_time = None
    entry_index = None
    target_price = None

    for i in range(len(df)):
        row = df.iloc[i]

        if not in_trade:
            if row["buy_signal"]:
                in_trade = True
                entry_price = row["close"]
                entry_time = row["timestamp"]
                entry_index = i
                target_price = entry_price * (1 + target_pct / 100.0)

        else:
            # Priority: target first if reached intrabar
            if row["high"] >= target_price:
                exit_price = target_price
                exit_time = row["timestamp"]
                trades.append({
                    "Entry Time": entry_time,
                    "Entry Price": round(entry_price, 2),
                    "Exit Time": exit_time,
                    "Exit Price": round(exit_price, 2),
                    "Exit Type": "TARGET",
                    "Return %": round(((exit_price / entry_price) - 1) * 100, 2),
                    "Bars Held": i - entry_index
                })
                in_trade = False
                entry_price = None
                entry_time = None
                entry_index = None
                target_price = None

            elif row["sl_signal"]:
                exit_price = row["close"]
                exit_time = row["timestamp"]
                trades.append({
                    "Entry Time": entry_time,
                    "Entry Price": round(entry_price, 2),
                    "Exit Time": exit_time,
                    "Exit Price": round(exit_price, 2),
                    "Exit Type": "SL",
                    "Return %": round(((exit_price / entry_price) - 1) * 100, 2),
                    "Bars Held": i - entry_index
                })
                in_trade = False
                entry_price = None
                entry_time = None
                entry_index = None
                target_price = None

    open_trade = None
    if in_trade:
        last_row = df.iloc[-1]
        open_trade = {
            "Entry Time": entry_time,
            "Entry Price": round(entry_price, 2),
            "Current Time": last_row["timestamp"],
            "Current Price": round(last_row["close"], 2),
            "Target Price": round(target_price, 2),
            "Unrealized %": round(((last_row["close"] / entry_price) - 1) * 100, 2)
        }

    trades_df = pd.DataFrame(trades)

    total_buys = len(trades) + (1 if open_trade is not None else 0)
    closed_trades = len(trades)

    target_hits = 0
    sl_hits = 0

    if not trades_df.empty:
        target_hits = (trades_df["Exit Type"] == "TARGET").sum()
        sl_hits = (trades_df["Exit Type"] == "SL").sum()

    target_hit_pct = round((target_hits / closed_trades) * 100, 2) if closed_trades > 0 else 0.0
    sl_hit_pct = round((sl_hits / closed_trades) * 100, 2) if closed_trades > 0 else 0.0

    # "Accuracy" here = target hit rate among closed trades
    accuracy_pct = target_hit_pct

    # Probability estimate for next buy
    # Based on historical closed trades only
    win_probability = target_hit_pct
    sl_probability = sl_hit_pct

    latest = df.iloc[-1]
    if open_trade is not None:
        current_condition = "IN OPEN BUY POSITION"
    elif latest["buy_signal"]:
        current_condition = "FRESH BUY SIGNAL"
    elif latest["in_buy_zone"]:
        current_condition = "BUY ZONE"
    else:
        current_condition = "NO ENTRY"

    summary = {
        "Total Buys": total_buys,
        "Closed Trades": closed_trades,
        "Target Hits": int(target_hits),
        "SL Hits": int(sl_hits),
        "Accuracy %": accuracy_pct,
        "Target Hit %": target_hit_pct,
        "SL Hit %": sl_hit_pct,
        "Win Probability Next Buy %": win_probability,
        "SL Probability Next Buy %": sl_probability,
        "Current Condition": current_condition
    }

    return df, trades_df, open_trade, summary

# ================= SCAN FUNCTION =================
def analyze_stock(symbol, token, interval, from_date, to_date, target_pct):
    df = fetch_data(token, interval, from_date, to_date)
    if df is None or df.empty:
        return None

    df = add_fibonacci_ma(df)
    df, trades_df, open_trade, summary = run_fib_strategy(df, target_pct)
    latest = df.iloc[-1]

    result = {
        "symbol": symbol,
        "df": df,
        "trades_df": trades_df,
        "open_trade": open_trade,
        "summary": summary,
        "latest_close": round(latest["close"], 2),
        "fib_high": round(latest["fib_high"], 2),
        "fib_low": round(latest["fib_low"], 2),
        "ma1": round(latest["ma1"], 2) if pd.notna(latest["ma1"]) else None,
        "ma2": round(latest["ma2"], 2) if pd.notna(latest["ma2"]) else None,
        "last_candle": latest["timestamp"]
    }
    return result

# ================= TABS =================
tab1, tab2, tab3 = st.tabs([
    "📦 Buy Zone Scanner",
    "🔍 Single Stock Analyzer",
    "📋 Trade Log / Backtest Summary"
])

# ================= COMMON INPUTS =================
st.sidebar.header("Scanner Inputs")
interval = st.sidebar.selectbox("Interval", ["ONE_DAY", "ONE_HOUR"], index=0)
from_date = st.sidebar.date_input("From Date", dt.date.today() - dt.timedelta(days=365))
to_date = st.sidebar.date_input("To Date", dt.date.today())
target_pct = st.sidebar.number_input("Target %", min_value=1.0, max_value=100.0, value=10.0, step=0.5)

# ================= TAB 1 =================
with tab1:
    st.subheader("Stocks Currently in Buy Zone")

    items = list(stock_list.items())
    batch_size = st.selectbox("Batch Size", [50, 100, 150, 200], index=1)
    batches = [items[i:i + batch_size] for i in range(0, len(items), batch_size)]
    batch_no = st.selectbox("Select Batch", list(range(1, len(batches) + 1)), key="tab1_batch")
    selected_batch = batches[batch_no - 1]

    scan_buy_zone = st.button("Run Buy Zone Scan")

    if scan_buy_zone:
        rows = []
        progress = st.progress(0)

        for i, (symbol, token) in enumerate(selected_batch):
            result = analyze_stock(symbol, token, interval, from_date, to_date, target_pct)
            if result is not None:
                cond = result["summary"]["Current Condition"]
                if cond in ["BUY ZONE", "FRESH BUY SIGNAL", "IN OPEN BUY POSITION"]:
                    rows.append({
                        "Symbol": symbol,
                        "Last Candle": result["last_candle"],
                        "Close": result["latest_close"],
                        "Fib High": result["fib_high"],
                        "Fib Low": result["fib_low"],
                        "Condition": cond,
                        "Accuracy %": result["summary"]["Accuracy %"],
                        "Target Hit %": result["summary"]["Target Hit %"],
                        "SL Hit %": result["summary"]["SL Hit %"],
                        "Win Probability %": result["summary"]["Win Probability Next Buy %"]
                    })

            progress.progress((i + 1) / len(selected_batch))
            time.sleep(0.1)

        if rows:
            out_df = pd.DataFrame(rows).sort_values(
                by=["Condition", "Win Probability %", "Accuracy %"],
                ascending=[True, False, False]
            )
            st.dataframe(out_df, use_container_width=True)

            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                out_df.to_excel(writer, index=False, sheet_name="Buy Zone")

            st.download_button(
                "Download Buy Zone List",
                data=buffer.getvalue(),
                file_name="fib_buy_zone_scanner.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.warning("No stocks found in buy zone for this batch.")

# ================= TAB 2 =================
with tab2:
    st.subheader("Single Stock Analyzer")

    stock_names = list(stock_list.keys())
    selected_stock = st.selectbox("Select Stock", stock_names)

    run_single = st.button("Analyze Selected Stock")

    if run_single:
        token = stock_list[selected_stock]
        result = analyze_stock(selected_stock, token, interval, from_date, to_date, target_pct)

        if result is None:
            st.error("Failed to fetch or analyze data.")
        else:
            summary = result["summary"]
            df = result["df"]
            trades_df = result["trades_df"]
            open_trade = result["open_trade"]

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Buys", summary["Total Buys"])
            c2.metric("Accuracy %", summary["Accuracy %"])
            c3.metric("Target Hit %", summary["Target Hit %"])
            c4.metric("SL Hit %", summary["SL Hit %"])

            c5, c6, c7, c8 = st.columns(4)
            c5.metric("Current Close", result["latest_close"])
            c6.metric("Fib High", result["fib_high"])
            c7.metric("Fib Low", result["fib_low"])
            c8.metric("Condition", summary["Current Condition"])

            st.markdown("### Probability View")
            p1, p2 = st.columns(2)
            p1.metric("Next Buy Win Probability %", summary["Win Probability Next Buy %"])
            p2.metric("Next Buy SL Probability %", summary["SL Probability Next Buy %"])

            if open_trade is not None:
                st.markdown("### Open Position")
                st.json(open_trade)

            st.markdown("### Latest 20 Candles")
            show_cols = [
                "timestamp", "open", "high", "low", "close",
                "fib_high", "fib_low", "ma1", "ma2",
                "buy_signal", "in_buy_zone", "sl_signal"
            ]
            st.dataframe(df[show_cols].tail(20), use_container_width=True)

            st.markdown("### Closed Trade Summary")
            if not trades_df.empty:
                st.dataframe(trades_df, use_container_width=True)
            else:
                st.info("No closed trades found in selected date range.")

# ================= TAB 3 =================
with tab3:
    st.subheader("Trade Log / Backtest Summary")

    stock_names_3 = list(stock_list.keys())
    selected_stock_3 = st.selectbox("Select Stock for Trade Log", stock_names_3, key="tab3_stock")
    run_tab3 = st.button("Generate Trade Log", key="tab3_run")

    if run_tab3:
        token = stock_list[selected_stock_3]
        result = analyze_stock(selected_stock_3, token, interval, from_date, to_date, target_pct)

        if result is None:
            st.error("Failed to fetch or analyze data.")
        else:
            trades_df = result["trades_df"]
            summary = result["summary"]

            s1, s2, s3, s4, s5 = st.columns(5)
            s1.metric("Total Buys", summary["Total Buys"])
            s2.metric("Closed Trades", summary["Closed Trades"])
            s3.metric("Target Hits", summary["Target Hits"])
            s4.metric("SL Hits", summary["SL Hits"])
            s5.metric("Condition", summary["Current Condition"])

            if not trades_df.empty:
                total_return = round(trades_df["Return %"].sum(), 2)
                avg_return = round(trades_df["Return %"].mean(), 2)
                avg_bars = round(trades_df["Bars Held"].mean(), 2)

                b1, b2, b3 = st.columns(3)
                b1.metric("Total Return %", total_return)
                b2.metric("Average Return %", avg_return)
                b3.metric("Avg Bars Held", avg_bars)

                st.dataframe(trades_df, use_container_width=True)

                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                    trades_df.to_excel(writer, index=False, sheet_name="Trade Log")

                st.download_button(
                    "Download Trade Log",
                    data=buffer.getvalue(),
                    file_name=f"{selected_stock_3}_fib_trade_log.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.info("No trades generated in the selected date range.")
