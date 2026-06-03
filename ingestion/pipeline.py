#!/usr/bin/env python3
"""
pipeline.py – Aluminum Signal Pipeline

Fetches daily ALI=F prices from Yahoo Finance, computes 30/90-day
calendar-day moving averages and mean-reversion signals in pandas,
then writes DAILY / WEEKLY / MONTHLY granularities to Google Sheets.

No Snowflake or dbt required.

Usage:
    python ingestion/pipeline.py

Environment variables:
    GOOGLE_SERVICE_ACCOUNT_PATH   path to service account JSON file
    GOOGLE_SERVICE_ACCOUNT_JSON   service account JSON as a string (alternative)
"""

import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

load_dotenv()

TICKER        = "ALI=F"
FETCH_DAYS    = 400        # enough for 90-day MA + 13 months of history
SHEET_ID      = "1ihpy_x48lPwr4KGbrBBshTIV5sIPuk3SXWm9FZ2sGFs"
WORKSHEET_GID = 535344928


# ---------------------------------------------------------------------------
# 1. Fetch
# ---------------------------------------------------------------------------

def fetch_prices() -> pd.DataFrame:
    start = (pd.Timestamp.today() - pd.Timedelta(days=FETCH_DAYS)).strftime("%Y-%m-%d")
    raw = yf.Ticker(TICKER).history(start=start, interval="1d", auto_adjust=True)
    if raw.empty:
        raise ValueError(f"No data returned from Yahoo Finance for {TICKER}")

    df = raw.reset_index()[["Date", "Close"]].copy()
    df.columns = ["price_date", "close_price"]
    df["price_date"] = pd.to_datetime(df["price_date"]).dt.tz_localize(None).dt.normalize()
    df = df[df["close_price"].notna()].sort_values("price_date").reset_index(drop=True)
    print(f"  Fetched {len(df)} trading days  "
          f"({df['price_date'].min().date()} → {df['price_date'].max().date()})")
    return df


# ---------------------------------------------------------------------------
# 2. Transform  (replicates dbt staging + intermediate + mart SQL logic)
# ---------------------------------------------------------------------------

def compute_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replicates int_aluminum_moving_averages + mart_aluminum_signal.

    Uses pandas calendar-day rolling windows, which match Snowflake's
    RANGE BETWEEN INTERVAL 'N days' PRECEDING AND CURRENT ROW exactly:
    both windows span [price_date - N days, price_date] inclusive.
    """
    df = df.set_index("price_date").sort_index()

    roll_30 = df["close_price"].rolling("30D", min_periods=1)
    roll_90 = df["close_price"].rolling("90D", min_periods=1)

    df["ma_30d"]             = roll_30.mean().round(2)
    df["ma_90d"]             = roll_90.mean().round(2)
    df["days_in_90d_window"] = roll_90.count().astype(int)
    df["price_vs_ma_90d"]    = (df["close_price"] - df["ma_90d"]).round(2)
    df["price_vs_ma_90d_pct"] = (
        (df["close_price"] - df["ma_90d"]) / df["ma_90d"] * 100
    ).round(2)
    df["ma_spread"] = (df["ma_30d"] - df["ma_90d"]).round(2)

    def _signal(row):
        if row["days_in_90d_window"] < 15:
            return "INSUFFICIENT DATA"
        if row["close_price"] < row["ma_30d"] and row["close_price"] < row["ma_90d"]:
            return "BUY"
        if row["close_price"] > row["ma_30d"] and row["close_price"] > row["ma_90d"]:
            return "HOLD"
        return "NEUTRAL"

    df["signal"] = df.apply(_signal, axis=1)
    return df.reset_index()


def build_granularities(df: pd.DataFrame, refreshed_at: str) -> pd.DataFrame:
    """
    Replicates mart_aluminum_signal granularity expansion:
      DAILY   – last 14 calendar days of data
      WEEKLY  – last trading day of each week, 13 most recent weeks
      MONTHLY – last trading day of each month, 13 most recent months
    """
    max_date = df["price_date"].max()

    # DAILY: last 14 days
    daily = df[df["price_date"] >= max_date - pd.Timedelta(days=13)].copy()
    daily.insert(0, "granularity", "DAILY")

    # WEEKLY: last trading day per week, 13 most recent
    wk = df.copy()
    wk["_week"] = wk["price_date"].dt.to_period("W")
    weekly = (
        wk.sort_values("price_date")
        .groupby("_week", as_index=False)
        .last()
        .sort_values("price_date", ascending=False)
        .head(13)
        .drop(columns="_week")
    )
    weekly.insert(0, "granularity", "WEEKLY")

    # MONTHLY: last trading day per month, 13 most recent
    mo = df.copy()
    mo["_month"] = mo["price_date"].dt.to_period("M")
    monthly = (
        mo.sort_values("price_date")
        .groupby("_month", as_index=False)
        .last()
        .sort_values("price_date", ascending=False)
        .head(13)
        .drop(columns="_month")
    )
    monthly.insert(0, "granularity", "MONTHLY")

    result = pd.concat([daily, weekly, monthly], ignore_index=True)
    result["refreshed_at"] = refreshed_at
    return result.sort_values(["granularity", "price_date"], ascending=[True, False])


# ---------------------------------------------------------------------------
# 3. Write to Google Sheets
# ---------------------------------------------------------------------------

OUTPUT_COLUMNS = [
    "granularity", "price_date", "close_price", "ma_30d", "ma_90d",
    "price_vs_ma_90d", "price_vs_ma_90d_pct", "ma_spread", "signal", "refreshed_at",
]

def _sheets_client() -> gspread.Client:
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    sa_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT_PATH")
    scopes  = ["https://www.googleapis.com/auth/spreadsheets"]

    if sa_json:
        creds = Credentials.from_service_account_info(json.loads(sa_json), scopes=scopes)
        return gspread.authorize(creds)
    if sa_path:
        return gspread.service_account(filename=sa_path)
    raise EnvironmentError(
        "Set GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_PATH"
    )


def push_to_sheets(df: pd.DataFrame) -> None:
    out = df[OUTPUT_COLUMNS].copy()
    out["price_date"] = out["price_date"].astype(str)

    gc  = _sheets_client()
    ws  = gc.open_by_key(SHEET_ID).get_worksheet_by_id(WORKSHEET_GID)
    ws.clear()
    ws.append_row([c.upper() for c in OUTPUT_COLUMNS], value_input_option="RAW")
    ws.append_rows(
        [[str(v) if v is not None else "" for v in row] for row in out.itertuples(index=False)],
        value_input_option="RAW",
    )
    print(f"  Wrote {len(out)} rows to Google Sheet")


# ---------------------------------------------------------------------------
# 4. Main
# ---------------------------------------------------------------------------

def main() -> None:
    refreshed_at = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S %Z")

    print("Fetching prices...")
    df = fetch_prices()

    print("Computing signals...")
    df = compute_signals(df)

    print("Building granularities...")
    df = build_granularities(df, refreshed_at)
    signal_counts = df.groupby("granularity")["signal"].value_counts().to_string()
    print(f"  Signal summary:\n{signal_counts}")

    print("Pushing to Google Sheets...")
    push_to_sheets(df)

    print("Done.")


if __name__ == "__main__":
    main()
