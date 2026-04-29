"""
Fetches daily aluminum futures EOD prices from Yahoo Finance (ALI=F)
and upserts them into Snowflake ALUMINUM.RAW.RAW_ALUMINUM_PRICES.

Run:
    python ingestion/load_prices.py               # incremental (last 7 days)
    python ingestion/load_prices.py --full         # full history backfill (5 years)
"""

import os
import argparse
from datetime import datetime, timedelta

import yfinance as yf
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

TICKER = "ALI=F"  # CME Aluminum Futures (continuous front-month)
BACKFILL_YEARS = 5


def fetch_prices(full: bool) -> pd.DataFrame:
    if full:
        start = (datetime.today() - timedelta(days=365 * BACKFILL_YEARS)).strftime("%Y-%m-%d")
        print(f"Full backfill from {start}")
    else:
        start = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        print(f"Incremental fetch from {start}")

    ticker = yf.Ticker(TICKER)
    raw = ticker.history(start=start, interval="1d", auto_adjust=True)

    if raw.empty:
        raise ValueError(f"No data returned from Yahoo Finance for {TICKER}. Check the ticker symbol.")

    df = raw.reset_index()
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    df = df.rename(columns={"date": "price_date"})
    df["price_date"] = pd.to_datetime(df["price_date"]).dt.date
    df["volume"] = df["volume"].fillna(0).astype(int)
    df = df[["price_date", "open", "high", "low", "close", "volume"]]
    df = df[df["close"].notna()].copy()
    print(f"Fetched {len(df)} trading days")
    return df


def _load_private_key():
    from cryptography.hazmat.primitives.serialization import load_pem_private_key, Encoding, PrivateFormat, NoEncryption
    key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "")
    passphrase_bytes = passphrase.encode() if passphrase else None
    with open(key_path, "rb") as f:
        private_key = load_pem_private_key(f.read(), password=passphrase_bytes)
    return private_key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())


def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=_load_private_key(),
        role="TRANSFORM",
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database="ALUMINUM",
        schema="RAW",
    )


def setup_table(cursor):
    # Database and schema must already exist — create them in Snowflake as SYSADMIN first
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ALUMINUM.RAW.RAW_ALUMINUM_PRICES (
            price_date    DATE          NOT NULL,
            open_price    FLOAT,
            high_price    FLOAT,
            low_price     FLOAT,
            close_price   FLOAT         NOT NULL,
            volume        BIGINT,
            loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (price_date)
        )
    """)


def upsert(cursor, df: pd.DataFrame):
    cursor.execute("""
        CREATE OR REPLACE TEMPORARY TABLE tmp_aluminum_prices (
            price_date  DATE,
            open_price  FLOAT,
            high_price  FLOAT,
            low_price   FLOAT,
            close_price FLOAT,
            volume      BIGINT
        )
    """)

    rows = [
        (row.price_date, row.open, row.high, row.low, row.close, row.volume)
        for row in df.itertuples(index=False)
    ]
    cursor.executemany(
        "INSERT INTO tmp_aluminum_prices VALUES (%s, %s, %s, %s, %s, %s)",
        rows,
    )

    cursor.execute("""
        MERGE INTO ALUMINUM.RAW.RAW_ALUMINUM_PRICES AS t
        USING tmp_aluminum_prices AS s
          ON t.price_date = s.price_date
        WHEN MATCHED THEN UPDATE SET
            open_price  = s.open_price,
            high_price  = s.high_price,
            low_price   = s.low_price,
            close_price = s.close_price,
            volume      = s.volume,
            loaded_at   = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (price_date, open_price, high_price, low_price, close_price, volume)
        VALUES
            (s.price_date, s.open_price, s.high_price, s.low_price, s.close_price, s.volume)
    """)
    print(f"Upserted {len(rows)} rows into ALUMINUM.RAW.RAW_ALUMINUM_PRICES")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true", help="Full 5-year backfill instead of incremental")
    args = parser.parse_args()

    df = fetch_prices(full=args.full)

    conn = get_connection()
    try:
        cursor = conn.cursor()
        setup_table(cursor)
        upsert(cursor, df)
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
