"""
Reads ALUMINUM.MARTS.MART_ALUMINUM_SIGNAL from Snowflake and writes
it to a Google Sheet using a service account.
"""

import os
import json

import snowflake.connector
import gspread
from dotenv import load_dotenv

load_dotenv()

SHEET_ID      = "1ihpy_x48lPwr4KGbrBBshTIV5sIPuk3SXWm9FZ2sGFs"
WORKSHEET_GID = 535344928

QUERY = """
select
    granularity,
    price_date::string                          as price_date,
    close_price,
    ma_30d,
    ma_90d,
    price_vs_ma_90d,
    price_vs_ma_90d_pct,
    ma_spread,
    signal
from ALUMINUM.MARTS.MART_ALUMINUM_SIGNAL
order by granularity, price_date desc
"""


def _load_private_key():
    from cryptography.hazmat.primitives.serialization import (
        load_pem_private_key, Encoding, PrivateFormat, NoEncryption
    )
    key_path   = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "")
    with open(key_path, "rb") as f:
        pk = load_pem_private_key(f.read(), password=passphrase.encode() if passphrase else None)
    return pk.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())


def fetch_from_snowflake():
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=_load_private_key(),
        role="TRANSFORM",
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database="ALUMINUM",
        schema="MARTS",
    )
    cursor = conn.cursor()
    cursor.execute(QUERY)
    columns = [d[0].upper() for d in cursor.description]
    rows    = cursor.fetchall()
    cursor.close()
    conn.close()
    return columns, rows


def push_to_sheets(columns, rows):
    sa_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    gc       = gspread.service_account_from_dict(sa_info)
    sheet    = gc.open_by_key(SHEET_ID)
    ws       = sheet.get_worksheet_by_id(WORKSHEET_GID)

    ws.clear()
    ws.append_row(columns, value_input_option="RAW")
    ws.append_rows(
        [[str(v) if v is not None else "" for v in row] for row in rows],
        value_input_option="RAW",
    )
    print(f"Wrote {len(rows)} rows to Google Sheet")


if __name__ == "__main__":
    print("Fetching from Snowflake...")
    columns, rows = fetch_from_snowflake()
    print(f"Fetched {len(rows)} rows")
    push_to_sheets(columns, rows)
