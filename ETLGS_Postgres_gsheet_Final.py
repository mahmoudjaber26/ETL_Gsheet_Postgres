import logging
import os
import json

import psycopg2
from psycopg2.extras import execute_values
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

# ================== LOGGING ==================
logging.basicConfig(
    filename="logs.txt",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.getLogger().addHandler(console)

# ================== CONFIG ==================
DEFAULT_DB_URI = os.getenv("DB_URI", "").strip()
if not DEFAULT_DB_URI:
    # Replace with your own if not using env vars
    DEFAULT_DB_URI = "postgresql://neondb_owner:npg_9vchZgGPO1dj@ep-fancy-surf-a8vwb975.eastus2.azure.neon.tech/neondb?sslmode=require&connect_timeout=10&keepalives=1&keepalives_idle=30&keepalives_interval=10&keepalives_count=5&gssencmode=disable"
    DB_URI = DEFAULT_DB_URI
    
    
    
def _ensure_sslmode(uri: str) -> str:
    if "sslmode=" in uri:
        return uri
    return uri + ("&sslmode=require" if "?" in uri else "?sslmode=require")

DB_URI = _ensure_sslmode(DEFAULT_DB_URI)
CONFIG_FILE = os.getenv("ETL_CONFIG_FILE", "config.json")
GSHEET_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "powerbi-etl-e1ebfd104446.json")

# ================== CONNECTIONS ==================
def connect_postgres():
    import urllib.parse as up
    try:
        # Log (without password)
        safe_uri = DB_URI
        if "://" in safe_uri and "@" in safe_uri:
            scheme, rest = safe_uri.split("://", 1)
            creds, hostdb = rest.split("@", 1)
            if ":" in creds:
                u, p = creds.split(":", 1)
                safe_uri = f"{scheme}://{u}:*@{hostdb}"
        logging.info(f"Connecting with: {safe_uri}")

        conn = psycopg2.connect(DB_URI)
        with conn.cursor() as cur:
            cur.execute("SHOW ssl;")
            ssl_on = cur.fetchone()[0]
        logging.info(f"✅ Connected to Render PostgreSQL (ssl={ssl_on})")
        return conn
    except Exception as e:
        logging.error(f"❌ Failed to connect to PostgreSQL: {e}")
        raise

def connect_gsheet():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(GSHEET_CREDENTIALS, scope)
        client = gspread.authorize(creds)
        logging.info("✅ Connected to Google Sheets")
        return client
    except Exception as e:
        logging.error(f"❌ Failed to connect to Google Sheets: {e}")
        raise

# ================== SCHEMA HELPERS ==================
def get_existing_columns(cursor, table_name):
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table_name,),
    )
    return [r[0] for r in cursor.fetchall()]

def create_table_if_not_exists(cursor, table_name, df):
    cols = []
    for col in df.columns:
        col_type = "TIMESTAMP" if col.strip().lower() == "submitted at" else "TEXT"
        cols.append(f'"{col}" {col_type}')
    ddl = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols)});'
    cursor.execute(ddl)

def ensure_missing_columns(cursor, table_name, df):
    existing = set(get_existing_columns(cursor, table_name))
    for col in df.columns:
        if col not in existing:
            cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" TEXT;')

def ensure_unique_index_on_cdn(cursor, table_name, has_cdn):
    if not has_cdn:
        return
    cursor.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS "ux_{table_name}_cdn" ON "{table_name}" ("cdn");')

# ================== INSERT ==================
def insert_rows(conn, table_name, df):
    if df.empty:
        logging.info(f"⚠ No data to insert for {table_name}")
        return

    df.columns = [c.strip() for c in df.columns]

    with conn.cursor() as cur:
        create_table_if_not_exists(cur, table_name, df)
        ensure_missing_columns(cur, table_name, df)
        conn.commit()

        existing_cols = get_existing_columns(cur, table_name)
        use_cols = [c for c in df.columns if c in existing_cols]
        if not use_cols:
            logging.warning(f"⚠ No overlapping columns for {table_name}; skipping.")
            return

        if "Submitted At" in use_cols:
            df["Submitted At"] = pd.to_datetime(df["Submitted At"], errors="coerce")

        placeholders_cols = ",".join([f'"{c}"' for c in use_cols])
        values = [tuple(row) for row in df[use_cols].itertuples(index=False, name=None)]

        has_cdn = any(c.lower() == "cdn" for c in use_cols)
        if has_cdn:
            ensure_unique_index_on_cdn(cur, table_name, True)
            on_conflict = " ON CONFLICT (cdn) DO NOTHING"
        else:
            on_conflict = ""

        sql = f'INSERT INTO "{table_name}" ({placeholders_cols}) VALUES %s{on_conflict};'
        try:
            execute_values(cur, sql, values)
            conn.commit()
            logging.info(f"✅ Inserted {len(values)} rows into {table_name}")
        except Exception as e:
            conn.rollback()
            logging.error(f"❌ Failed to insert rows into {table_name}: {e}")
            raise

# ================== MAIN ==================
if __name__ == "__main__":
    logging.info("🚀 Starting ETL Job")
    try:
        conn = connect_postgres()
        gclient = connect_gsheet()

        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            sheet_files = json.load(f)
        logging.info("✅ Loaded config.json successfully")

        for _, cfg in sheet_files.items():
            file_name = cfg["file_name"]
            sheet_map = cfg.get("sheet_file", {})
            logging.info(f"📂 Processing file: {file_name}")
            gfile = gclient.open(file_name)

            for ws_name, table_name in sheet_map.items():
                try:
                    logging.info(f"➡ Loading sheet '{ws_name}' → table '{table_name}'")
                    worksheet = gfile.worksheet(ws_name)
                    rows = worksheet.get_all_records()
                    if not rows:
                        logging.info(f"⚠ Sheet '{ws_name}' empty; skipping")
                        continue
                    df = pd.DataFrame(rows)
                    df = df.loc[:, df.columns.notna()]
                    insert_rows(conn, table_name, df)
                except Exception as e:
                    logging.error(f"❌ Failed loading sheet {ws_name} into {table_name}: {e}")

    except Exception as e:
        logging.error(f"❌ ETL job failed: {e}")
    finally:
        try:
            if conn:
                conn.close()
                logging.info("🔒 PostgreSQL connection closed")
        except NameError:
            pass

    logging.info("✅ ETL Job Finished")
