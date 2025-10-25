import logging
import os
import json
from typing import Dict
import re
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery

# ================ LOGGING ================
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

# Allow DEBUG via env
if os.getenv("DEBUG", "0") in ("1", "true", "True"):
    logging.getLogger().setLevel(logging.DEBUG)

# ================ CONFIG =================
CONFIG_FILE = os.getenv("ETL_CONFIG_FILE", "config.json")
GSHEET_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "powerbi-etl-e1ebfd104446.json")
GCP_PROJECT = os.getenv("GCP_PROJECT")  # optional
BQ_DATASET = os.getenv("BQ_DATASET", "etl_dataset")  # use lowercase id

# ================ HELPERS ================
def connect_gsheet():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(GSHEET_CREDENTIALS, scope)
        client = gspread.authorize(creds)
        logging.info("✅ Connected to Google Sheets")
        return client
    except Exception:
        logging.exception("❌ Failed to connect to Google Sheets")
        raise

def connect_bigquery():
    try:
        client = bigquery.Client(project=GCP_PROJECT) if GCP_PROJECT else bigquery.Client()
        logging.info(f"✅ Connected to BigQuery (project={client.project})")
        return client
    except Exception:
        logging.exception("❌ Failed to connect to BigQuery")
        raise

def safe_table_name(name: str) -> str:
    import re
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_") or "sheet"

def ensure_dataset(client: bigquery.Client, dataset_id: str):
    full_id = f"{client.project}.{dataset_id}"
    try:
        client.get_dataset(full_id)
    except Exception:
        logging.info(f"Creating dataset {full_id}")
        ds = bigquery.Dataset(full_id)
        client.create_dataset(ds, exists_ok=True)

def bq_safe_columns(df):
    """
    Return a copy of df with BigQuery-safe column names and a mapping dict.
    Rules:
      - strip spaces
      - lowercase
      - replace any non [a-z0-9_] with _
      - ensure starts with a letter or underscore (prefix 'col_' if not)
      - truncate to 300 chars (BQ limit)
      - deduplicate by appending _2, _3, ...
    """
    orig = list(df.columns)
    new_cols = []
    seen = set()
    for c in orig:
        s = str(c).strip().lower()
        s = re.sub(r'[^a-z0-9_]', '_', s)  # spaces & punctuation -> _
        if not re.match(r'^[a-z_]', s):
            s = 'col_' + s
        s = s[:300] if len(s) > 300 else s
        if s == '' or s == '_':
            s = 'col'
        base = s
        i = 2
        while s in seen:
            suffix = f"_{i}"
            s = (base[:300-len(suffix)] if len(base) + len(suffix) > 300 else base) + suffix
            i += 1
        seen.add(s)
        new_cols.append(s)

    mapping = dict(zip(orig, new_cols))
    if mapping != {c: c for c in orig}:
        logging.info(f"🔤 Column rename mapping applied: {mapping}")
    df2 = df.copy()
    df2.columns = new_cols
    return df2, mapping

def load_to_bigquery(client: bigquery.Client, dataset: str, table: str, df: pd.DataFrame):
    if df.empty:
        logging.info(f"⚠ No data to load for {table}")
        return

    # Sanitize column names
    df, _ = bq_safe_columns(df)

    # Normalize timestamp column
    lower_to_actual = {c.lower(): c for c in df.columns}
    ts_col = lower_to_actual.get("submitted_at")
    if ts_col:
        df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")

    # Convert other columns to string
    for c in df.columns:
        if c != ts_col:
            df[c] = df[c].where(df[c].isna(), df[c].astype(str))

    # Ensure dataset exists
    ensure_dataset(client, dataset)

    target_table = f"{client.project}.{dataset}.{safe_table_name(table)}"
    staging_table = f"{client.project}.{dataset}.{safe_table_name(table)}_staging"

    # 🔹 Fetch target schema (if table exists)
    try:
        table_obj = client.get_table(target_table)
        expected_cols = [field.name for field in table_obj.schema]

        # Add any missing columns to df with None
        for col in expected_cols:
            if col not in df.columns:
                df[col] = None

        # Reorder df to match target schema
        df = df[expected_cols]
    except Exception:
        # Table doesn't exist yet → first load will create it
        logging.info(f"ℹ️ Target table {target_table} not found, will be created on first load")
        expected_cols = list(df.columns)

    # Build schema for staging
    schema = []
    for c in expected_cols:
        if ts_col and c == ts_col:
            schema.append(bigquery.SchemaField(c, "TIMESTAMP"))
        else:
            schema.append(bigquery.SchemaField(c, "STRING"))

    # 1) Load batch into staging table (overwrite each run)
    load_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=schema,
    )

    logging.info(f"⬆️ Loading {len(df)} rows into staging table {staging_table}")
    client.load_table_from_dataframe(df, staging_table, job_config=load_config).result()

    # 2) Merge staging into target (insert only new cdn)
    merge_sql = f"""
    MERGE `{target_table}` T
    USING `{staging_table}` S
    ON T.cdn = S.cdn
    WHEN NOT MATCHED THEN
      INSERT ROW
    """

    logging.info(f"🔄 Merging staging into {target_table}")
    client.query(merge_sql).result()
    logging.info(f"✅ Merge complete for {table}")

    # 3) Drop staging table to keep dataset clean
    try:
        client.delete_table(staging_table, not_found_ok=True)
        logging.info(f"🧹 Dropped staging table {staging_table}")
    except Exception as e:
        logging.warning(f"Could not drop staging table {staging_table}: {e}")
# ================ MAIN ====================
if __name__ == "__main__":
    logging.info("🚀 Starting ETL Job (BigQuery)")
    try:
        gclient = connect_gsheet()
        bq_client = connect_bigquery()

        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            sheet_files: Dict[str, dict] = json.load(f)
        logging.info("✅ Loaded config.json")

        for _, cfg in sheet_files.items():
            file_name = cfg["file_name"]
            sheet_map = cfg.get("sheet_file", {})
            logging.info(f"📂 Processing file: {file_name}")
            gfile = gclient.open(file_name)

            for ws_name, table_name in sheet_map.items():
                try:
                    logging.info(f"➡ Reading '{ws_name}' → BQ table '{table_name}'")
                    worksheet = gfile.worksheet(ws_name)
                    rows = worksheet.get_all_records()
                    if not rows:
                        logging.info(f"⚠ Sheet '{ws_name}' empty; skipping")
                        continue

                    df = pd.DataFrame(rows)
                    df = df.loc[:, df.columns.notna()]
                    load_to_bigquery(bq_client, BQ_DATASET, table_name, df)
                except Exception:
                    logging.exception(f"❌ Failed loading sheet {ws_name} into {table_name}")
    except Exception:
        logging.exception("❌ ETL job failed (top-level)")
        raise
    finally:
        logging.info("✅ ETL Job Finished")






