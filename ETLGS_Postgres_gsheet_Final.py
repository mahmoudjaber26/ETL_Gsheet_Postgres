import logging
import os
import json
from typing import Dict

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery

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
CONFIG_FILE = os.getenv("ETL_CONFIG_FILE", "config.json")
GSHEET_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "powerbi-etl-e1ebfd104446.json")
GCP_PROJECT = os.getenv("GCP_PROJECT")  # optional; if not set, client will infer from credentials
BQ_DATASET = os.getenv("BQ_DATASET", "ETL_Dataset")  # change if you prefer a different dataset name

# ================== HELPERS ==================
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

def connect_bigquery():
    try:
        client = bigquery.Client(project=GCP_PROJECT) if GCP_PROJECT else bigquery.Client()
        logging.info(f"✅ Connected to BigQuery (project={client.project})")
        return client
    except Exception as e:
        logging.error(f"❌ Failed to connect to BigQuery: {e}")
        raise

def safe_table_name(name: str) -> str:
    """
    Make a BigQuery-friendly table name:
    - lower-case
    - replace spaces and non-alphanumerics with underscores
    - trim to 1024 chars (BQ limit is 1024 for table id part)
    """
    import re
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_") or "sheet"

def load_to_bigquery(client: bigquery.Client, dataset: str, table: str, df: pd.DataFrame):
    if df.empty:
        logging.info(f"⚠ No data to load for {table}")
        return

    # Normalize columns (strip) and coerce timestamps if a common header exists
    df.columns = [str(c).strip() for c in df.columns]
    if "Submitted At" in df.columns:
        df["Submitted At"] = pd.to_datetime(df["Submitted At"], errors="coerce")

    table_id = f"{client.project}.{dataset}.{safe_table_name(table)}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # keep history
        autodetect=True,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )

    # Ensure dataset exists
    try:
        client.get_dataset(dataset)
    except Exception:
        logging.info(f"Creating dataset {dataset} in project {client.project}")
        client.create_dataset(dataset, exists_ok=True)

    logging.info(f"⬆️ Loading {len(df)} rows into {table_id}")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # wait
    logging.info(f"✅ Loaded {len(df)} rows into {table_id}")

# ================== MAIN ==================
if _name_ == "_main_":
    logging.info("🚀 Starting ETL Job (BigQuery)")

    try:
        gclient = connect_gsheet()
        bq_client = connect_bigquery()

        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            sheet_files: Dict[str, dict] = json.load(f)
        logging.info("✅ Loaded config.json")

        # Iterate each Google Spreadsheet and its worksheet→table mapping
        for _, cfg in sheet_files.items():
            file_name = cfg["file_name"]
            sheet_map = cfg.get("sheet_file", {})  # {"WorksheetName": "target_table_name", ...}
            logging.info(f"📂 Processing file: {file_name}")
            gfile = gclient.open(file_name)

            for ws_name, table_name in sheet_map.items():
                try:
                    logging.info(f"➡ Reading sheet '{ws_name}' → BQ table '{table_name}'")
                    worksheet = gfile.worksheet(ws_name)
                    rows = worksheet.get_all_records()
                    if not rows:
                        logging.info(f"⚠ Sheet '{ws_name}' empty; skipping")
                        continue

                    df = pd.DataFrame(rows)
                    # Drop columns that are literally None as header
                    df = df.loc[:, df.columns.notna()]
                    load_to_bigquery(bq_client, BQ_DATASET, table_name, df)

                except Exception as e:
                    logging.error(f"❌ Failed loading sheet {ws_name} into {table_name}: {e}")

    except Exception as e:
        logging.error(f"❌ ETL job failed: {e}")

    logging.info("✅ ETL Job Finished")
