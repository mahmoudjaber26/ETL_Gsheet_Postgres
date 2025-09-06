import logging
import os
import json
from typing import Dict

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

# ================ CONFIG =================
CONFIG_FILE = os.getenv("ETL_CONFIG_FILE", "config.json")
GSHEET_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "powerbi-etl-e1ebfd104446.json")
GCP_PROJECT = os.getenv("GCP_PROJECT")  # optional
BQ_DATASET = os.getenv("BQ_DATASET", "etl_dataset")  # use lowercase id

# ================ HELPERS ================
def connect_gsheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GSHEET_CREDENTIALS, scope)
    client = gspread.authorize(creds)
    logging.info("✅ Connected to Google Sheets")
    return client

def connect_bigquery():
    client = bigquery.Client(project=GCP_PROJECT) if GCP_PROJECT else bigquery.Client()
    logging.info(f"✅ Connected to BigQuery (project={client.project})")
    return client

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

def load_to_bigquery(client: bigquery.Client, dataset: str, table: str, df: pd.DataFrame):
    if df.empty:
        logging.info(f"⚠ No data to load for {table}")
        return

    df.columns = [str(c).strip() for c in df.columns]
    if "Submitted At" in df.columns:
        df["Submitted At"] = pd.to_datetime(df["Submitted At"], errors="coerce")

    ensure_dataset(client, dataset)

    table_id = f"{client.project}.{dataset}.{safe_table_name(table)}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    logging.info(f"⬆️ Loading {len(df)} rows into {table_id}")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    logging.info(f"✅ Loaded {len(df)} rows into {table_id}")

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
                except Exception as e:
                    logging.error(f"❌ Failed loading sheet {ws_name} into {table_name}: {e}")

    except Exception as e:
        logging.error(f"❌ ETL job failed: {e}")
    logging.info("✅ ETL Job Finished")
