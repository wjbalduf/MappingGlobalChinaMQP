"""
Usage:
    python scripts/10_upload_to_database.py
"""
import pandas as pd
from sqlalchemy import create_engine
import os
import glob

# ---------- Config ----------
non_master_dir = os.path.join("data", "intermediate") 
master_dir = os.path.join("data", "clean") 
db_url = "postgresql+psycopg2://doadmin:PASSWORD@HOST:25060/defaultdb"
schema = "edgar_schema"

# Prefix maps for auto-detecting latest CSV
non_master_prefixes = {
    "cik_map": "cik_map",
    "dei_facts": "dei_facts",
    "subs_raw": "subs_ex21_ex8_raw",
    "charter_addresses_raw": "charter_addresses_raw"
}

master_prefixes = {
    "address_master": "addresses_master",
    "subs_master": "subs_master",
    "parents_master": "parents_master"
}

# ---------- Create SQLAlchemy engine ----------
engine = create_engine(db_url)

# ---------- Helper: find latest CSV by prefix ----------
def find_latest_csv(folder, prefix):
    pattern = os.path.join(folder, f"{prefix}_*.csv")
    files = glob.glob(pattern)
    if not files:
        print(f"WARNING: No CSV found for prefix '{prefix}' in {folder}")
        return None

    files.sort(reverse=True)  # newest first
    return files[0]

# ---------- Helper: convert date columns ----------
def convert_date_columns(df):
    # Handles formats like '31-Dec-24'
    for col in df.columns:
        if 'date' in col.lower() or 'period_end' in col.lower():
            df[col] = pd.to_datetime(
                df[col],
                format='%d-%b-%y',
                errors='coerce'
            )
    return df

# ---------- Load CSV ----------
def load_csv(csv_path, table_name):
    if csv_path is None:
        return

    df = pd.read_csv(csv_path)

    # Convert any date-like columns automatically
    df = convert_date_columns(df)

    print(f"Loading {len(df)} rows from {os.path.basename(csv_path)} into {table_name}")

    df.to_sql(
        table_name,
        schema=schema,
        con=engine,
        if_exists="append",
        index=False
    )

# ---------- Load non-master CSVs ----------
print("=== Loading non-master CSVs ===")
for table, prefix in non_master_prefixes.items():
    csv_path = find_latest_csv(non_master_dir, prefix)
    load_csv(csv_path, table)

# ---------- Load master CSVs ----------
print("=== Loading master CSVs ===")
for table, prefix in master_prefixes.items():
    csv_path = find_latest_csv(master_dir, prefix)
    load_csv(csv_path, table)

print("=== All CSVs loaded successfully ===")