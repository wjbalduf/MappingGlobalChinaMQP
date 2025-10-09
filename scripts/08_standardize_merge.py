"""
Generate Parents Master from DEI CSV only
Outputs CSV with columns:
parent_ticker,parent_cik10,parent_name,incorp_country_iso3,
incorp_state_or_region,legal_form,latest_20f_year,latest_20f_accession,
sources_used,lineage
"""

import os
import json
import pandas as pd
from datetime import datetime

# -------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------
BASE_DIR = r"C:\Users\silly\OneDrive\School\2025_Fall\MQP\MappingGlobalChinaMQP"
DEI_FILE = os.path.join(BASE_DIR, "data", "intermediate", "dei_facts_20251008.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "clean")
os.makedirs(OUTPUT_DIR, exist_ok=True)

RUN_DATE = datetime.now().strftime("%Y%m%d")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, f"parents_master_{RUN_DATE}.csv")

# -------------------------------------------------------------
# LOAD DEI FACTS
# -------------------------------------------------------------
dei_df = pd.read_csv(DEI_FILE)

# -------------------------------------------------------------
# BUILD RECORDS
# -------------------------------------------------------------
records = []

for _, row in dei_df.iterrows():
    parent_ticker = row.get("ticker")
    parent_cik10 = None  # If you have CIK in DEI, replace None with row.get("cik10")

    parent_name = row.get("registrant_name")
    incorp_country_iso3 = row.get("Country_Address")
    incorp_state_or_region = row.get("incorp_state_raw")
    legal_form = row.get("legal_form")

    # Since we're using DEI only, sources_used is "DEI"
    sources_used = "DEI"

    # Lineage JSON points to DEI CSV
    lineage = {
        "dei_path": DEI_FILE
    }

    # Only include rows with a country
    if incorp_country_iso3:
        records.append({
            "parent_ticker": parent_ticker,
            "parent_cik10": parent_cik10,
            "parent_name": parent_name,
            "incorp_country_iso3": incorp_country_iso3,
            "incorp_state_or_region": incorp_state_or_region,
            "legal_form": legal_form,
            "latest_20f_year": None,
            "latest_20f_accession": None,
            "sources_used": sources_used,
            "lineage": json.dumps(lineage)
        })

# -------------------------------------------------------------
# SAVE OUTPUT
# -------------------------------------------------------------
df = pd.DataFrame(records)

# Reorder columns exactly as requested
cols = [
    "parent_ticker", "parent_cik10", "parent_name", "incorp_country_iso3",
    "incorp_state_or_region", "legal_form", "latest_20f_year",
    "latest_20f_accession", "sources_used", "lineage"
]
df = df.reindex(columns=cols)

df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
print(f"✅ Saved {len(df)} records → {OUTPUT_PATH}")
