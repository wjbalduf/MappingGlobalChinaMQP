"""
Generate Parents Master from DEI CSV only
Uses DEI facts and CIK mapping
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
# Relative paths assuming current working directory is project root
DEI_FILE = os.path.join("data", "intermediate", "dei_facts_20251008.csv")
CIK_FILE = os.path.join("data", "intermediate", "cik_map_20251008.csv")
OUTPUT_DIR = os.path.join("data", "clean")
os.makedirs(OUTPUT_DIR, exist_ok=True)

RUN_DATE = datetime.now().strftime("%Y%m%d")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, f"parents_master_{RUN_DATE}.csv")

# -------------------------------------------------------------
# LOAD DEI FACTS AND CIK MAP
# -------------------------------------------------------------
dei_df = pd.read_csv(DEI_FILE)
cik_df = pd.read_csv(CIK_FILE)

# Ensure ticker columns match
dei_df.rename(columns={"ticker": "parent_ticker"}, inplace=True)
cik_df.rename(columns={"ticker": "parent_ticker", "cik10": "parent_cik10"}, inplace=True)

# Merge DEI with CIK map
merged_df = pd.merge(dei_df, cik_df[["parent_ticker", "parent_cik10"]], 
                     on="parent_ticker", how="left")

# -------------------------------------------------------------
# BUILD RECORDS
# -------------------------------------------------------------
records = []

for _, row in merged_df.iterrows():
    parent_ticker = row.get("parent_ticker")
    parent_cik10 = row.get("parent_cik10")

    parent_name = row.get("registrant_name")
    incorp_country_iso3 = row.get("Country_Address")
    incorp_state_or_region = row.get("incorp_state_raw")
    legal_form = row.get("legal_form")

    sources_used = "DEI"
    lineage = {"dei_path": DEI_FILE, "cik_path": CIK_FILE}

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

cols = [
    "parent_ticker", "parent_cik10", "parent_name", "incorp_country_iso3",
    "incorp_state_or_region", "legal_form", "latest_20f_year",
    "latest_20f_accession", "sources_used", "lineage"
]
df = df.reindex(columns=cols)

df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
print(f"✅ Saved {len(df)} records → {OUTPUT_PATH}")
