import os
import json
import pandas as pd
import math
from datetime import datetime

# -------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------
DEI_FILE = os.path.join("data", "intermediate", "dei_facts_20251008.csv")
CIK_FILE = os.path.join("data", "intermediate", "cik_map_20251008.csv")
EDGAR_DIR = os.path.join("data", "raw", "EDGAR")
OUTPUT_DIR = os.path.join("data", "clean")
os.makedirs(OUTPUT_DIR, exist_ok=True)

RUN_DATE = datetime.now().strftime("%Y%m%d")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, f"parents_master_{RUN_DATE}.csv")

# -------------------------------------------------------------
# LOAD DEI FACTS AND CIK MAP
# -------------------------------------------------------------
dei_df = pd.read_csv(DEI_FILE)
cik_df = pd.read_csv(CIK_FILE)

dei_df.rename(columns={"ticker": "parent_ticker"}, inplace=True)
cik_df.rename(columns={"ticker": "parent_ticker", "cik10": "parent_cik10"}, inplace=True)

merged_df = pd.merge(dei_df, cik_df[["parent_ticker", "parent_cik10"]],
                     on="parent_ticker", how="left")

# -------------------------------------------------------------
# FUNCTION TO GET NAME FROM submissions.json
# -------------------------------------------------------------
def get_name_from_submissions(ticker):
    submissions_path = os.path.join(EDGAR_DIR, ticker, "submissions.json")
    if not os.path.exists(submissions_path):
        return None
    try:
        with open(submissions_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            name = data.get("name")
            if name and name.strip():
                return name.strip()
    except Exception as e:
        print(f"Error reading {submissions_path}: {e}")
    return None

# -------------------------------------------------------------
# LOAD USCC CSV
# -------------------------------------------------------------
USCC_FILE = os.path.join("data", "raw", "USCC", "20251008_chinese_companies_USA.csv")
uscc_df = pd.read_csv(USCC_FILE)
uscc_df.rename(columns={"ticker": "parent_ticker", "company_name": "uscc_name"}, inplace=True)
uscc_lookup = dict(zip(uscc_df["parent_ticker"], uscc_df["uscc_name"]))

# -------------------------------------------------------------
# BUILD PARENTS RECORDS
# -------------------------------------------------------------
records = []

for _, row in merged_df.iterrows():
    parent_ticker = row.get("parent_ticker")
    parent_cik10 = row.get("parent_cik10")
    
    # Helper to check if a value is non-empty
    def has_value(val):
        return val is not None and not (isinstance(val, float) and math.isnan(val)) and str(val).strip() != ""
    
    # 1️⃣ Start with DEI
    parent_name = row.get("registrant_name")
    sources_used = ["DEI"] if has_value(parent_name) else []
    
    # 2️⃣ Fallback to submissions.json
    if not has_value(parent_name):
        parent_name_sub = get_name_from_submissions(parent_ticker)
        if has_value(parent_name_sub):
            parent_name = parent_name_sub
            sources_used.append("submissions")
    
    # 3️⃣ Fallback to USCC
    if not has_value(parent_name):
        parent_name_uscc = uscc_lookup.get(parent_ticker)
        if has_value(parent_name_uscc):
            parent_name = parent_name_uscc
            sources_used.append("USCC")
    
    # If no source was found at all, keep DEI anyway
    if not sources_used:
        sources_used = ["DEI"]
    
    # Other fields
    incorp_country_iso3 = row.get("Country_Address")
    incorp_state_or_region = row.get("incorp_state_raw")
    legal_form = row.get("legal_form")
    lineage = {
        "dei_path": DEI_FILE,
        "cik_path": CIK_FILE,
        "uscc_path": USCC_FILE
    }
    
    latest_20f_year = None
    latest_20f_accession = None
    
    records.append({
        "parent_ticker": parent_ticker,
        "parent_cik10": parent_cik10,
        "parent_name": parent_name,
        "incorp_country_iso3": incorp_country_iso3,
        "incorp_state_or_region": incorp_state_or_region,
        "legal_form": legal_form,
        "latest_20f_year": latest_20f_year,
        "latest_20f_accession": latest_20f_accession,
        "sources_used": "|".join(sources_used),
        "lineage": json.dumps(lineage)
    })

# -------------------------------------------------------------
# SAVE CSV
# -------------------------------------------------------------
df_out = pd.DataFrame(records)
df_out.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
print(f"✅ Saved {len(df_out)} records → {OUTPUT_PATH}")