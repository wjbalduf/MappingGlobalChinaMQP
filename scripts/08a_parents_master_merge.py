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
EX21_FILE = os.path.join("data", "intermediate", "subs_ex21_raw_20251008.csv")
EDGAR_DIR = os.path.join("data", "raw", "EDGAR")
USCC_FILE = os.path.join("data", "raw", "USCC", "20251008_chinese_companies_USA.csv")
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

merged_df = pd.merge(
    dei_df,
    cik_df[["parent_ticker", "parent_cik10"]],
    on="parent_ticker",
    how="left"
)

# -------------------------------------------------------------
# LOAD USCC CSV
# -------------------------------------------------------------
uscc_df = pd.read_csv(USCC_FILE)
uscc_df.rename(columns={"ticker": "parent_ticker", "company_name": "uscc_name"}, inplace=True)
uscc_lookup = dict(zip(uscc_df["parent_ticker"], uscc_df["uscc_name"]))

# -------------------------------------------------------------
# LOAD EXHIBIT 21 (subsidiary data)
# -------------------------------------------------------------
if os.path.exists(EX21_FILE):
    ex21_df = pd.read_csv(EX21_FILE)
    ex21_df["parent_ticker"] = ex21_df["parent_ticker"].astype(str).str.strip()
    ex21_df["parent_cik10"] = ex21_df["parent_cik10"].astype(str).str.strip()
else:
    ex21_df = pd.DataFrame()

# -------------------------------------------------------------
# FUNCTIONS
# -------------------------------------------------------------
def has_value(val):
    return val is not None and not (isinstance(val, float) and math.isnan(val)) and str(val).strip() != ""

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

def get_state_from_submissions(ticker):
    """Pull stateOfIncorporationDescription from submissions.json"""
    submissions_path = os.path.join(EDGAR_DIR, ticker, "submissions.json")
    if not os.path.exists(submissions_path):
        return None
    try:
        with open(submissions_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            desc = data.get("stateOfIncorporationDescription")
            if desc and desc.strip():
                return desc.strip()
    except Exception as e:
        print(f"Error reading {submissions_path}: {e}")
    return None

# -------------------------------------------------------------
# BUILD PARENTS RECORDS
# -------------------------------------------------------------
records = []

for _, row in merged_df.iterrows():
    parent_ticker = row.get("parent_ticker")
    parent_cik10 = row.get("parent_cik10")

    parent_name = row.get("registrant_name")
    sources_used = []
    lineage = {}

    # -----------------------------
    # Name source
    # -----------------------------
    if has_value(parent_name):
        sources_used.append("DEI")
        lineage["dei_path"] = DEI_FILE
    else:
        parent_name_sub = get_name_from_submissions(parent_ticker)
        if has_value(parent_name_sub):
            parent_name = parent_name_sub
            sources_used.append("submissions")
            lineage["submissions_path"] = os.path.join(EDGAR_DIR, parent_ticker, "submissions.json")
        elif has_value(uscc_lookup.get(parent_ticker)):
            parent_name = uscc_lookup.get(parent_ticker)
            sources_used.append("USCC")
            lineage["uscc_path"] = USCC_FILE

    # -----------------------------
    # Ticker / CIK sources
    # -----------------------------
    if has_value(parent_ticker) and "DEI" not in sources_used:
        sources_used.append("DEI")
        lineage["dei_path"] = DEI_FILE

    if has_value(parent_cik10) and "CIK" not in sources_used:
        sources_used.append("CIK")
        lineage["cik_path"] = CIK_FILE

    if not has_value(parent_ticker) and parent_ticker in uscc_lookup and "USCC" not in sources_used:
        sources_used.append("USCC")
        lineage["uscc_path"] = USCC_FILE

    if not has_value(parent_ticker) and not has_value(parent_cik10):
        submissions_path = os.path.join(EDGAR_DIR, parent_ticker, "submissions.json")
        if os.path.exists(submissions_path) and "submissions" not in sources_used:
            sources_used.append("submissions")
            lineage["submissions_path"] = submissions_path

    # -----------------------------
    # Other fields
    # -----------------------------
    incorp_country_iso3 = ''
    incorp_state_or_region = row.get("incorp_state_raw")
    legal_form = row.get("legal_form")
    latest_20f_year = None
    latest_20f_accession = None

    # -------------------------------------------------------------
    # Fill incorporation state from submissions if missing
    # -------------------------------------------------------------
    if not has_value(incorp_state_or_region):
        sub_state = get_state_from_submissions(parent_ticker)
        if has_value(sub_state):
            incorp_state_or_region = sub_state
            if "submissions" not in sources_used:
                sources_used.append("submissions")
            lineage["submissions_path"] = os.path.join(EDGAR_DIR, parent_ticker, "submissions.json")

    # -------------------------------------------------------------
    # Pull latest 20-F year and accession from EX-21 index
    # -------------------------------------------------------------
    if not ex21_df.empty:
        ex_rows = ex21_df[
            (ex21_df["parent_ticker"].astype(str) == str(parent_ticker)) |
            (ex21_df["parent_cik10"].astype(str) == str(parent_cik10))
        ]
        if not ex_rows.empty:
            latest_row = ex_rows.sort_values("exhibit_year", ascending=False).iloc[0]
            latest_20f_year = latest_row.get("exhibit_year")
            latest_20f_accession = latest_row.get("accession")

            if has_value(latest_20f_year) or has_value(latest_20f_accession):
                if "EX-21" not in sources_used:
                    sources_used.append("EX-21")
                lineage["ex21_path"] = latest_row.get("source_path")

    # -----------------------------
    # Append record
    # -----------------------------
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