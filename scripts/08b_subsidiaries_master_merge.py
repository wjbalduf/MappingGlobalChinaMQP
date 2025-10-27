"""
Usage:
    python scripts/08b_subsidiaries_master_merge.py
"""
import os
import re
import glob
import pandas as pd
from datetime import datetime
import uuid

# -------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------
RAW_PATTERN = os.path.join("data", "intermediate", "subs_ex21_raw_*.csv")
files = glob.glob(RAW_PATTERN)
if not files:
    raise FileNotFoundError("No files found matching subs_ex21_raw_*.csv in data/intermediate")

# Get latest by date in filename (digits after last underscore)
def extract_date(f):
    m = re.search(r"(\d{8})(?=\.csv$)", os.path.basename(f))
    return m.group(1) if m else "00000000"

latest_file = max(files, key=lambda f: extract_date(f))
RUN_DATE = extract_date(latest_file)

RAW_FILE = latest_file
OUTPUT_FILE = os.path.join("data", "clean", f"subs_master_{RUN_DATE}.csv")
os.makedirs("data/clean", exist_ok=True)

# Legal suffixes to keep
LEGAL_SUFFIXES_KEEP = [
    "Inc", "Inc.", "Corp", "Corp.", "Ltd", "Ltd.", "LLC", "LLP", "PLC", "Limited",
    "Co.", "Company", "S.A.", "S.A", "AG", "GmbH", "B.V.", "NV", "Pte.", "KK"
]

# -------------------------------------------------------------
# FUNCTIONS
# -------------------------------------------------------------
def normalize_sub_name(name: str) -> str:
    """Normalize spacing but keep legal suffixes."""
    if pd.isna(name):
        return ""
    name = re.sub(r"\s+", " ", name.strip())
    return name

def normalize_jurisdiction(j: str):
    """Normalize messy jurisdiction names and map to ISO3 country codes."""
    if pd.isna(j) or not j.strip():
        return "", None

    # Clean basic formatting and fix encoding errors
    j = j.strip()
    j = j.replace("â€™", "'")  # Fix encoding artifact
    j = re.sub(r"Jurisdiction.*Organization", "", j, flags=re.I).strip()
    j = re.sub(r"[^A-Za-z' ]", "", j).strip()

    # Normalize common variations
    mapping = {
        # China / PRC
        "PRC": "China",
        "People's Republic of China": "China",
        "Peoples Republic of China": "China",
        "China": "China",
        "Mainland China": "China",
        "Taiwan": "Taiwan",

        # Hong Kong
        "HK": "Hong Kong",
        "Hongkong": "Hong Kong",
        "Hong Kong": "Hong Kong",

        # Macau
        "Macau": "Macau",
        "Macao": "Macau",

        # Singapore
        "SGP": "Singapore",
        "Singapore": "Singapore",

        # BVI
        "BVI": "British Virgin Islands",
        "British Virgin Islands": "British Virgin Islands",

        # Cayman
        "Cayman": "Cayman Islands",
        "Cayman Islands": "Cayman Islands",

        # USA
        "US": "United States",
        "U.S.": "United States",
        "USA": "United States",
        "United States": "United States",
        "The United States":"United States",
        "California": "United States",
        "Delaware": "United States",
        "Delware": "United States",
        "Nevada": "United States",
        "Missouri": "United States",
        "Kansas": "United States",

        # UK
        "UK": "United Kingdom",
        "United Kingdom": "United Kingdom",

        # India
        "India": "India",

        # Japan
        "Japan": "Japan",

        # Malaysia
        "Malaysia": "Malaysia",

        # Australia
        "Australia": "Australia",

        # Dubai / UAE
        "Dubai": "United Arab Emirates",
        "UAE": "United Arab Emirates",
    }

    j_norm = mapping.get(j, j)

    # ISO3 lookup
    JURIS_ISO3_MAP = {
        "China": "CHN",
        "Hong Kong": "HKG",
        "Macau": "MAC",
        "Singapore": "SGP",
        "British Virgin Islands": "VGB",
        "Cayman Islands": "CYM",
        "United States": "USA",
        "United Kingdom": "GBR",
        "India": "IND",
        "Japan": "JPN",
        "Malaysia": "MYS",
        "Australia": "AUS",
        "United Arab Emirates": "ARE",
        "Taiwan": "TWN",
        "Philippines": "PHL",
        "Canada": "CAN",
        "Belgium": "BEL",
    }

    iso3 = JURIS_ISO3_MAP.get(j_norm, None)
    return j_norm, iso3


def extract_ownership_from_text(text: str):
    """Extract ownership percentage (numeric) from text if present."""
    if pd.isna(text):
        return None
    text = str(text).lower()

    # Match patterns like '100%', '84.32%', '51 percent', '55% owned', etc.
    match = re.search(r"(\d+(?:\.\d+)?)\s*%|\b(\d+(?:\.\d+)?)\s*percent\b", text)
    if match:
        return float(match.group(1) or match.group(2))

    # Handle non-numeric terms like 'wholly-owned'
    if "wholly" in text or "wholly-owned" in text:
        return 100.0

    return None


def make_uuid(parent_cik10, sub_name_norm, jurisdiction_norm):
    """UUID5 based on parent_cik10 + normalized_sub_name + jurisdiction_norm."""
    base_str = f"{parent_cik10}|{sub_name_norm}|{jurisdiction_norm or ''}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, base_str))

# -------------------------------------------------------------
# MAIN
# -------------------------------------------------------------
print(f"Reading {RAW_FILE}...")
df = pd.read_csv(RAW_FILE)

# Normalize and derive fields
df["subsidiary_name"] = df["subsidiary_name_raw"].apply(normalize_sub_name)
df[["jurisdiction_norm", "jurisdiction_iso3"]] = df["jurisdiction_raw"].apply(
    lambda x: pd.Series(normalize_jurisdiction(x))
)
df["ownership_pct"] = df["ownership_raw"].apply(extract_ownership_from_text)

# Derive sub_uuid
df["sub_uuid"] = df.apply(
    lambda r: make_uuid(r["parent_cik10"], r["subsidiary_name"], r["jurisdiction_norm"]),
    axis=1,
)

# Parse confidence placeholder
df["parse_confidence"] = 1.0  # you can later replace with NLP-derived confidence

# Lineage info
df["lineage"] = df["source_path"]

# First/last seen year
df["first_seen_year"] = df["exhibit_year"]
df["last_seen_year"] = df["exhibit_year"]

# Deduplicate (parent_cik10, sub_uuid)
df = df.drop_duplicates(subset=["parent_cik10", "sub_uuid"])

# Select & reorder columns
final_cols = [
    "sub_uuid",
    "parent_ticker",
    "parent_cik10",
    "subsidiary_name",
    "jurisdiction_iso3",
    "ownership_pct",
    "first_seen_year",
    "last_seen_year",
    "accession",
    "exhibit_label",
    "parse_confidence",
    "lineage",
]
df_final = df[final_cols]

# Save
df_final.to_csv(OUTPUT_FILE, index=False)
print(f"Subs master written to {OUTPUT_FILE}")