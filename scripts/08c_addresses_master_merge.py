"""
Usage:
    python scripts/08c_addresses_master_merge.py
"""

import os
import re
import glob
import hashlib
import pandas as pd
from datetime import datetime

# -----------------------------
# CONFIG: find latest subs_ex21_raw CSV
# -----------------------------
RAW_PATTERN = os.path.join("data", "intermediate", "subs_ex21_raw_*.csv")
files = glob.glob(RAW_PATTERN)
if not files:
    raise FileNotFoundError("No files found matching subs_ex21_raw_*.csv in data/intermediate")

def extract_date(f):
    m = re.search(r"(\d{8})(?=\.csv$)", os.path.basename(f))
    return m.group(1) if m else "00000000"

latest_subs_file = max(files, key=lambda f: extract_date(f))
RUN_DATE = extract_date(latest_subs_file)

RAW_FILE = latest_subs_file

# -----------------------------
# CONFIG: find latest charter_addresses_raw CSV
# -----------------------------
ADDR_PATTERN = os.path.join("data", "intermediate", "charter_addresses_raw_*.csv")
addr_files = glob.glob(ADDR_PATTERN)
if not addr_files:
    raise FileNotFoundError("No files found matching charter_addresses_raw_*.csv in data/intermediate")

latest_addr_file = max(addr_files, key=lambda f: extract_date(f))
ADDR_FILE = latest_addr_file

# -----------------------------
# OUTPUT
# -----------------------------
OUTPUT_FILE = os.path.join("data", "clean", f"addresses_master_{RUN_DATE}.csv")
os.makedirs("data/clean", exist_ok=True)

# -----------------------------
# HELPER FUNCTIONS
# -----------------------------
def generate_addr_id(entity_id: str, address_raw: str) -> str:
    to_hash = f"{entity_id}_{address_raw or ''}".encode("utf-8")
    return hashlib.md5(to_hash).hexdigest()

def parse_address(address_raw: str):
    if pd.isna(address_raw):
        return None, None, None, None, None
    return address_raw, None, None, None, None  # addr_line, locality, region, postal_code, country_iso3

# -----------------------------
# MAIN
# -----------------------------
# Load latest subs_ex21_raw CSV
df = pd.read_csv(RAW_FILE)

# Load latest charter_addresses_raw CSV
addr_df = pd.read_csv(ADDR_FILE)

# Keep only most recent exhibit_year per company
addr_df["company_id"] = addr_df.apply(
    lambda x: x["parent_cik10"] if pd.notna(x.get("parent_cik10")) else x["subsidiary_name_raw"], axis=1
)
addr_df = addr_df.sort_values("exhibit_year", ascending=False).drop_duplicates("company_id")

# Merge address_raw into df
df["company_id"] = df.apply(
    lambda x: x["parent_cik10"] if pd.notna(x.get("parent_cik10")) else x["subsidiary_name_raw"], axis=1
)
df = df.merge(
    addr_df[["company_id", "address_raw", "exhibit_year"]],
    on="company_id",
    how="left"
)

# Initialize addresses_master
addresses_master = pd.DataFrame()
addresses_master["entity_type"] = df.apply(
    lambda x: "parent" if pd.notna(x.get("parent_cik10")) else "subsidiary", axis=1
)
addresses_master["entity_id"] = df.apply(
    lambda x: x.get("parent_cik10") if pd.notna(x.get("parent_cik10")) else x.get("subsidiary_name_raw"), axis=1
)
addresses_master["address_raw"] = df["address_raw"]

# Parsed parts
addresses_master[["addr_line","locality","region","postal_code","country_iso3"]] = pd.DataFrame(
    [parse_address(a) for a in addresses_master["address_raw"]], index=addresses_master.index
)

# source_accession and address_type
addresses_master["source_accession"] = df.get("accession", pd.NA)
addresses_master["address_type"] = pd.NA

# parse_confidence
addresses_master["parse_confidence"] = df.get("parse_confidence", pd.NA)

# addr_id: hash of entity_id + address_raw
addresses_master["addr_id"] = addresses_master.apply(
    lambda x: generate_addr_id(str(x["entity_id"]), str(x["address_raw"])), axis=1
)

# Save CSV
addresses_master.to_csv(OUTPUT_FILE, index=False)
print(f"Saved addresses_master CSV to {OUTPUT_FILE}")