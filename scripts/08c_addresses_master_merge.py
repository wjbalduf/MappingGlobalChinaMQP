"""
Usage:
    python scripts/08c_addresses_master_merge.py
"""
import os
import glob
import pandas as pd
import hashlib
import re

# -----------------------------
# HELPER FUNCTIONS
# -----------------------------
def extract_date(f):
    m = re.search(r"(\d{8})(?=\.csv$)", os.path.basename(f))
    return m.group(1) if m else "00000000"

def generate_addr_id(entity_id: str, address_raw: str) -> str:
    to_hash = f"{entity_id}_{address_raw or ''}".encode("utf-8")
    return hashlib.md5(to_hash).hexdigest()

def parse_address(address_raw: str):
    """
    Parse raw_address into addr_line, locality, region, postal_code.
    Uses simple heuristic: split by commas.
    """
    if not address_raw or pd.isna(address_raw):
        return address_raw, None, None, None, None

    parts = [p.strip() for p in address_raw.split(",") if p.strip()]

    addr_line = None
    locality = None
    region = None
    postal_code = None

    # Assign parts from the end backwards
    if parts:
        last_part = parts[-1]
        postal_match = re.search(r"\b\d{4,6}\b", last_part)
        if postal_match:
            postal_code = postal_match.group(0)

        if len(parts) >= 2:
            region = parts[-2]
        if len(parts) >= 3:
            locality = parts[-3]
        if len(parts) >= 4:
            addr_line = ", ".join(parts[:-3])
        elif len(parts) == 3:
            addr_line = parts[0]
        elif len(parts) == 2:
            addr_line = parts[0]

    return address_raw, addr_line, locality, region, postal_code

def normalize_cik(val):
    """Normalize CIK into 10-digit string or 'n/a'."""
    if pd.isna(val):
        return "n/a"
    s = str(val).strip()
    if s.isdigit():
        return s.zfill(10)
    return "n/a"

# -----------------------------
# 1. LOAD SUBS_MASTER CSV
# -----------------------------
subs_files = glob.glob(os.path.join("data", "clean", "subs_master_*.csv"))
if not subs_files:
    raise FileNotFoundError("No subs_master_*.csv found")
latest_subs_file = max(subs_files, key=extract_date)
RUN_DATE = extract_date(latest_subs_file)
subs_df = pd.read_csv(latest_subs_file)

subs_df["address_raw"] = pd.NA
subs_df["entity_type"] = "subsidiary"
subs_df["entity_id"] = subs_df["sub_uuid"]

# Fill country_iso3 for subsidiaries from jurisdiction_iso3
if "jurisdiction_iso3" in subs_df.columns:
    subs_df["country_iso3"] = subs_df["jurisdiction_iso3"]
else:
    subs_df["country_iso3"] = pd.NA

# -----------------------------
# 2. LOAD PARENTS_MASTER CSV
# -----------------------------
parents_file = os.path.join("data", "clean", f"parents_master_{RUN_DATE}.csv")
if not os.path.exists(parents_file):
    raise FileNotFoundError(f"No parents_master_{RUN_DATE}.csv found")
parents_df = pd.read_csv(parents_file)

parents_df["entity_type"] = "parent"
parents_df["entity_id"] = parents_df["parent_cik10"]
parents_df["parent_cik10"] = parents_df["parent_cik10"].apply(normalize_cik)

# -----------------------------
# 3. LOAD ADDRESSES CSV
# -----------------------------
addr_files = glob.glob(os.path.join("data", "intermediate", "charter_addresses_raw_*.csv"))
if not addr_files:
    raise FileNotFoundError("No charter_addresses_raw_*.csv found")
latest_addr_file = max(addr_files, key=extract_date)
addr_df = pd.read_csv(latest_addr_file)
addr_df["parent_cik10"] = addr_df["parent_cik10"].apply(normalize_cik)

# -----------------------------
# 4. MERGE ADDRESSES INTO PARENTS
# -----------------------------
# Merge both address_raw and address_type
parents_df = parents_df.merge(
    addr_df[["parent_cik10", "address_raw", "address_type"]],
    on="parent_cik10",
    how="left"
)

# -----------------------------
# 5. ADD COUNTRY ISO3 FOR PARENTS
# -----------------------------
if "incorp_country_iso3" in parents_df.columns:
    parents_df = parents_df.rename(columns={"incorp_country_iso3": "country_iso3"})
else:
    parents_df["country_iso3"] = pd.NA

# -----------------------------
# 6. COMBINE SUBS AND PARENTS
# -----------------------------
addresses_master = pd.concat([subs_df, parents_df], ignore_index=True, sort=False)

# -----------------------------
# 7. PARSE ADDRESSES
# -----------------------------
parsed_cols = ["address_raw", "addr_line", "locality", "region", "postal_code"]
parsed_addresses = [parse_address(a) for a in addresses_master["address_raw"]]
addresses_master[parsed_cols] = pd.DataFrame(parsed_addresses, index=addresses_master.index)

# -----------------------------
# 8. ADD ADDITIONAL COLUMNS
# -----------------------------
# Subsidiaries: source_accession from 'accession'
addresses_master["source_accession"] = addresses_master.get("accession", pd.NA)

# Parents: overwrite with 'latest_20f_accession' if exists
if "latest_20f_accession" in addresses_master.columns:
    parent_mask = addresses_master["entity_type"] == "parent"
    addresses_master.loc[parent_mask, "source_accession"] = addresses_master.loc[parent_mask, "latest_20f_accession"]

# For any parents without address_type, keep NA (already merged above)
addresses_master["parse_confidence"] = addresses_master.get("parse_confidence", pd.NA)
addresses_master["addr_id"] = addresses_master.apply(
    lambda x: generate_addr_id(str(x["entity_id"]), str(x["address_raw"])),
    axis=1
)

# -----------------------------
# 9. DROP UNNECESSARY COLUMNS
# -----------------------------
addresses_master = addresses_master.drop(columns=[
    "sub_uuid", "parent_cik10", "subsidiary_name", "ownership_pct",
    "first_seen_year", "last_seen_year","jurisdiction_iso3","accession",
    "exhibit_label","lineage","parent_ticker","latest_20f_accession","parent_name",
    "incorp_state_or_region","legal_form","latest_20f_year","sources_used"
], errors="ignore")

# -----------------------------
# 10. REORDER COLUMNS
# -----------------------------
front_cols = ["entity_type", "entity_id", "address_raw", "addr_line", "locality", "region",
              "postal_code", "country_iso3", "source_accession", "address_type"]
other_cols = [c for c in addresses_master.columns if c not in front_cols]
addresses_master = addresses_master[front_cols + other_cols]

# -----------------------------
# 11. SAVE CSV
# -----------------------------
OUTPUT_FILE = os.path.join("data", "clean", f"addresses_master_{RUN_DATE}.csv")
os.makedirs("data/clean", exist_ok=True)
addresses_master.to_csv(OUTPUT_FILE, index=False)
print(f"Saved addresses_master CSV to {OUTPUT_FILE}")