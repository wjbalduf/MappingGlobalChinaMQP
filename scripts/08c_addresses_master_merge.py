"""
Usage:
    python scripts/08c_addresses_master_merge.py
"""
import os
import glob
import pandas as pd
import hashlib

# -----------------------------
# HELPER FUNCTIONS
# -----------------------------
def extract_date(f):
    import re
    m = re.search(r"(\d{8})(?=\.csv$)", os.path.basename(f))
    return m.group(1) if m else "00000000"

def generate_addr_id(entity_id: str, address_raw: str) -> str:
    to_hash = f"{entity_id}_{address_raw or ''}".encode("utf-8")
    return hashlib.md5(to_hash).hexdigest()

def parse_address(address_raw: str):
    if pd.isna(address_raw):
        return None, None, None, None, None
    return address_raw, None, None, None, None

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

# -----------------------------
# 2. LOAD PARENTS_MASTER CSV
# -----------------------------
parents_file = os.path.join("data", "clean", f"parents_master_{RUN_DATE}.csv")
if not os.path.exists(parents_file):
    raise FileNotFoundError(f"No parents_master_{RUN_DATE}.csv found")
parents_df = pd.read_csv(parents_file)

parents_df["entity_type"] = "parent"
parents_df["entity_id"] = parents_df["parent_cik10"]

# -----------------------------
# 3. LOAD ADDRESSES CSV
# -----------------------------
addr_files = glob.glob(os.path.join("data", "intermediate", "charter_addresses_raw_*.csv"))
if not addr_files:
    raise FileNotFoundError("No charter_addresses_raw_*.csv found")
latest_addr_file = max(addr_files, key=extract_date)
addr_df = pd.read_csv(latest_addr_file)

# Merge addresses for parents only (subs will have NA)
parents_df = parents_df.merge(
    addr_df[["parent_cik10", "address_raw"]],
    left_on="parent_cik10",
    right_on="parent_cik10",
    how="left"
)

# -----------------------------
# 4. COMBINE SUBS AND PARENTS
# -----------------------------
addresses_master = pd.concat([subs_df, parents_df], ignore_index=True, sort=False)

# -----------------------------
# 5. PARSE ADDRESSES
# -----------------------------
addresses_master[["addr_line","locality","region","postal_code","country_iso3"]] = pd.DataFrame(
    [parse_address(a) for a in addresses_master["address_raw"]],
    index=addresses_master.index
)

# -----------------------------
# 6. ADDITIONAL COLUMNS
# -----------------------------
addresses_master["source_accession"] = addresses_master.get("accession", pd.NA)
addresses_master["address_type"] = pd.NA
addresses_master["parse_confidence"] = addresses_master.get("parse_confidence", pd.NA)
addresses_master["addr_id"] = addresses_master.apply(
    lambda x: generate_addr_id(str(x["entity_id"]), str(x["address_raw"])), axis=1
)

# -----------------------------
# 7. DROP UNNECESSARY COLUMNS
# -----------------------------
addresses_master = addresses_master.drop(columns=[
    "sub_uuid", "parent_cik10", "parent_ticker"
], errors="ignore")  # ignore if not present

# -----------------------------
# 8. REORDER COLUMNS
# -----------------------------
cols = addresses_master.columns.tolist()
# Move entity_type and entity_id to the front
cols = ["entity_type", "entity_id"] + [c for c in cols if c not in ("entity_type", "entity_id")]
addresses_master = addresses_master[cols]

# -----------------------------
# 9. SAVE CSV
# -----------------------------
OUTPUT_FILE = os.path.join("data", "clean", f"addresses_master_{RUN_DATE}.csv")
os.makedirs("data/clean", exist_ok=True)
addresses_master.to_csv(OUTPUT_FILE, index=False)
print(f"Saved addresses_master CSV to {OUTPUT_FILE}")