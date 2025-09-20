import requests
import os
import json
import pandas as pd
from datetime import datetime
import glob

# Config
INPUT_DIR = "data/raw/USCC/"
OUTPUT_DIR = "data/intermediate/"
CACHE_FILE = "data/cache/sec_tickers.json"
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
HEADERS = {
    "User-Agent": "First-Name Last-Name Email" #Enter your information
}

# Load or fetch SEC mapping
def load_sec_mapping():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            tickers_data = json.load(f)
    else:
        res = requests.get(SEC_TICKERS_URL, headers=HEADERS)
        res.raise_for_status()
        tickers_data = res.json()
        os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
        with open(CACHE_FILE, "w") as f:
            json.dump(tickers_data, f)
    return {
        item["ticker"].upper(): str(item["cik_str"]).zfill(10)
        for item in tickers_data.values()
    }

# Load USCC file
def load_uscc_file(path):
    df = pd.read_csv(path)
    df["ticker"] = df["ticker"].str.upper()
    return df

# Resolve mappings
def resolve_mappings(uscc_df, sec_map):
    resolved_rows = []
    missing = []

    for _, row in uscc_df.iterrows():
        ticker = row["ticker"]
        company_name_uscc = row.get("company_name", "")
        resolved_at = datetime.utcnow().isoformat()

        if ticker in sec_map:
            cik10 = sec_map[ticker]
            mapping_source = "SEC_official"
        else:
            cik10 = "PENDING"
            mapping_source = "not_found"
            missing.append(ticker)

        resolved_rows.append({
            "ticker": ticker,
            "cik10": cik10,
            "company_name_uscc": company_name_uscc,
            "mapping_source": mapping_source,
            "resolved_at": resolved_at,
        })

    return pd.DataFrame(resolved_rows), missing

#Process all files
def main():
    sec_map = load_sec_mapping()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    uscc_files = sorted(glob.glob(os.path.join(INPUT_DIR, "*_chinese_companies_USA.csv")))

    for file_path in uscc_files:
        run_date = os.path.basename(file_path).split("_")[0]
        output_file = os.path.join(OUTPUT_DIR, f"cik_map_{run_date}.csv")

        print(f"[INFO] Processing {file_path} → {output_file}")

        uscc_df = load_uscc_file(file_path)
        resolved_df, missing = resolve_mappings(uscc_df, sec_map)

        resolved_df.to_csv(output_file, index=False)

        print(f"[INFO] Wrote {len(resolved_df)} rows, "
              f"{resolved_df['cik10'].ne('PENDING').mean() * 100:.1f}% mapped")
        if missing:
            print(f"[WARN] Missing tickers for {run_date}: {missing}")

if __name__ == "__main__":
    main()