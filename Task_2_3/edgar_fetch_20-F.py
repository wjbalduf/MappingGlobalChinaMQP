# Filename: edgar_fetch_20-F.py

import requests
import uuid
import os
import json
import time
from datetime import datetime

# ---------------------------------------------
# Step 1: Gather Tickers and resolve CIKs
# ---------------------------------------------
TICKERS = ["BABA", "BIDU", "TME", "PDD", "NTES"]
CIK_URL = "https://www.sec.gov/files/company_tickers.json"

# Add a proper User-Agent header
headers = {
    "User-Agent": "William Balduf silly12billy@gmail.com"  # Change to your info
}

res = requests.get(CIK_URL, headers=headers)
res.raise_for_status()
tickers_data = res.json()

ticker_to_cik = {item['ticker']: str(item['cik_str']).zfill(10) for item in tickers_data.values()}

for ticker in TICKERS:
    cik = ticker_to_cik.get(ticker)
    if cik:
        print(f"[INFO] Resolved ticker {ticker} → CIK {cik}")
    else:
        print(f"[ERROR] Could not resolve ticker {ticker}")

# ---------------------------------------------
# Step 2: Create company folders
# ---------------------------------------------
for ticker in TICKERS:
    folder = os.path.join("companies", ticker)
    os.makedirs(folder, exist_ok=True)  # creates folder if it doesn't exist

    # Create aggregate & logs folders for step 5
    os.makedirs("aggregate", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    INDEX_FILE = "aggregate/annual_reports_index.json"
    if os.path.exists(INDEX_FILE):
        with open(INDEX_FILE, "r") as f:
            annual_index = json.load(f)
    else:
        annual_index = []

    ERRORS_FILE = "logs/errors.jsonl"
    RUN_SUMMARY_FILE = "logs/run_summary.json"

# ---------------------------------------------
# Step 3: Function to fetch submissions JSON
# ---------------------------------------------
def get_submissions(cik):
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    res = requests.get(url, headers=headers)
    res.raise_for_status()
    return res.json()

# ---------------------------------------------
# Step 4: Function to filter 20-F filings
# ---------------------------------------------
def filter_20f(submissions):
    recent = submissions['filings']['recent']
    filings = []
    for form, accession, date, doc in zip(recent['form'], recent['accessionNumber'], recent['filingDate'], recent['primaryDocument']):
        if form == "20-F":
            filings.append({
                "form": form,
                "accession": accession,
                "date": date,
                "document": doc
            })
    return filings

# ---------------------------------------------
# Step 5: Download filings for each company
# Step 5a: Log filings
# ---------------------------------------------
run_summary = {}

for ticker in TICKERS:
    cik = ticker_to_cik[ticker]
    start_time = time.time()

    submissions = get_submissions(cik)
    filings_20f = filter_20f(submissions)
    folder = os.path.join("companies", ticker)
    os.makedirs(folder, exist_ok=True)


    print(f"[{ticker}] Found {len(filings_20f)} 20-F filings")

    success_count, fail_count = 0, 0

    for idx, f in enumerate(filings_20f, start=1):
        year = f['date'][:4]
        file_uuid = str(uuid.uuid4())
        filename = f"{year}_{ticker}_20-F_{file_uuid}.html"
        filepath = os.path.join(folder, filename)
        url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{f['accession'].replace('-', '')}/{f['document']}"

        # TEST CASE FOR ERRORS! Can comment out for no errors
        # if idx == 1:  # break the first
        #    url = url.replace("https://www.sec.gov", "https://bad.sec.gov")

        try:
            r = requests.get(url, headers=headers)
            r.raise_for_status()
            with open(filepath, "wb") as out:
                out.write(r.content)

            # Aggregate index
            entry = {
                "ticker": ticker,
                "cik": cik,
                "year": year,
                "filing_date": f['date'],
                "accession": f['accession'],
                "url": url,
                "saved_path": filepath,
                "uuid": file_uuid
            }
            annual_index.append(entry)

            print(f"[{ticker}] {idx}/{len(filings_20f)} Saved → {filepath}")
            print(f"[{ticker}] Index updated for {year} 20-F (uuid {file_uuid[:8]}…)")
            success_count += 1

        except Exception as e:
            error_entry = {
                "ticker": ticker,
                "year": year,
                "url": url,
                "error": str(e)
            }
            with open(ERRORS_FILE, "a") as ef:
                ef.write(json.dumps(error_entry) + "\n")
            print(f"[{ticker}] {year} FAILED → {str(e)}")
            fail_count += 1

    # Per-company run summary
    runtime = round(time.time() - start_time, 2)
    run_summary[ticker] = {
        "total_filings": len(filings_20f),
        "successes": success_count,
        "failures": fail_count,
        "runtime_sec": runtime,
        "last_run": datetime.now().isoformat()
    }

    if fail_count == 0:
        print(f"[INFO] Run complete: {ticker} → {success_count} filings ({success_count} ok, 0 failed)")
    else:
        print(f"[INFO] Run complete: {ticker} → {len(filings_20f)} filings "
              f"({success_count} ok, =1{fail_count} failed, see {ERRORS_FILE})")
        
# ---------------------------------------------
# Step 6: Save run summary log + index 
# ---------------------------------------------
with open(INDEX_FILE, "w") as f:
    json.dump(annual_index, f, indent=2)

with open(RUN_SUMMARY_FILE, "w") as f:
    json.dump(run_summary, f, indent=2)
