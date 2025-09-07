# Filename: edgar_fetch_20-F.py

import requests
import uuid
import os

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
# ---------------------------------------------
for ticker in TICKERS:
    cik = ticker_to_cik[ticker]
    submissions = get_submissions(cik)
    filings_20f = filter_20f(submissions)
    folder = os.path.join("companies", ticker)

    print(f"[{ticker}] Found {len(filings_20f)} 20-F filings")

    for idx, f in enumerate(filings_20f, start=1):
        year = f['date'][:4]
        file_uuid = uuid.uuid4()
        filename = f"{year}_{ticker}_20-F_{file_uuid}.html"
        filepath = os.path.join(folder, filename)
        url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{f['accession'].replace('-', '')}/{f['document']}"

        try:
            r = requests.get(url, headers=headers)
            r.raise_for_status()
            with open(filepath, "wb") as out:
                out.write(r.content)
            print(f"[{ticker}] {idx}/{len(filings_20f)} Saved → {filepath}")
        except Exception as e:
            print(f"[{ticker}] {year} FAILED → {str(e)}")
