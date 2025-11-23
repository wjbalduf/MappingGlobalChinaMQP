import pandas as pd
import requests
import os
import time
from bs4 import BeautifulSoup
from datetime import datetime

# -----------------------------
# CONFIGURATION
# -----------------------------
CSV_FILE = "data/intermediate/cik_map_20251120.csv"  # input CSV

RUN_DATE = datetime.now().strftime("%Y%m%d")
OUTPUT_FILE = f"data/intermediate/charter_addresses_raw_{RUN_DATE}.csv"

HEADERS = {"User-Agent": "William Balduf silly12billy@gmail.com"}
RATE_LIMIT = 0.2  # seconds between SEC requests

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

# -----------------------------
# READ INPUT CSV
# -----------------------------
cik_df = pd.read_csv(CSV_FILE)
print(f"Found {len(cik_df)} companies in CSV.")

# -----------------------------
# FUNCTION: Get most recent 20-F accession and filing year
# -----------------------------
def get_most_recent_20f_accession(cik10):
    try:
        cik_int = int(cik10)
    except ValueError:
        return None, None

    url = f"https://data.sec.gov/submissions/CIK{cik_int:010d}.json"
    r = requests.get(url, headers=HEADERS)
    if r.status_code != 200:
        return None, None

    data = r.json()
    forms = data.get("filings", {}).get("recent", {}).get("form", [])
    accessions = data.get("filings", {}).get("recent", {}).get("accessionNumber", [])
    filing_dates = data.get("filings", {}).get("recent", {}).get("reportDate", [])

    for i, form in enumerate(forms):
        if form.upper() == "20-F":
            accession = accessions[i]
            filing_date = filing_dates[i] if i < len(filing_dates) else None
            year = None
            if filing_date:
                try:
                    year = datetime.strptime(filing_date, "%Y-%m-%d").year
                except:
                    pass
            return accession, year

    return None, None

# -----------------------------
# FUNCTION: Scrape Business Address from <div class="mailer">
# -----------------------------
def get_business_address(cik10, accession):
    url = f"https://www.sec.gov/cgi-bin/viewer?action=view&cik={cik10}&accession_number={accession}&xbrl_type=r"
    r = requests.get(url, headers=HEADERS)
    if r.status_code != 200:
        return None, None, "Low", url

    soup = BeautifulSoup(r.content, "lxml")

    # SEC uses <div class="mailer"> for addresses
    mailer_divs = soup.find_all("div", class_="mailer")

    for div in mailer_divs:
        text = div.get_text(" ", strip=True).lower()

        # Only process the BUSINESS ADDRESS block
        if "business address" not in text:
            continue

        spans = div.find_all("span", class_="mailerAddress")
        address_lines = []
        phone_note = None

        for sp in spans:
            raw = sp.get_text(strip=True)
            if not raw:
                continue

            # phone number detection
            if any(c in raw for c in ["+", "-", "(", ")"]) and any(d.isdigit() for d in raw):
                phone_note = raw
            else:
                address_lines.append(raw)

        if address_lines:
            address_raw = ", ".join(address_lines)
            return address_raw, phone_note, "High", url

    return None, None, "Low", url

# -----------------------------
# MAIN LOOP
# -----------------------------
results = []

for idx, row in cik_df.iterrows():
    cik10 = str(row['cik10']).strip()
    parent_ticker = row.get("ticker", "")
    company_name = str(row.get("company_name", "unknown"))

    # Skip non-numeric CIKs (e.g. PENDING)
    if not cik10.isdigit():
        print(f"Skipping non-numeric CIK: {cik10}")
        results.append({
            "parent_ticker": parent_ticker,
            "parent_cik10": cik10,
            "accession": "",
            "exhibit_year": "",
            "address_raw": "",
            "address_type": "other",
            "source_path": "",
            "parse_confidence": "Low",
            "address_note": ""
        })
        continue

    accession, exhibit_year = get_most_recent_20f_accession(cik10)

    if accession:
        address_raw, address_note, parse_confidence, source_path = get_business_address(cik10, accession)
        address_type = "principal_office" if address_raw else "other"
    else:
        accession = ""
        exhibit_year = ""
        address_raw = ""
        address_note = ""
        parse_confidence = "Low"
        source_path = f"https://www.sec.gov/cgi-bin/browse-edgar?CIK={cik10}&action=filing"
        address_type = "other"

    results.append({
        "parent_ticker": parent_ticker,
        "parent_cik10": cik10,
        "accession": accession,
        "exhibit_year": exhibit_year,
        "address_raw": address_raw,
        "address_type": address_type,
        "source_path": source_path,
        "parse_confidence": parse_confidence,
        "address_note": address_note
    })

    time.sleep(RATE_LIMIT)

# -----------------------------
# SAVE TO CSV
# -----------------------------
df_results = pd.DataFrame(results)
df_results.to_csv(OUTPUT_FILE, index=False)

print(f"Saved structured business addresses to {OUTPUT_FILE}")