#!/usr/bin/env python3
"""
Parse company HTML files to extract DEI information:
registrant_name, incorp_country, incorp_state, trading_symbol,
filer_category, document_period_end

Output CSV: /data/intermediate/dei_facts_{RUN_DATE}.csv
Also prints raw state values that resulted in null entries.
"""

import html
import os
import csv
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import warnings
import re

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# Paths
COMPANIES_DIR = Path("companies")
OUTPUT_DIR = Path("data/intermediate")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

RUN_DATE = datetime.now().strftime("%Y%m%d")
OUTPUT_FILE = OUTPUT_DIR / f"dei_facts_{RUN_DATE}.csv"

# Mapping for states
STATE_MAP = {
    "FJ": "Fujian"
}

# Mapping for countries
COUNTRY_MAP = {
    "CN": "China",
    "THE PEOPLE'S REPUBLIC OF CHINA": "China",
    "PEOPLE'S REPUBLIC OF CHINA": "China",
    "US": "United States of America",
    "USA": "United States of America",
    "United States": "United States of America",
}

# Known Chinese provinces / municipalities / regions
CHINA_PROVINCES = {
    "Anhui", "Fujian", "Gansu", "Guangdong", "Guizhou", "Hainan", "Hebei",
    "Heilongjiang", "Henan", "Hubei", "Hunan", "Jiangsu", "Jiangxi", "Jilin",
    "Liaoning", "Qinghai", "Shaanxi", "Shandong", "Shanxi", "Sichuan", "Yunnan",
    "Zhejiang", "Beijing", "Shanghai", "Tianjin", "Chongqing", "Guangxi", "Ningxia",
    "Tibet", "Xinjiang", "Inner Mongolia", "Hong Kong", "Macau"
}

# Common suffixes to remove for China
SUFFIXES = [" Province"]

# Helper function to extract text from ix:nonNumeric tag
def extract_field(soup, field_name):
    tag = soup.find(attrs={"name": field_name})
    if not tag:
        return None
    
    text_fragments = list(tag.stripped_strings)
    if not text_fragments:
        return None
    
    text = " ".join(text_fragments).replace("\xa0", " ").replace("\n", " ").strip()
    text = html.unescape(text)
    text = text.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    text = " ".join(text.split())
    
    return text or None

# Extract only state/province/region
def extract_state(soup, country):
    tag = soup.find(attrs={"name": "dei:EntityAddressCityOrTown"})
    if not tag:
        return None, None

    raw = " ".join(tag.stripped_strings).replace("\xa0", " ").strip()
    state = None

    parts = [p.strip() for p in raw.split(",")]
    if parts:
        candidate = parts[-1]
        if country == "China":
            # Remove suffixes for China
            for suf in SUFFIXES:
                if candidate.endswith(suf):
                    candidate = candidate[:-len(suf)].strip()
            if candidate in CHINA_PROVINCES:
                state = candidate
        else:
            # For all other countries, just take the last part
            state = candidate

    if state in STATE_MAP:
        state = STATE_MAP[state]

    return state, raw

# Extract year from filename
def get_year_from_filename(file_path):
    match = re.match(r"(\d{4})_", file_path.name)
    if match:
        return int(match.group(1))
    return 0

# Parse HTML files for one company
def parse_company_html(ticker_dir):
    html_files = sorted(
        [f for f in ticker_dir.glob("*.html")],
        key=get_year_from_filename,
        reverse=True
    )

    for html_file in html_files:
        with open(html_file, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f, "lxml")

        registrant_name = extract_field(soup, "dei:EntityRegistrantName")
        incorp_country = extract_field(soup, "dei:EntityAddressCountry")
        
        if incorp_country:
            code = incorp_country.strip().upper()
            if code in COUNTRY_MAP:
                incorp_country = COUNTRY_MAP[code]

        incorp_state, raw_state = extract_state(soup, incorp_country)

        trading_symbol = extract_field(soup, "dei:TradingSymbol")
        filer_category = extract_field(soup, "dei:EntityFilerCategory")
        document_period_end = extract_field(soup, "dei:DocumentPeriodEndDate")

        if any([registrant_name, incorp_country, incorp_state,
                trading_symbol, filer_category, document_period_end]):
            return registrant_name, incorp_country, incorp_state, trading_symbol, filer_category, document_period_end, raw_state

    return None, None, None, None, None, None, None

# Collect results
results = []
null_states = set()

for ticker_dir in COMPANIES_DIR.iterdir():
    if not ticker_dir.is_dir():
        continue

    ticker = ticker_dir.name
    registrant_name, incorp_country, incorp_state, trading_symbol, filer_category, document_period_end, raw_state = parse_company_html(ticker_dir)

    if incorp_state is None and raw_state:
        null_states.add(raw_state)

    if not any([registrant_name, incorp_country, incorp_state]):
        print(f"No DEI info found for {ticker}, skipping")

    results.append({
        "ticker": ticker,
        "registrant_name": registrant_name,
        "incorp_country": incorp_country,
        "incorp_state": incorp_state,
        "trading_symbol": trading_symbol,
        "filer_category": filer_category,
        "document_period_end": document_period_end
    })

# Write CSV
fieldnames = ["ticker", "registrant_name", "incorp_country", "incorp_state",
              "trading_symbol", "filer_category", "document_period_end"]
with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(results)

print(f"Saved {len(results)} rows to {OUTPUT_FILE}")

# Print all raw states that ended up null
if null_states:
    print("\n=== Raw state values that resulted in null ===")
    for s in sorted(null_states):
        print(repr(s))