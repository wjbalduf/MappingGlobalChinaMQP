#!/usr/bin/env python3
"""
Parse company HTML files to extract DEI information:
registrant_name, incorp_country, incorp_state_raw, trading_symbol,
filer_category, document_period_end

Output CSV: /data/intermediate/dei_facts_{RUN_DATE}.csv
"""

import unicodedata
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

# Mapping for countries
COUNTRY_MAP = {
    "CN": "China",
    "THE PEOPLE'S REPUBLIC OF CHINA": "China",
    "PEOPLE'S REPUBLIC OF CHINA": "China",
    "US": "United States of America",
    "USA": "United States of America",
    "United States": "United States of America",
    "HK": "Hong Kong",
    "GB": "United Kingdom",
}

# normalize text and fix curly quotes
def clean_text(text):
    if not text:
        return None
    # convert weird unicode to standard forms
    text = unicodedata.normalize("NFKC", text)
    # replace curly quotes with normal quotes
    text = text.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    # replace non-breaking spaces and line breaks with single space
    text = text.replace("\xa0", " ").replace("\n", " ").replace("\r", " ")
    # collapse multiple spaces
    text = re.sub(r"\s+", " ", text).strip()
    return text or None

def extract_field(soup, field_name):
    tag = soup.find(attrs={"name": field_name})
    if not tag:
        return None
    text = " ".join(tag.stripped_strings)
    return clean_text(text)

def extract_incorp_state_raw(soup):
    tags = soup.find_all(attrs={"name": "dei:EntityIncorporationStateCountryCode"})
    if not tags:
        return None
    for tag in reversed(tags):
        text = " ".join(tag.stripped_strings)
        cleaned = clean_text(text)
        if cleaned:
            return cleaned
    return None

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

        incorp_state_raw = extract_incorp_state_raw(soup)

        trading_symbol = extract_field(soup, "dei:TradingSymbol")
        filer_category = extract_field(soup, "dei:EntityFilerCategory")
        document_period_end = extract_field(soup, "dei:DocumentPeriodEndDate")

        if any([registrant_name, incorp_country, incorp_state_raw,
                trading_symbol, filer_category, document_period_end]):
            return registrant_name, incorp_country, incorp_state_raw, trading_symbol, filer_category, document_period_end

    # Fallback if no valid HTML found
    return None, None, None, None, None, None

# Collect results
results = []

for ticker_dir in COMPANIES_DIR.iterdir():
    if not ticker_dir.is_dir():
        continue

    ticker = ticker_dir.name
    registrant_name, incorp_country, incorp_state_raw, trading_symbol, filer_category, document_period_end = parse_company_html(ticker_dir)

    if not any([registrant_name, incorp_country, incorp_state_raw]):
        print(f"No DEI info found for {ticker}, skipping")

    results.append({
        "ticker": ticker,
        "registrant_name": registrant_name,
        "Country_Address": incorp_country,
        "incorp_state_raw": incorp_state_raw,
        "trading_symbol": trading_symbol,
        "filer_category": filer_category,
        "document_period_end": document_period_end
    })

# Write CSV
fieldnames = ["ticker", "registrant_name", "Country_Address", "incorp_state_raw",
              "trading_symbol", "filer_category", "document_period_end"]
with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(results)

print(f"Saved {len(results)} rows to {OUTPUT_FILE}")