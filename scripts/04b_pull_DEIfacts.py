#!/usr/bin/env python3
"""
Parse company HTML files to extract DEI information:
registrant_name, incorp_country, incorp_state, trading_symbol,
filer_category, document_period_end

Output CSV: /data/intermediate/dei_facts_{RUN_DATE}.csv
"""

import html
import os
import csv
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import warnings

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
    "US": "United States",
    "USA": "United States",
}

# Helper function to extract text from ix:nonNumeric tag
def extract_field(soup, field_name):
    tag = soup.find(attrs={"name": field_name})
    if not tag:
        return None
    
    # Get all text fragments inside the tag
    text_fragments = list(tag.stripped_strings)
    if not text_fragments:
        return None
    
    # Join fragments with a space, replace internal newlines, and normalize whitespace
    text = " ".join(text_fragments).replace("\xa0", " ").replace("\n", " ").strip()
    
    # Unescape HTML entities (like &rsquo;) and fix encoding issues
    text = html.unescape(text)
    
    # Normalize curly quotes to straight quotes
    text = text.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    
    # Collapse multiple spaces
    text = " ".join(text.split())
    
    return text or None

# Helper function to extract only state/province/region
def extract_state(soup):
    tag = soup.find(attrs={"name": "dei:EntityAddressCityOrTown"})
    if not tag:
        return None

    # Join all text fragments
    raw = " ".join(tag.stripped_strings).replace("\xa0", " ").strip()

    # If it's "City, State" or "District, Province" → take only the last part
    parts = [p.strip() for p in raw.split(",")]
    state = parts[-1] if parts else None

    # Expand known state abbreviations
    if state in STATE_MAP:
        state = STATE_MAP[state]

    return state

# Helper function to iterate all HTMLs for a company
def parse_company_html(ticker_dir):
    html_files = sorted(
        [f for f in ticker_dir.glob("*.html")],
        key=lambda f: f.stat().st_mtime,
        reverse=True  # most recent first
    )

    for html_file in html_files:
        with open(html_file, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f, "lxml")

        registrant_name = extract_field(soup, "dei:EntityRegistrantName")
        incorp_country = extract_field(soup, "dei:EntityAddressCountry")
        incorp_state = extract_state(soup)
        
        # Map countries
        if incorp_country:
            code = incorp_country.strip().upper()
            if code in COUNTRY_MAP:
                incorp_country = COUNTRY_MAP[code]

        trading_symbol = extract_field(soup, "dei:TradingSymbol")
        filer_category = extract_field(soup, "dei:EntityFilerCategory")
        document_period_end = extract_field(soup, "dei:DocumentPeriodEndDate")

        # Return first HTML where at least one field exists
        if any([registrant_name, incorp_country, incorp_state,
                trading_symbol, filer_category, document_period_end]):
            return registrant_name, incorp_country, incorp_state, trading_symbol, filer_category, document_period_end

    # Nothing found in any HTML
    return None, None, None, None, None, None

# Collect all results
results = []

for ticker_dir in COMPANIES_DIR.iterdir():
    if not ticker_dir.is_dir():
        continue

    ticker = ticker_dir.name
    registrant_name, incorp_country, incorp_state, trading_symbol, filer_category, document_period_end = parse_company_html(ticker_dir)

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