#!/usr/bin/env python3
"""
Pull SEC company facts JSON and save raw files.
"""

import json
import csv
import time
import logging
from datetime import datetime
from pathlib import Path
import requests
from typing import Dict, Optional, List

# Constants
BASE_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{}.json"
USER_AGENT = "Mozilla/5.0 (Research Academic Contact: research@university.edu)"
RATE_LIMIT_DELAY = 0.15
MAX_RETRIES = 3
RETRY_DELAY = 5

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('logs/04_pull_companyfacts.log'),
              logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def setup_directories() -> Dict[str, Path]:
    dirs = {
        'raw': Path('data/raw/EDGAR'),
        'logs': Path('logs')
    }
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    return dirs

def load_cik_map(run_date: str) -> List[Dict[str, str]]:
    file_path = Path(f'data/intermediate/cik_map_{run_date}.csv')
    if not file_path.exists():
        raise FileNotFoundError(f"CIK map not found: {file_path}")

    companies = []
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('cik10'):
                companies.append(row)
    logger.info(f"Loaded {len(companies)} companies")
    return companies

def fetch_companyfacts(cik10: str, ticker: str, session: requests.Session) -> Optional[Dict]:
    url = BASE_URL.format(cik10)
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, headers={'User-Agent': USER_AGENT}, timeout=30)
            if resp.status_code == 200:
                logger.info(f"Fetched {ticker} (CIK {cik10})")
                return resp.json()
            elif resp.status_code == 404:
                logger.warning(f"No company facts for {ticker} (CIK {cik10})")
                return None
            elif resp.status_code == 429:
                logger.warning("Rate limited, sleeping 30s")
                time.sleep(30)
        except requests.RequestException as e:
            logger.error(f"Request error for {ticker}: {e}")
        time.sleep(RETRY_DELAY * (attempt + 1))
    logger.error(f"Failed to fetch {ticker} after {MAX_RETRIES} attempts")
    return None

def save_raw_json(ticker: str, facts: Dict, dirs: Dict[str, Path]) -> None:
    ticker_dir = dirs['raw'] / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)
    output_file = ticker_dir / 'companyfacts.json'
    with open(output_file, 'w') as f:
        json.dump(facts, f, indent=2)
    logger.debug(f"Saved JSON for {ticker} to {output_file}")

def main():
    run_date = datetime.now().strftime('%Y%m%d')
    dirs = setup_directories()
    companies = load_cik_map(run_date)
    session = requests.Session()
    session.headers.update({'User-Agent': USER_AGENT})

    for i, company in enumerate(companies, 1):
        ticker = company['ticker']
        cik10 = company['cik10']
        logger.info(f"Processing {i}/{len(companies)}: {ticker}")
        if i > 1:
            time.sleep(RATE_LIMIT_DELAY)
        facts = fetch_companyfacts(cik10, ticker, session)
        if facts:
            save_raw_json(ticker, facts, dirs)

if __name__ == "__main__":
    main()
