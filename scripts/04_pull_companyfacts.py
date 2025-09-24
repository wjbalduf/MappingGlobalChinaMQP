#!/usr/bin/env python3
"""
04_pull_companyfacts.py - XBRL DEI Facts Extraction

Goal: Capture legal domicile, registrant name, and legal form from XBRL DEI
for Chinese companies listed on U.S. exchanges.

Input: /data/intermediate/cik_map_{RUN_DATE}.csv
API: https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json
Output: /data/intermediate/dei_facts_{RUN_DATE}.csv
        /data/raw/EDGAR/{TICKER}/companyfacts.json (raw JSON)


Usage:
    python scripts/04_pull_companyfacts.py

"""

import json
import csv
import os
import time
import logging
from datetime import datetime
from pathlib import Path
import requests
from typing import Dict, Optional, Any, List

# Constants
BASE_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{}.json"
USER_AGENT = "Mozilla/5.0 (Research Academic Contact: research@university.edu)"
RATE_LIMIT_DELAY = 0.15  # SEC rate limit: ~10 requests per second
MAX_RETRIES = 3
RETRY_DELAY = 5

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/04_pull_companyfacts.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def setup_directories(run_date: str) -> Dict[str, Path]:
    """Create necessary directories for data storage"""
    dirs = {
        'intermediate': Path(f'data/intermediate'),
        'raw': Path(f'data/raw/EDGAR'),
        'logs': Path('logs')
    }

    for dir_path in dirs.values():
        dir_path.mkdir(parents=True, exist_ok=True)

    return dirs


def load_cik_map(run_date: str) -> List[Dict[str, str]]:
    """Load the CIK mapping from previous step"""
    file_path = Path(f'data/intermediate/cik_map_{run_date}.csv')

    if not file_path.exists():
        raise FileNotFoundError(f"CIK map file not found: {file_path}")

    companies = []
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('cik10'):  # Only process companies with resolved CIKs
                companies.append(row)

    logger.info(f"Loaded {len(companies)} companies with CIK mappings")
    return companies


def fetch_companyfacts(cik10: str, ticker: str, session: requests.Session) -> Optional[Dict]:
    """Fetch company facts JSON from SEC API"""
    url = BASE_URL.format(cik10)

    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(
                url,
                headers={'User-Agent': USER_AGENT},
                timeout=30
            )

            if response.status_code == 200:
                logger.info(f"Successfully fetched company facts for {ticker} (CIK: {cik10})")
                return response.json()
            elif response.status_code == 404:
                logger.warning(f"No company facts found for {ticker} (CIK: {cik10})")
                return None
            elif response.status_code == 429:
                logger.warning(f"Rate limited, waiting longer...")
                time.sleep(30)
            else:
                logger.warning(f"Unexpected status {response.status_code} for {ticker}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for {ticker}: {e}")

        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_DELAY * (attempt + 1))

    logger.error(f"Failed to fetch company facts for {ticker} after {MAX_RETRIES} attempts")
    return None


def extract_dei_value(facts: Dict, concept: str) -> Optional[str]:
    """Extract the most recent value for a DEI concept"""
    try:
        dei_data = facts.get('facts', {}).get('dei', {}).get(concept, {})
        units_data = dei_data.get('units', {})

        # Try different unit types
        for unit_type in ['USD', 'pure', 'shares']:
            if unit_type in units_data:
                values = units_data[unit_type]
                if values:
                    # Sort by end date to get most recent
                    sorted_values = sorted(
                        values,
                        key=lambda x: x.get('end', x.get('instant', '')),
                        reverse=True
                    )
                    # Return the first non-null value
                    for val in sorted_values:
                        if val.get('val') is not None:
                            return str(val['val'])

        # For string values without units
        if isinstance(dei_data, dict) and 'value' in dei_data:
            return str(dei_data['value'])

    except Exception as e:
        logger.debug(f"Error extracting {concept}: {e}")

    return None


def process_company_facts(ticker: str, cik10: str, facts: Dict) -> Dict[str, str]:
    """Extract DEI fields from company facts JSON"""

    # Initialize result with basic info
    result = {
        'ticker': ticker,
        'cik10': cik10,
        'registrant_name': None,
        'incorp_country_raw': None,
        'incorp_state_raw': None,
        'legal_form': None,
        'trading_symbol': None,
        'filer_category': None,
        'latest_period_end': None
    }

    if not facts:
        return result

    # Map of DEI concepts to our field names
    dei_mappings = {
        'EntityRegistrantName': 'registrant_name',
        'EntityIncorporationStateCountryCode': 'incorp_state_raw',
        'CountryRegion': 'incorp_country_raw',
        'EntityIncorporationCountryCode': 'incorp_country_raw',  # Alternative field
        'EntityLegalForm': 'legal_form',
        'TradingSymbol': 'trading_symbol',
        'EntityFilerCategory': 'filer_category',
        'DocumentPeriodEndDate': 'latest_period_end'
    }

    # Extract each DEI concept
    for concept, field in dei_mappings.items():
        value = extract_dei_value(facts, concept)
        if value and (not result[field] or field == 'incorp_country_raw'):
            # For country, prefer EntityIncorporationCountryCode over CountryRegion
            if field == 'incorp_country_raw' and concept == 'EntityIncorporationCountryCode':
                result[field] = value
            elif field != 'incorp_country_raw' or not result[field]:
                result[field] = value

    # Clean up and normalize
    if result['registrant_name']:
        result['registrant_name'] = ' '.join(result['registrant_name'].split())

    logger.info(f"Extracted DEI data for {ticker}: {result['registrant_name']}, "
                f"Country: {result['incorp_country_raw']}, State: {result['incorp_state_raw']}")

    return result


def save_raw_json(ticker: str, facts: Dict, dirs: Dict[str, Path]) -> None:
    """Save raw company facts JSON for audit"""
    ticker_dir = dirs['raw'] / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)

    output_file = ticker_dir / 'companyfacts.json'
    with open(output_file, 'w') as f:
        json.dump(facts, f, indent=2)

    logger.debug(f"Saved raw JSON for {ticker} to {output_file}")


def main():
    """Main execution function"""
    # Get run date
    run_date = datetime.now().strftime('%Y%m%d')
    logger.info(f"Starting company facts extraction - Run date: {run_date}")

    # Setup directories
    dirs = setup_directories(run_date)

    # Load CIK mappings
    companies = load_cik_map(run_date)

    # Setup session for requests
    session = requests.Session()
    session.headers.update({'User-Agent': USER_AGENT})

    # Process each company
    dei_results = []
    errors = []

    for i, company in enumerate(companies, 1):
        ticker = company['ticker']
        cik10 = company['cik10']

        logger.info(f"Processing {i}/{len(companies)}: {ticker} (CIK: {cik10})")

        # Rate limiting
        if i > 1:
            time.sleep(RATE_LIMIT_DELAY)

        # Fetch company facts
        facts = fetch_companyfacts(cik10, ticker, session)

        if facts:
            # Save raw JSON
            save_raw_json(ticker, facts, dirs)

            # Extract DEI data
            dei_data = process_company_facts(ticker, cik10, facts)
            dei_results.append(dei_data)
        else:
            # Record error
            errors.append({
                'ticker': ticker,
                'cik10': cik10,
                'error': 'Failed to fetch company facts',
                'timestamp': datetime.now().isoformat()
            })
            # Add minimal entry to results
            dei_results.append({
                'ticker': ticker,
                'cik10': cik10,
                'registrant_name': None,
                'incorp_country_raw': None,
                'incorp_state_raw': None,
                'legal_form': None,
                'trading_symbol': None,
                'filer_category': None,
                'latest_period_end': None
            })

    # Save DEI facts to CSV
    output_file = dirs['intermediate'] / f'dei_facts_{run_date}.csv'

    if dei_results:
        fieldnames = ['ticker', 'cik10', 'registrant_name', 'incorp_country_raw',
                     'incorp_state_raw', 'legal_form', 'trading_symbol',
                     'filer_category', 'latest_period_end']

        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(dei_results)

        logger.info(f"Saved DEI facts to {output_file}")

    # Save errors if any
    if errors:
        error_file = dirs['logs'] / f'04_errors_{run_date}.json'
        with open(error_file, 'w') as f:
            json.dump(errors, f, indent=2)
        logger.warning(f"Recorded {len(errors)} errors to {error_file}")

    # Summary statistics
    logger.info("=" * 50)
    logger.info("SUMMARY")
    logger.info(f"Total companies processed: {len(companies)}")
    logger.info(f"Successful DEI extractions: {len([d for d in dei_results if d['registrant_name']])}")
    logger.info(f"Companies with country data: {len([d for d in dei_results if d['incorp_country_raw']])}")
    logger.info(f"Companies with state data: {len([d for d in dei_results if d['incorp_state_raw']])}")
    logger.info(f"Errors: {len(errors)}")

    # Acceptance test
    success_rate = len([d for d in dei_results if any(d[k] for k in ['registrant_name', 'incorp_country_raw', 'incorp_state_raw'])]) / len(companies) * 100
    logger.info(f"Success rate (≥1 DEI field): {success_rate:.1f}%")

    if success_rate < 75:
        logger.warning(f"WARNING: Success rate below 75% threshold")
    else:
        logger.info("✓ Acceptance test passed")


if __name__ == "__main__":
    main()
