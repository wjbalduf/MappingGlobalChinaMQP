#!/usr/bin/env python3
"""

Checks:
- Exhibit files and indexes (EX-21, EX-3)
- Annual reports index
- CIK mappings
- DEI facts data
- Company directories and file statistics
"""

import json
import os
import sys
import csv
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
import hashlib
from collections import defaultdict

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get run date from environment or use current date
RUN_DATE = os.environ.get('RUN_DATE', datetime.now().strftime('%Y%m%d'))

# Define paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
LOGS_DIR = BASE_DIR / 'logs'
COMPANIES_DIR = BASE_DIR / 'companies'

# Ensure logs directory exists
LOGS_DIR.mkdir(exist_ok=True)

# Error codes
ERROR_CODES = {
    'MISSING_INCORP_COUNTRY': 'Parent missing incorporation country',
    'DEI_CONFLICT': 'Conflicting DEI signals not resolved properly',
    'MISSING_JURISDICTION': 'Subsidiary missing jurisdiction ISO3',
    'LOW_PARSE_CONFIDENCE': 'Parse confidence below threshold',
    'JURISDICTION_DRIFT': 'Jurisdiction changed over time for same subsidiary',
    'MISSING_ADDRESS': 'No address found for key jurisdiction',
    'NO_EX21_FOUND': 'No EX-21 exhibit found',
    'NO_EX3_FOUND': 'No EX-3 exhibit found',
    'MISSING_FILE': 'Expected file not found',
    'DATA_INTEGRITY': 'Data integrity issue detected'
}

# Key addresses
KEY_JURISDICTIONS = {'CYM', 'HKG', 'VGB', 'BMU', 'SGP'}


class QCChecker:
    """Quality control checker for EDGAR pipeline outputs"""

    def __init__(self, run_date: str):
        self.run_date = run_date
        self.errors = []
        self.warnings = []
        self.stats = defaultdict(int)
        self.start_time = datetime.now()

    def check_parents_master(self) -> Dict[str, Any]:
        """Check parents_master CSV for completeness and consistency"""
        file_path = DATA_DIR / 'clean' / f'parents_master_{self.run_date}.csv'

        if not file_path.exists():
            self.log_error('MISSING_FILE', f'Parents master file not found: {file_path}')
            return {'checked': 0, 'errors': len(self.errors)}

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            total_rows = len(rows)
            self.stats['parents_total'] = total_rows

            # Check for missing incorporation country
            missing_country = []
            valid_records = []
            dei_count = 0

            for row in rows:
                if not row.get('incorp_country_iso3') or row['incorp_country_iso3'].strip() == '':
                    missing_country.append(row)
                    self.log_error(
                        'MISSING_INCORP_COUNTRY',
                        f"Parent {row.get('parent_ticker', 'UNKNOWN')} missing incorporation country",
                        {'ticker': row.get('parent_ticker'), 'cik10': row.get('parent_cik10')}
                    )
                else:
                    valid_records.append(row)

                # Check for DEI in sources_used
                if row.get('sources_used') and 'DEI' in row['sources_used']:
                    dei_count += 1

            self.stats['parents_with_dei'] = dei_count
            self.stats['parents_valid'] = len(valid_records)

            logger.info(f"Parents check: {len(valid_records)}/{total_rows} valid records")

            return {
                'checked': total_rows,
                'valid': len(valid_records),
                'errors': len(missing_country)
            }

        except Exception as e:
            self.log_error('DATA_INTEGRITY', f"Error reading parents master: {str(e)}")
            return {'checked': 0, 'errors': 1}

    def check_subsidiaries_master(self) -> Dict[str, Any]:
        """Check subsidiaries_master CSV for data quality"""
        file_path = DATA_DIR / 'clean' / f'subs_master_{self.run_date}.csv'

        if not file_path.exists():
            logger.warning(f"Subsidiaries master file not found: {file_path}")
            return {'checked': 0, 'errors': 0}

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            total_rows = len(rows)
            self.stats['subs_total'] = total_rows

            issues_count = 0
            missing_jurisdiction_count = 0
            high_confidence_count = 0

            # Track sub_uuid jurisdictions for drift detection
            sub_uuid_jurisdictions = defaultdict(set)

            for row in rows:
                # Check for missing jurisdiction ISO3
                if not row.get('jurisdiction_iso3') or row['jurisdiction_iso3'].strip() == '':
                    missing_jurisdiction_count += 1
                    self.log_warning(
                        'MISSING_JURISDICTION',
                        f"Subsidiary {row.get('subsidiary_name', 'UNKNOWN')} missing jurisdiction ISO3",
                        {'parent_ticker': row.get('parent_ticker'), 'sub_name': row.get('subsidiary_name')}
                    )
                    issues_count += 1

                # Check for low parse confidence (< 0.60)
                if 'parse_confidence' in row:
                    try:
                        confidence = float(row['parse_confidence'])
                        if confidence < 0.60:
                            self.log_warning(
                                'LOW_PARSE_CONFIDENCE',
                                f"Low confidence ({confidence}) for {row.get('subsidiary_name', 'UNKNOWN')}",
                                {'parent_ticker': row.get('parent_ticker'), 'confidence': confidence}
                            )
                            issues_count += 1
                        else:
                            high_confidence_count += 1
                    except (ValueError, TypeError):
                        pass

                # Track jurisdictions by sub_uuid for drift detection
                if row.get('sub_uuid') and row.get('jurisdiction_iso3'):
                    sub_uuid_jurisdictions[row['sub_uuid']].add(row['jurisdiction_iso3'])

            # Check for jurisdiction drift
            for sub_uuid, jurisdictions in sub_uuid_jurisdictions.items():
                if len(jurisdictions) > 1:
                    self.log_warning(
                        'JURISDICTION_DRIFT',
                        f"Jurisdiction drift detected for subsidiary {sub_uuid}",
                        {
                            'sub_uuid': sub_uuid,
                            'jurisdictions': list(jurisdictions)
                        }
                    )
                    issues_count += 1

            self.stats['subs_high_confidence'] = high_confidence_count

            logger.info(f"Subsidiaries check: {total_rows} total, {issues_count} issues found")

            return {
                'checked': total_rows,
                'issues': issues_count,
                'missing_jurisdiction': missing_jurisdiction_count
            }

        except Exception as e:
            self.log_error('DATA_INTEGRITY', f"Error reading subsidiaries master: {str(e)}")
            return {'checked': 0, 'errors': 1}

    def check_addresses_master(self) -> Dict[str, Any]:
        """Check addresses_master CSV for completeness"""
        file_path = DATA_DIR / 'clean' / f'addresses_master_{self.run_date}.csv'

        if not file_path.exists():
            logger.warning(f"Addresses master file not found: {file_path}")
            return {'checked': 0, 'errors': 0}

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                address_rows = list(reader)

            total_rows = len(address_rows)
            self.stats['addresses_total'] = total_rows

            # Load parents to check for key jurisdictions
            parents_file = DATA_DIR / 'clean' / f'parents_master_{self.run_date}.csv'
            if parents_file.exists():
                with open(parents_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    parent_rows = list(reader)

                # Build index of addresses by entity
                parent_addresses = defaultdict(list)
                for addr in address_rows:
                    if addr.get('entity_type') == 'parent':
                        parent_addresses[addr.get('entity_id')].append(addr)

                # Check if parents in key jurisdictions have addresses
                for parent in parent_rows:
                    if parent.get('incorp_country_iso3') in KEY_JURISDICTIONS:
                        cik = parent.get('parent_cik10')
                        if cik not in parent_addresses or len(parent_addresses[cik]) == 0:
                            self.log_warning(
                                'MISSING_ADDRESS',
                                f"Parent {parent.get('parent_ticker')} in {parent['incorp_country_iso3']} has no address",
                                {
                                    'ticker': parent.get('parent_ticker'),
                                    'cik10': cik,
                                    'jurisdiction': parent['incorp_country_iso3']
                                }
                            )

            # Count addresses with parsed country
            with_country_count = 0
            for addr in address_rows:
                if addr.get('country_iso3') and addr['country_iso3'].strip():
                    with_country_count += 1

            self.stats['addresses_with_country'] = with_country_count
            logger.info(f"Addresses with country: {with_country_count}/{total_rows}")

            return {
                'checked': total_rows,
                'with_country': with_country_count
            }

        except Exception as e:
            self.log_error('DATA_INTEGRITY', f"Error reading addresses master: {str(e)}")
            return {'checked': 0, 'errors': 1}

    def check_intermediate_files(self) -> Dict[str, Any]:
        """Check intermediate data files for completeness"""
        results = {}

        # Check annual reports index
        annual_reports_path = DATA_DIR / 'intermediate' / 'annual_reports_index.json'
        if annual_reports_path.exists():
            try:
                with open(annual_reports_path, 'r') as f:
                    reports = json.load(f)
                self.stats['annual_reports_count'] = len(reports)
                logger.info(f"Annual reports index: {len(reports)} entries")
                results['annual_reports'] = len(reports)
            except Exception as e:
                logger.warning(f"Error reading annual reports index: {e}")
        else:
            logger.info("Annual reports index not found (expected if script 03 not run)")

        # Check CIK map
        cik_files = list((DATA_DIR / 'intermediate').glob('cik_map_*.csv'))
        if cik_files:
            latest_cik = max(cik_files, key=lambda x: x.stem.split('_')[-1])
            try:
                with open(latest_cik, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    cik_rows = list(reader)
                self.stats['cik_mappings'] = len(cik_rows)
                logger.info(f"CIK mappings: {len(cik_rows)} companies")
                results['cik_mappings'] = len(cik_rows)
            except Exception as e:
                logger.warning(f"Error reading CIK map: {e}")
        else:
            logger.info("CIK map not found (expected if script 02 not run)")

        # Check DEI facts
        dei_files = list((DATA_DIR / 'intermediate').glob('dei_facts_*.csv'))
        if dei_files:
            latest_dei = max(dei_files, key=lambda x: x.stem.split('_')[-1])
            try:
                with open(latest_dei, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    dei_rows = list(reader)
                self.stats['dei_facts'] = len(dei_rows)
                logger.info(f"DEI facts: {len(dei_rows)} companies")
                results['dei_facts'] = len(dei_rows)
            except Exception as e:
                logger.warning(f"Error reading DEI facts: {e}")
        else:
            logger.info("DEI facts not found (expected if script 04 not run)")

        return results

    def check_exhibit_files(self) -> Dict[str, Any]:
        """Check exhibit files and indexes"""
        exhibits_index_path = DATA_DIR / 'intermediate' / f'exhibits_index_{self.run_date}.json'

        if not exhibits_index_path.exists():
            logger.warning(f"Exhibits index not found: {exhibits_index_path}")
            return {'checked': 0, 'errors': 0}

        try:
            with open(exhibits_index_path, 'r') as f:
                exhibits = json.load(f)

            total_exhibits = len(exhibits)
            self.stats['exhibits_total'] = total_exhibits

            ex21_count = 0
            ex3_count = 0
            missing_files = 0

            for exhibit in exhibits:
                if exhibit.get('exhibit_type') == 'ex21':
                    ex21_count += 1
                elif exhibit.get('exhibit_type') == 'ex3':
                    ex3_count += 1

                # Check if local file exists
                local_path = exhibit.get('localPath')
                if local_path:
                    full_path = BASE_DIR / local_path
                    if not full_path.exists():
                        missing_files += 1
                        self.log_warning(
                            'MISSING_FILE',
                            f"Exhibit file not found: {local_path}",
                            {'ticker': exhibit.get('ticker'), 'exhibit': exhibit.get('exhibit_label')}
                        )

            self.stats['exhibits_ex21'] = ex21_count
            self.stats['exhibits_ex3'] = ex3_count
            self.stats['exhibits_missing_files'] = missing_files

            logger.info(f"Exhibits: {ex21_count} EX-21, {ex3_count} EX-3, {missing_files} missing files")

            return {
                'total': total_exhibits,
                'ex21': ex21_count,
                'ex3': ex3_count,
                'missing': missing_files
            }

        except Exception as e:
            self.log_error('DATA_INTEGRITY', f"Error reading exhibits index: {str(e)}")
            return {'checked': 0, 'errors': 1}

    def generate_run_summary(self) -> Dict[str, Any]:
        """Generate comprehensive run summary"""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()

        # Count files created
        files_created = 0
        total_bytes = 0

        # Count company directories
        if COMPANIES_DIR.exists():
            company_dirs = len(list(COMPANIES_DIR.iterdir()))
            self.stats['company_directories'] = company_dirs

            # Count all files and calculate size
            for company_dir in COMPANIES_DIR.iterdir():
                if company_dir.is_dir():
                    for file_path in company_dir.rglob('*'):
                        if file_path.is_file():
                            files_created += 1
                            total_bytes += file_path.stat().st_size

        summary = {
            'run_date': self.run_date,
            'start_time': self.start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': duration,
            'stats': dict(self.stats),
            'files_created': files_created,
            'total_bytes': total_bytes,
            'errors_count': len(self.errors),
            'warnings_count': len(self.warnings),
            'error_breakdown': self._count_errors_by_code()
        }

        return summary

    def _count_errors_by_code(self) -> Dict[str, int]:
        """Count errors and warnings by error code"""
        counts = defaultdict(int)

        for error in self.errors:
            counts[error['error_code']] += 1

        for warning in self.warnings:
            counts[warning['error_code']] += 1

        return dict(counts)

    def log_error(self, error_code: str, message: str, context: Optional[Dict] = None):
        """Log an error"""
        error_entry = {
            'timestamp': datetime.now().isoformat(),
            'level': 'ERROR',
            'error_code': error_code,
            'error_msg': message,
            'context': context or {}
        }
        self.errors.append(error_entry)
        logger.error(f"{error_code}: {message}")

    def log_warning(self, error_code: str, message: str, context: Optional[Dict] = None):
        """Log a warning"""
        warning_entry = {
            'timestamp': datetime.now().isoformat(),
            'level': 'WARNING',
            'error_code': error_code,
            'error_msg': message,
            'context': context or {}
        }
        self.warnings.append(warning_entry)
        logger.warning(f"{error_code}: {message}")

    def write_logs(self):
        """Write logs to files"""
        # Write run summary
        summary_path = LOGS_DIR / 'run_summary.json'
        summary = self.generate_run_summary()

        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)

        logger.info(f"Run summary written to {summary_path}")

        # Write errors to JSONL
        errors_path = LOGS_DIR / 'errors.jsonl'

        with open(errors_path, 'w') as f:
            for error in self.errors:
                f.write(json.dumps(error) + '\n')
            for warning in self.warnings:
                f.write(json.dumps(warning) + '\n')

        logger.info(f"Errors/warnings written to {errors_path}")

        return summary_path, errors_path


def main():
    """Main execution function"""
    logger.info("=" * 80)
    logger.info("Starting QC and Reporting Script")
    logger.info(f"Run date: {RUN_DATE}")
    logger.info("=" * 80)

    # Initialize QC checker
    checker = QCChecker(RUN_DATE)

    # Run all checks
    logger.info("\n--- Checking Exhibit Files ---")
    exhibits_results = checker.check_exhibit_files()

    # Check for other intermediate files
    logger.info("\n--- Checking Intermediate Data Files ---")
    checker.check_intermediate_files()

    # Write logs
    logger.info("\n--- Writing Logs ---")
    summary_path, errors_path = checker.write_logs()

    # Print summary
    logger.info("\n" + "=" * 80)
    logger.info("QC SUMMARY")
    logger.info("=" * 80)

    summary = checker.generate_run_summary()

    logger.info(f"Duration: {summary['duration_seconds']:.2f} seconds")
    logger.info(f"Files created: {summary['files_created']}")
    logger.info(f"Total bytes: {summary['total_bytes']:,}")
    logger.info(f"Errors: {summary['errors_count']}")
    logger.info(f"Warnings: {summary['warnings_count']}")

    if summary['error_breakdown']:
        logger.info("\nError breakdown:")
        for code, count in summary['error_breakdown'].items():
            logger.info(f"  {code}: {count}")

    # Determine exit status
    critical_errors = len(checker.errors)
    if critical_errors > 0:
        logger.error(f"\n QC completed with {critical_errors} critical errors")
        logger.error(f"Review {errors_path} for details")
        sys.exit(1)
    else:
        logger.info("\n QC completed successfully")
        if checker.warnings:
            logger.info(f"Note: {len(checker.warnings)} warnings found - review {errors_path}")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        sys.exit(1)
