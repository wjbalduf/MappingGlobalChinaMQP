"""
09_qc_and_reports.py - Quality Control and Reporting

Purpose:
Run comprehensive quality checks on the processed EDGAR data pipeline outputs
and generate detailed reports for validation and monitoring.

This script performs:
1. Data completeness checks for parents, subsidiaries, and addresses
2. Data consistency validation across different sources
3. Jurisdiction drift detection for subsidiaries
4. Generation of run summary statistics
5. Error logging in JSONL format

Usage:
    python scripts/09_qc_and_reports.py

Outputs:
    - logs/run_summary.json: Comprehensive statistics about the pipeline run
    - logs/errors.jsonl: Detailed error log with context
    - logs/qc_report_{RUN_DATE}.html: Human-readable HTML report
"""

import os
import json
import glob
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Any
import hashlib
import re

# Configuration
RUN_DATE = datetime.now().strftime("%Y%m%d")
BASE_DIR = Path(__file__).parent.parent.absolute()
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"

# Create logs directory if it doesn't exist
LOGS_DIR.mkdir(exist_ok=True)

# Error codes for standardized logging
ERROR_CODES = {
    "MISSING_INCORP_COUNTRY": "Parent company missing incorporation country",
    "MISSING_JURISDICTION": "Subsidiary missing jurisdiction",
    "LOW_PARSE_CONFIDENCE": "Parse confidence below threshold",
    "JURISDICTION_DRIFT": "Subsidiary jurisdiction changed over time",
    "NO_ADDRESS": "Company missing address despite offshore domicile",
    "NO_EX21_FOUND": "No EX-21 exhibit found for company",
    "DEI_MISSING": "DEI data missing for company",
    "CONFLICT_SOURCES": "Conflicting data between sources",
    "DATA_INTEGRITY": "Data integrity check failed",
    "FILE_NOT_FOUND": "Expected file not found",
    "INVALID_CIK": "Invalid CIK format",
    "DUPLICATE_ENTRY": "Duplicate entries found"
}

# Offshore jurisdictions that should have addresses
OFFSHORE_JURISDICTIONS = {"CYM", "HKG", "VGB", "BMU", "VIR", "SGP"}

# Quality thresholds
MIN_PARSE_CONFIDENCE = 0.60
TARGET_MAPPING_RATE = 0.95


class QCChecker:
    """Main quality control checker class"""

    def __init__(self):
        self.errors = []
        self.warnings = []
        self.stats = {
            "start_time": datetime.now().isoformat(),
            "run_date": RUN_DATE,
            "checks_performed": [],
            "total_parents": 0,
            "total_subsidiaries": 0,
            "total_addresses": 0,
            "errors_by_code": {},
            "warnings_count": 0,
            "files_processed": []
        }

    def log_error(self, ticker: str, cik10: str, error_code: str,
                  error_msg: str, context: Dict = None):
        """Log an error with context"""
        error_entry = {
            "timestamp": datetime.now().isoformat(),
            "ticker": ticker,
            "cik10": cik10,
            "error_code": error_code,
            "error_msg": error_msg,
            "context": context or {}
        }
        self.errors.append(error_entry)

        # Update stats
        if error_code not in self.stats["errors_by_code"]:
            self.stats["errors_by_code"][error_code] = 0
        self.stats["errors_by_code"][error_code] += 1

    def log_warning(self, msg: str):
        """Log a warning"""
        self.warnings.append({
            "timestamp": datetime.now().isoformat(),
            "message": msg
        })
        self.stats["warnings_count"] += 1

    def get_latest_file(self, pattern: str) -> Path:
        """Get the latest file matching a pattern"""
        files = list(DATA_DIR.glob(pattern))
        if not files:
            return None

        # Sort by date in filename
        def extract_date(f):
            match = re.search(r'(\d{8})(?=\.)', f.name)
            return match.group(1) if match else "00000000"

        return max(files, key=lambda f: extract_date(f))

    def check_parents_master(self):
        """Check parents master data quality"""
        print("\n[QC] Checking parents master data...")
        self.stats["checks_performed"].append("parents_master")

        # Find latest parents file
        parents_file = self.get_latest_file("clean/parents_master_*.csv")
        if not parents_file:
            self.log_error("", "", "FILE_NOT_FOUND",
                          "No parents_master file found")
            return

        self.stats["files_processed"].append(str(parents_file))
        # FIX: Read CIK as string to preserve leading zeros
        df = pd.read_csv(parents_file, dtype={'parent_cik10': str})
        self.stats["total_parents"] = len(df)

        # Check 1: Missing incorporation country
        missing_country = df[df["incorp_country_iso3"].isna()]
        for _, row in missing_country.iterrows():
            self.log_error(
                row.get("parent_ticker", ""),
                row.get("parent_cik10", ""),
                "MISSING_INCORP_COUNTRY",
                "Parent company missing incorporation country",
                {"parent_name": row.get("parent_name", "")}
            )

        # Check 2: Invalid CIK format
        if "parent_cik10" in df.columns:
            invalid_ciks = df[~df["parent_cik10"].astype(str).str.match(r'^\d{10}$')]
            for _, row in invalid_ciks.iterrows():
                self.log_error(
                    row.get("parent_ticker", ""),
                    row.get("parent_cik10", ""),
                    "INVALID_CIK",
                    f"Invalid CIK format: {row.get('parent_cik10', '')}",
                    {}
                )

        # Check 3: Source conflicts (if DEI disagrees with other sources)
        if "sources_used" in df.columns:
            multi_source = df[df["sources_used"].str.contains(r'\|', na=False)]
            # Log as info, not error
            if len(multi_source) > 0:
                print(f"  [INFO] {len(multi_source)} parents use multiple sources")

        # Check 4: Missing legal form
        missing_legal = df[df["legal_form"].isna()]
        if len(missing_legal) > 0:
            print(f"  [WARN] {len(missing_legal)} parents missing legal form")

        print(f"  [OK] Processed {len(df)} parent companies")
        print(f"  [ERROR] Found {len(missing_country)} missing incorporation country")

    def check_subsidiaries_master(self):
        """Check subsidiaries master data quality"""
        print("\n[QC] Checking subsidiaries master data...")
        self.stats["checks_performed"].append("subsidiaries_master")

        # Find latest subs file
        subs_file = self.get_latest_file("clean/subs_master_*.csv")
        if not subs_file:
            self.log_error("", "", "FILE_NOT_FOUND",
                          "No subs_master file found")
            return

        self.stats["files_processed"].append(str(subs_file))
        df = pd.read_csv(subs_file)
        self.stats["total_subsidiaries"] = len(df)

        # Check 1: Missing jurisdiction
        missing_jurisdiction = df[df["jurisdiction_iso3"].isna()]
        for _, row in missing_jurisdiction.iterrows():
            self.log_error(
                row.get("parent_ticker", ""),
                row.get("parent_cik10", ""),
                "MISSING_JURISDICTION",
                f"Subsidiary '{row.get('subsidiary_name', '')}' missing jurisdiction",
                {"subsidiary_name": row.get("subsidiary_name", ""),
                 "sub_uuid": row.get("sub_uuid", "")}
            )

        # Check 2: Low parse confidence
        if "parse_confidence" in df.columns:
            low_confidence = df[df["parse_confidence"] < MIN_PARSE_CONFIDENCE]
            for _, row in low_confidence.iterrows():
                self.log_error(
                    row.get("parent_ticker", ""),
                    row.get("parent_cik10", ""),
                    "LOW_PARSE_CONFIDENCE",
                    f"Parse confidence {row.get('parse_confidence', 0):.2f} below threshold",
                    {"subsidiary_name": row.get("subsidiary_name", ""),
                     "confidence": row.get("parse_confidence", 0)}
                )

        # Check 3: Jurisdiction drift (same subsidiary, different jurisdictions over time)
        if "sub_uuid" in df.columns and "jurisdiction_iso3" in df.columns:
            drift_check = df.groupby("sub_uuid")["jurisdiction_iso3"].nunique()
            drifted = drift_check[drift_check > 1]
            for sub_uuid in drifted.index:
                sub_data = df[df["sub_uuid"] == sub_uuid].iloc[0]
                jurisdictions = df[df["sub_uuid"] == sub_uuid]["jurisdiction_iso3"].unique()
                self.log_error(
                    sub_data.get("parent_ticker", ""),
                    sub_data.get("parent_cik10", ""),
                    "JURISDICTION_DRIFT",
                    f"Subsidiary jurisdiction changed: {list(jurisdictions)}",
                    {"subsidiary_name": sub_data.get("subsidiary_name", ""),
                     "sub_uuid": sub_uuid,
                     "jurisdictions": list(jurisdictions)}
                )

        # Check 4: Duplicate entries
        if "sub_uuid" in df.columns:
            duplicates = df[df.duplicated(subset=["parent_cik10", "sub_uuid"], keep=False)]
            if len(duplicates) > 0:
                for cik in duplicates["parent_cik10"].unique():
                    parent_dups = duplicates[duplicates["parent_cik10"] == cik]
                    self.log_error(
                        parent_dups.iloc[0].get("parent_ticker", ""),
                        cik,
                        "DUPLICATE_ENTRY",
                        f"Duplicate subsidiary entries found",
                        {"count": len(parent_dups)}
                    )

        print(f"  [OK] Processed {len(df)} subsidiaries")
        print(f"  [ERROR] Found {len(missing_jurisdiction)} missing jurisdiction")
        if "parse_confidence" in df.columns:
            print(f"  [ERROR] Found {len(low_confidence) if 'low_confidence' in locals() else 0} with low parse confidence")

    def check_addresses_master(self):
        """Check addresses master data quality"""
        print("\n[QC] Checking addresses master data...")
        self.stats["checks_performed"].append("addresses_master")

        # Find latest addresses file
        addr_file = self.get_latest_file("clean/addresses_master_*.csv")
        if not addr_file:
            self.log_warning("No addresses_master file found")
            return

        self.stats["files_processed"].append(str(addr_file))
        df = pd.read_csv(addr_file)
        self.stats["total_addresses"] = len(df)

        # Load parents to check offshore without addresses
        parents_file = self.get_latest_file("clean/parents_master_*.csv")
        if parents_file:
            parents_df = pd.read_csv(parents_file)

            # Check: Offshore parents without addresses
            offshore_parents = parents_df[
                parents_df["incorp_country_iso3"].isin(OFFSHORE_JURISDICTIONS)
            ]

            for _, parent in offshore_parents.iterrows():
                cik = parent.get("parent_cik10", "")
                # Check if this parent has any address
                parent_addresses = df[
                    (df["entity_type"] == "parent") &
                    (df["entity_id"] == cik)
                ]

                if len(parent_addresses) == 0:
                    self.log_error(
                        parent.get("parent_ticker", ""),
                        cik,
                        "NO_ADDRESS",
                        f"Offshore company ({parent.get('incorp_country_iso3', '')}) missing address",
                        {"parent_name": parent.get("parent_name", ""),
                         "country": parent.get("incorp_country_iso3", "")}
                    )

        # Check: Address parsing quality
        if "country_iso3" in df.columns:
            missing_country = df[df["country_iso3"].isna()]
            if len(missing_country) > 0:
                print(f"  [WARN] {len(missing_country)} addresses missing parsed country")

        print(f"  [OK] Processed {len(df)} addresses")

    def check_exhibits_completeness(self):
        """Check exhibit file completeness"""
        print("\n[QC] Checking exhibits completeness...")
        self.stats["checks_performed"].append("exhibits_completeness")

        # Load exhibits index
        exhibits_file = self.get_latest_file("intermediate/exhibits_index_*.json")
        if not exhibits_file:
            self.log_warning("No exhibits_index file found")
            return

        self.stats["files_processed"].append(str(exhibits_file))
        with open(exhibits_file, 'r') as f:
            exhibits = json.load(f)

        # Group by ticker to check EX-21 coverage
        ticker_exhibits = {}
        for exhibit in exhibits:
            ticker = exhibit.get("ticker", "")
            if ticker not in ticker_exhibits:
                ticker_exhibits[ticker] = {"ex21": False, "ex3": False}

            if exhibit.get("exhibit_type") == "ex21":
                ticker_exhibits[ticker]["ex21"] = True
            elif exhibit.get("exhibit_type") == "ex3":
                ticker_exhibits[ticker]["ex3"] = True

        # Check companies without EX-21
        no_ex21 = [t for t, e in ticker_exhibits.items() if not e["ex21"]]
        for ticker in no_ex21:
            # Find CIK for this ticker
            cik_file = self.get_latest_file("intermediate/cik_map_*.csv")
            if cik_file:
                cik_df = pd.read_csv(cik_file)
                ticker_row = cik_df[cik_df["ticker"] == ticker]
                if not ticker_row.empty:
                    self.log_error(
                        ticker,
                        ticker_row.iloc[0].get("cik10", ""),
                        "NO_EX21_FOUND",
                        "No EX-21 exhibit found",
                        {}
                    )

        print(f"  [OK] Found exhibits for {len(ticker_exhibits)} companies")
        print(f"  [ERROR] {len(no_ex21)} companies missing EX-21")

    def calculate_statistics(self):
        """Calculate summary statistics"""
        print("\n[QC] Calculating statistics...")

        # Calculate data coverage rates
        if self.stats["total_parents"] > 0:
            # Load CIK map to get total expected companies
            cik_file = self.get_latest_file("intermediate/cik_map_*.csv")
            if cik_file:
                cik_df = pd.read_csv(cik_file)
                total_expected = len(cik_df)
                self.stats["mapping_rate"] = self.stats["total_parents"] / total_expected

                if self.stats["mapping_rate"] < TARGET_MAPPING_RATE:
                    self.log_warning(
                        f"Mapping rate {self.stats['mapping_rate']:.1%} below target {TARGET_MAPPING_RATE:.0%}"
                    )

        # Calculate error rates
        total_errors = sum(self.stats["errors_by_code"].values())
        self.stats["total_errors"] = total_errors
        self.stats["critical_errors"] = len([e for e in self.errors
                                            if e["error_code"] in ["MISSING_INCORP_COUNTRY", "NO_EX21_FOUND"]])

        # Calculate file sizes processed
        total_bytes = 0
        for file_path in self.stats["files_processed"]:
            if os.path.exists(file_path):
                total_bytes += os.path.getsize(file_path)
        self.stats["total_bytes_processed"] = total_bytes

        # End time
        self.stats["end_time"] = datetime.now().isoformat()
        start = datetime.fromisoformat(self.stats["start_time"])
        end = datetime.fromisoformat(self.stats["end_time"])
        self.stats["duration_seconds"] = (end - start).total_seconds()

    def generate_html_report(self):
        """Generate human-readable HTML report"""
        print("\n[QC] Generating HTML report...")

        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>EDGAR Pipeline QC Report - {RUN_DATE}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        .header {{ background: #2c3e50; color: white; padding: 20px; border-radius: 5px; }}
        .summary {{ background: white; padding: 20px; margin: 20px 0; border-radius: 5px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .metric {{ display: inline-block; margin: 10px 20px; }}
        .metric-value {{ font-size: 24px; font-weight: bold; color: #2c3e50; }}
        .metric-label {{ color: #7f8c8d; font-size: 12px; }}
        .error {{ background: #ffebee; padding: 15px; margin: 10px 0; border-left: 4px solid #f44336; }}
        .warning {{ background: #fff3e0; padding: 15px; margin: 10px 0; border-left: 4px solid #ff9800; }}
        .success {{ background: #e8f5e9; padding: 15px; margin: 10px 0; border-left: 4px solid #4caf50; }}
        table {{ width: 100%; border-collapse: collapse; background: white; }}
        th {{ background: #ecf0f1; padding: 10px; text-align: left; }}
        td {{ padding: 10px; border-bottom: 1px solid #ecf0f1; }}
        .critical {{ color: #f44336; font-weight: bold; }}
        .footer {{ text-align: center; color: #7f8c8d; margin-top: 30px; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>EDGAR Pipeline Quality Control Report</h1>
        <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>Run Date: {RUN_DATE}</p>
    </div>

    <div class="summary">
        <h2>Summary Statistics</h2>
        <div class="metrics">
            <div class="metric">
                <div class="metric-value">{self.stats['total_parents']}</div>
                <div class="metric-label">Parent Companies</div>
            </div>
            <div class="metric">
                <div class="metric-value">{self.stats['total_subsidiaries']}</div>
                <div class="metric-label">Subsidiaries</div>
            </div>
            <div class="metric">
                <div class="metric-value">{self.stats['total_addresses']}</div>
                <div class="metric-label">Addresses</div>
            </div>
            <div class="metric">
                <div class="metric-value">{self.stats['total_errors']}</div>
                <div class="metric-label">Total Errors</div>
            </div>
            <div class="metric">
                <div class="metric-value">{self.stats['critical_errors']}</div>
                <div class="metric-label">Critical Errors</div>
            </div>
        </div>
    </div>

    <div class="summary">
        <h2>Quality Checks Performed</h2>
        <ul>
        """

        for check in self.stats["checks_performed"]:
            html_content += f"<li>✓ {check.replace('_', ' ').title()}</li>\n"

        html_content += """
        </ul>
    </div>

    <div class="summary">
        <h2>Errors by Category</h2>
        <table>
            <tr>
                <th>Error Code</th>
                <th>Description</th>
                <th>Count</th>
            </tr>
        """

        for code, count in sorted(self.stats["errors_by_code"].items(),
                                  key=lambda x: x[1], reverse=True):
            severity_class = "critical" if code in ["MISSING_INCORP_COUNTRY", "NO_EX21_FOUND"] else ""
            html_content += f"""
            <tr>
                <td class="{severity_class}">{code}</td>
                <td>{ERROR_CODES.get(code, code)}</td>
                <td>{count}</td>
            </tr>
            """

        html_content += """
        </table>
    </div>
    """

        # Add top errors detail
        if self.errors:
            html_content += """
    <div class="summary">
        <h2>Sample Errors (First 10)</h2>
        """
            for error in self.errors[:10]:
                html_content += f"""
        <div class="error">
            <strong>{error['ticker']} ({error['cik10']})</strong><br>
            {error['error_code']}: {error['error_msg']}<br>
            <small>{error.get('context', {})}</small>
        </div>
        """
            html_content += "</div>"

        # Add warnings
        if self.warnings:
            html_content += """
    <div class="summary">
        <h2>Warnings</h2>
        """
            for warning in self.warnings:
                html_content += f"""
        <div class="warning">
            {warning['message']}
        </div>
        """
            html_content += "</div>"

        # Add success message if no critical errors
        if self.stats["critical_errors"] == 0:
            html_content += """
    <div class="summary">
        <div class="success">
            <h3>✓ No Critical Errors Found</h3>
            <p>All parent companies have incorporation country data and exhibit coverage is complete.</p>
        </div>
    </div>
        """

        html_content += f"""
    <div class="footer">
        <p>Pipeline Duration: {self.stats.get('duration_seconds', 0):.2f} seconds</p>
        <p>Total Data Processed: {self.stats.get('total_bytes_processed', 0) / 1024 / 1024:.2f} MB</p>
    </div>
</body>
</html>
        """

        # Save HTML report
        report_path = LOGS_DIR / f"qc_report_{RUN_DATE}.html"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(html_content)

        print(f"  [OK] HTML report saved to {report_path}")

    def save_logs(self):
        """Save error logs and run summary"""
        print("\n[QC] Saving logs...")

        # Save errors to JSONL
        errors_path = LOGS_DIR / "errors.jsonl"
        with open(errors_path, 'a', encoding='utf-8') as f:
            for error in self.errors:
                f.write(json.dumps(error) + '\n')
        print(f"  [OK] Appended {len(self.errors)} errors to {errors_path}")

        # Also save timestamped version
        errors_dated_path = LOGS_DIR / f"errors_{RUN_DATE}.jsonl"
        with open(errors_dated_path, 'w', encoding='utf-8') as f:
            for error in self.errors:
                f.write(json.dumps(error) + '\n')

        # Save run summary
        summary_path = LOGS_DIR / "run_summary.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(self.stats, f, indent=2)
        print(f"  [OK] Saved run summary to {summary_path}")

    def run_all_checks(self):
        """Run all quality checks"""
        print("=" * 60)
        print("EDGAR PIPELINE QUALITY CONTROL")
        print(f"Run Date: {RUN_DATE}")
        print("=" * 60)

        # Run all checks
        self.check_parents_master()
        self.check_subsidiaries_master()
        self.check_addresses_master()
        self.check_exhibits_completeness()

        # Calculate statistics
        self.calculate_statistics()

        # Generate reports
        self.generate_html_report()
        self.save_logs()

        # Print final summary
        print("\n" + "=" * 60)
        print("QC SUMMARY")
        print("=" * 60)
        print(f"Total Errors: {self.stats['total_errors']}")
        print(f"Critical Errors: {self.stats['critical_errors']}")
        print(f"Warnings: {self.stats['warnings_count']}")

        if self.stats['critical_errors'] == 0:
            print("\n[PASS] PIPELINE PASSED: No critical errors found")
            return 0
        else:
            print(f"\n[FAIL] PIPELINE FAILED: {self.stats['critical_errors']} critical errors found")
            print("   Review the HTML report for details")
            return 1


def main():
    """Main entry point"""
    checker = QCChecker()
    exit_code = checker.run_all_checks()

    print(f"\n[INFO] Reports generated:")
    print(f"   - logs/qc_report_{RUN_DATE}.html")
    print(f"   - logs/run_summary.json")
    print(f"   - logs/errors.jsonl")

    return exit_code


if __name__ == "__main__":
    exit(main())