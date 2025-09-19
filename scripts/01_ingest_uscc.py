"""
Extracts Chinese company listings from the USCC PDF and outputs
a canonical seed list cleaned CSV.

Usage:
    python 01_ingest_uscc.py --pdf "path/to/USCC_report.pdf" --out ./data
"""

import pdfplumber
import pandas as pd
import datetime
import os
import re
import argparse
from dateutil import parser

# Helper functions
def clean_numeric_column(series):
    """Convert numeric strings with $/commas/n/a to pandas nullable integer."""
    return (
        series
        .replace(r"[\$,]", "", regex=True)   # remove $ and commas
        .replace(r"n/?a", None, regex=True) # treat 'n/a' as NaN
        .astype(float)
        .astype("Int64") # nullable integer type
    )

def merge_continuation_rows(df, key_col='ticker'):
    """Merge rows where the key column is NaN/empty into the previous row."""
    merged_rows = []
    buffer = None

    for _, row in df.iterrows():
        key_val = str(row[key_col]).strip() if pd.notna(row[key_col]) else ""
        if key_val != "":
            if buffer is not None:
                merged_rows.append(buffer)
            buffer = row.copy()
        else:
            if buffer is not None:
                for col in df.columns:
                    if pd.notna(row[col]) and str(row[col]).strip() != "":
                        if pd.isna(buffer[col]):
                            buffer[col] = row[col]
                        else:
                            buffer[col] = str(buffer[col]).strip() + " " + str(row[col]).strip()
    if buffer is not None:
        merged_rows.append(buffer)

    return pd.DataFrame(merged_rows).reset_index(drop=True)

def fix_multiline_ipo(df, col='IPO Month'):
    """Merge IPO Month values that are split across multiple rows."""
    df = df.copy()
    skip_next = False

    for i in range(len(df)):
        if skip_next:
            skip_next = False
            continue

        val = str(df.at[i, col]).strip()
        if i + 1 < len(df):
            next_val = str(df.at[i + 1, col]).strip()
            if re.match(r"^[A-Za-z]{3,}$", val) and re.match(r"^\d{4}$", next_val):
                merged_val = val + " " + next_val
                df.at[i, col] = merged_val
                df.at[i + 1, col] = merged_val
                skip_next = True
    return df

def parse_ipo_month(val):
    """Convert IPO Month string to YYYY-MM format."""
    if pd.isna(val):
        return None

    val = str(val)
    val = val.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    val = val.replace("\xa0", " ").replace("–", " ")
    val = re.sub(r"\s+", " ", val).strip()

    if val == "":
        return None

    if re.fullmatch(r"\d{4}", val):
        return f"{val}-01"

    try:
        dt = parser.parse(val, default=datetime.datetime(1900, 1, 1))
        return dt.strftime("%Y-%m")
    except Exception:
        months = {
            "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
            "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
            "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"
        }
        month_match = re.search(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)", val, re.IGNORECASE)
        year_match = re.search(r"(\d{4})", val)
        if year_match:
            year = year_match.group(1)
            month = months.get(month_match.group(1).title(), "01") if month_match else "01"
            return f"{year}-{month}"
        return None

def detect_listing_pages(pdf_path, min_rows=2):
    """
    Detect start and end pages of USCC listings in a PDF.
    - start_page: first page whose table header contains 'ticker' or 'symbol'
    - end_page: last page that contains a table with at least `min_rows` of data
    Returns (start_page, end_page)
    """
    start_page = None
    last_page_with_table = None

    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables()
            if not tables:
                continue

            # Detect start_page using header
            if start_page is None:
                for t in tables:
                    header = t[0]
                    header_text = " ".join([str(h) if h else "" for h in header]).lower()
                    if "ticker" in header_text or "symbol" in header_text:
                        start_page = i
                        break

            # Detect last_page using table length
            for t in tables:
                if len(t) > min_rows:
                    last_page_with_table = i
                    break

    if start_page is None or last_page_with_table is None:
        raise ValueError("Could not detect listing pages in PDF")

    return start_page, last_page_with_table

# Main extraction function
def extract_tables(pdf_path, save_csv=True, output_dir="./data"):
    """
    Extract and clean tables from USCC PDF with automatic page detection.
    """
    start_page, end_page = detect_listing_pages(pdf_path)
    print(f"Detected tables on pages {start_page} to {end_page}")

    # Extract tables from PDF
    tables = []
    with pdfplumber.open(pdf_path) as pdf:
        for i in range(start_page - 1, end_page):
            page = pdf.pages[i]
            page_tables = page.extract_tables()
            for t in page_tables:
                df = pd.DataFrame(t[1:], columns=t[0][:8])
                tables.append(df)

    if not tables:
        raise ValueError("No tables found in PDF.")

    tables = [df.dropna(how='all') for df in tables if not df.dropna(how='all').empty]

    # Fix header
    header = tables[0].columns.tolist()
    if header[0].strip() == '':
        header[0] = 'ticker'

    standardized_tables = [df.iloc[:, :8].copy() for df in tables]
    for df in standardized_tables:
        df.columns = header

    # Combine all tables
    df_raw = pd.concat(standardized_tables, ignore_index=True)

    # Merge continuation rows
    df_clean = merge_continuation_rows(df_raw, key_col='ticker')

    # Normalize ticker & Exchange
    df_clean["Exchange"] = ""
    df_clean["Symbol"] = df_clean["Symbol"].astype(str)
    mask_hk = df_clean["Symbol"].str.endswith("+HK", na=False)
    df_clean.loc[mask_hk, "Symbol"] = df_clean.loc[mask_hk, "Symbol"].str.replace("+HK", "", regex=False)
    df_clean.loc[mask_hk, "Exchange"] = "HK"
    df_clean = merge_continuation_rows(df_clean, key_col='ticker')

    # Clean numeric columns
    if "Market Cap" in df_clean.columns:
        df_clean["Market Cap"] = clean_numeric_column(df_clean["Market Cap"])
    if "IPO Value" in df_clean.columns:
        df_clean["IPO Value"] = clean_numeric_column(df_clean["IPO Value"])

    # Handle multi-line IPO Month and parse
    if "IPO Month" in df_clean.columns:
        df_clean = fix_multiline_ipo(df_clean, col="IPO Month")
        df_clean["IPO Month"] = (
            df_clean["IPO Month"]
            .astype(str)
            .str.replace(r"[\n\r\t]+", " ", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
            .apply(parse_ipo_month)
        )

    # Remove unwanted symbols
    df_clean = df_clean.applymap(
        lambda x: re.sub(r"§", "", str(x)) if isinstance(x, str) else x
    )

    # Rename columns
    if "Symbol" in df_clean.columns:
        df_clean = df_clean.drop(columns=["ticker"], errors="ignore")
        df_clean = df_clean.rename(columns={
            "Symbol": "ticker",
            "Name": "company_name",
            "Market Cap": "market_cap_usd_mil",
            "IPO Month": "ipo_date",
            "IPO Value": "ipo_value_usd_mil",
            "Lead Underwriter": "lead_underwriters",
            "Exchange": "ticker_hk"
        })

    df_clean.columns = [col.lower() if col.strip().lower() == "sector" else col for col in df_clean.columns]

    # Save CSV
    if save_csv:
        os.makedirs(output_dir, exist_ok=True)
        run_date = datetime.datetime.now().strftime("%Y%m%d")
        output_path = os.path.join(output_dir, f"{run_date}_chinese_companies_USA.csv")
        df_clean.to_csv(output_path, index=False)
        print(f"✅ Saved cleaned CSV to: {output_path}")

    return df_clean

# CLI Entry Point
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract Chinese company listings from USCC PDF.")
    parser.add_argument("--pdf", required=True, help="Path to USCC PDF file")
    parser.add_argument("--out", default="./data", help="Output directory for CSV")
    args = parser.parse_args()

    extract_tables(args.pdf, save_csv=True, output_dir=args.out)
