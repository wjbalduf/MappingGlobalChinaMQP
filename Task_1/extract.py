import pdfplumber
import pandas as pd
import datetime
import os

# ------------------------------
# Helper functions
# ------------------------------

def clean_numeric_column(series):
    """
    Convert numeric strings with $/commas/n/a to pandas nullable integer.
    """
    return (
        series
        .replace(r"[\$,]", "", regex=True)   # remove $ and commas
        .replace(r"n/?a", None, regex=True) # treat 'n/a' as NaN
        .astype(float)
        .astype("Int64")                     # nullable integer type
    )

def merge_continuation_rows(df, key_col='ticker'):
    """
    Merge rows where the key column is NaN/empty into the previous row.
    Handles multi-line cells.
    """
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

def parse_ipo_month(val):
    """
    Convert IPO Month string to YYYY-MM format.
    Handles multi-line, hyphenated, and year-first formats.
    """
    if pd.isna(val) or val.strip() == "":
        return None
    val = val.replace("-", " ").strip()
    try:
        return pd.to_datetime(val, errors="raise").strftime("%Y-%m")
    except Exception:
        parts = val.split()
        if len(parts) == 2 and parts[0].isdigit():
            val = f"{parts[1]} {parts[0]}"
            try:
                return pd.to_datetime(val, errors="raise").strftime("%Y-%m")
            except Exception:
                return None
        return None

# ------------------------------
# Main extraction function
# ------------------------------

def extract_tables(pdf_path, start_page=8, end_page=22, save_csv=True, output_dir="./data"):
    """
    Extract and clean tables from USCC PDF.
    Steps:
      - Extract tables with pdfplumber
      - Merge continuation rows
      - Normalize ticker/Exchange
      - Clean numeric columns (Market Cap, IPO Value)
      - Normalize IPO Month
      - Lowercase 'Sector' column
      - Save as CSV (optional)
    """
    # ------------------------------
    # 1. Extract tables from PDF
    # ------------------------------
    tables = []
    with pdfplumber.open(pdf_path) as pdf:
        for i in range(start_page - 1, end_page):
            page = pdf.pages[i]
            page_tables = page.extract_tables()
            for t in page_tables:
                df = pd.DataFrame(t[1:], columns=t[0][:8])  # keep first 8 columns
                tables.append(df)

    if not tables:
        raise ValueError("No tables found in PDF.")

    # Drop fully empty rows
    tables = [df.dropna(how='all') for df in tables if not df.dropna(how='all').empty]

    # Fix header: first column empty → 'ticker'
    header = tables[0].columns.tolist()
    if header[0].strip() == '':
        header[0] = 'ticker'

    # Standardize columns for all tables
    standardized_tables = [df.iloc[:, :8].copy() for df in tables]
    for df in standardized_tables:
        df.columns = header

    # Combine all tables
    df_raw = pd.concat(standardized_tables, ignore_index=True)

    # ------------------------------
    # 2. Merge continuation rows
    # ------------------------------
    df_clean = merge_continuation_rows(df_raw, key_col='ticker')

    # ------------------------------
    # 3. Normalize ticker & Exchange
    # ------------------------------
    df_clean["Exchange"] = ""  # default empty
    df_clean["Symbol"] = df_clean["Symbol"].astype(str)
    mask_hk = df_clean["Symbol"].str.endswith("+HK", na=False)
    df_clean.loc[mask_hk, "Symbol"] = df_clean.loc[mask_hk, "Symbol"].str.replace("+HK", "", regex=False)
    df_clean.loc[mask_hk, "Exchange"] = "HK"

    # Merge again in case multi-line symbols exist
    df_clean = merge_continuation_rows(df_clean, key_col='ticker')

    # ------------------------------
    # 4. Clean numeric columns
    # ------------------------------
    if "Market Cap" in df_clean.columns:
        df_clean["Market Cap"] = clean_numeric_column(df_clean["Market Cap"])
    if "IPO Value" in df_clean.columns:
        df_clean["IPO Value"] = clean_numeric_column(df_clean["IPO Value"])

    # ------------------------------
    # 5. Normalize IPO Month
    # ------------------------------
    if "IPO Month" in df_clean.columns:
        df_clean["IPO Month"] = (
            df_clean["IPO Month"]
            .astype(str)
            .str.replace(r"[\n\r\t]+", " ", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
            .apply(parse_ipo_month)
        )

    # ------------------------------
    # 6. Rename columns and lowercase Sector
    # ------------------------------
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

    # Lowercase 'Sector' column if present
    df_clean.columns = [col.lower() if col.strip().lower() == "sector" else col for col in df_clean.columns]

    # ------------------------------
    # 7. Save CSV if requested
    # ------------------------------
    if save_csv:
        os.makedirs(output_dir, exist_ok=True)
        RUN_DATE = datetime.datetime.now().strftime("%Y%m%d")
        output_path = os.path.join(output_dir, f"{RUN_DATE}_chinese_companies_USA.csv")
        df_clean.to_csv(output_path, index=False)
        print(f"Saved cleaned CSV to: {output_path}")

    return df_clean