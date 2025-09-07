import pdfplumber
import pandas as pd
import datetime
import os

def extract_tables(pdf_path, start_page=8, end_page=22, save_csv=True, output_dir="./data"):
    """
    Extract raw tables from USCC PDF using pdfplumber only,
    collapse split rows, and optionally save as CSV.
    """
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
    tables[0].columns = header

    # Standardize tables
    standardized_tables = []
    for df in tables:
        df.columns = header
        df = df.iloc[:, :8]
        standardized_tables.append(df)

    # Combine all tables
    df_raw = pd.concat(standardized_tables, ignore_index=True)

    # Collapse continuation rows
    clean_rows = []
    buffer = None
    for _, row in df_raw.iterrows():
        if pd.notna(row['ticker']):
            if buffer is not None:
                clean_rows.append(buffer)
            buffer = row.copy()
        else:
            for col in df_raw.columns:
                if pd.notna(row[col]):
                    if pd.isna(buffer[col]):
                        buffer[col] = row[col]
                    else:
                        buffer[col] = str(buffer[col]).strip() + ' ' + str(row[col]).strip()
    if buffer is not None:
        clean_rows.append(buffer)

    df_clean = pd.DataFrame(clean_rows).reset_index(drop=True)

    # --- Save CSV if requested ---
    if save_csv:
        os.makedirs(output_dir, exist_ok=True)
        RUN_DATE = datetime.datetime.now().strftime("%Y%m%d")
        output_path = os.path.join(output_dir, f"{RUN_DATE}_chinese_companies_USA.csv")
        df_clean.to_csv(output_path, index=False)
        print(f"Saved cleaned CSV to: {output_path}")

    return df_clean
