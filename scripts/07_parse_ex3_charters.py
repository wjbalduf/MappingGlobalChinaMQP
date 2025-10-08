import os
import re
import json
import csv
from datetime import datetime
from bs4 import BeautifulSoup
import pdfplumber

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data", "intermediate")
os.makedirs(DATA_DIR, exist_ok=True)

# Helper Function - locate the latest exhibits_index JSON
def get_latest_exhibits_index():
    files = [f for f in os.listdir(DATA_DIR) if f.startswith("exhibits_index_") and f.endswith(".json")]
    if not files:
        raise FileNotFoundError("No exhibits_index_*.json found in data/intermediate")

    dated_files = []
    for f in files:
        m = re.search(r"exhibits_index_(\d{8})\.json", f)
        if m:
            try:
                run_date = datetime.strptime(m.group(1), "%Y%m%d")
                dated_files.append((run_date, f))
            except ValueError:
                continue

    if not dated_files:
        raise ValueError("No valid exhibits_index_*.json files with YYYYMMDD format")

    latest_date, latest_file = max(dated_files, key=lambda x: x[0])
    return latest_date.strftime("%Y%m%d"), os.path.join(DATA_DIR, latest_file)

# Load the latest index
RUN_DATE, INPUT_FILE = get_latest_exhibits_index()
print(f"Using exhibits_index for run_date={RUN_DATE}: {INPUT_FILE}")

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    exhibits = json.load(f)

# Parsing Functions
def clean_text(txt: str):
    return re.sub(r"\s+", " ", txt).strip()

def parse_html_ex3(filepath: str):
    rows = []
    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            soup = BeautifulSoup(f, "html.parser")
        for line in soup.get_text("\n").split("\n"):
            line = clean_text(line)
            if line:
                rows.append(line)
    except Exception as e:
        print(f"Failed to parse HTML: {filepath} — {e}")
    return rows

def parse_pdf_ex3(filepath: str):
    rows = []
    try:
        with pdfplumber.open(filepath) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue
                for line in text.split("\n"):
                    line = clean_text(line)
                    if line:
                        rows.append(line)
    except Exception as e:
        print(f"Failed to parse PDF: {filepath} — {e}")
    return rows

# Address extraction
def extract_addresses(lines):
    results = []
    current_block = []
    current_type = None
    extract = False
    line_limit = 5

    for line in lines:
        lower = line.lower().strip()

        # Start of an address
        if any(key in lower for key in ["registered office", "principal office", "principal executive", "c/o", "agent"]):
            if current_block:
                # Save previous
                results.append({
                    "address_raw": " ".join(current_block),
                    "address_type": current_type or "other"
                })
            # Type of address
            current_type = (
                "registered_office" if "registered office" in lower else
                "principal_office" if "principal" in lower else
                "agent_address" if "c/o" in lower or "agent" in lower else
                "other"
            )
            # New extraction
            current_block = [line]
            extract = True
            continue

        # Stop extract when end of address occurs
        if extract:
            if re.match(r"^\d{1,2}\s*$", line) or any(
                kw in lower for kw in ["the company", "articles", "section", "act (revised)"]
            ):
                extract = False
                results.append({
                    "address_raw": " ".join(current_block),
                    "address_type": current_type or "other"
                })
                current_block, current_type = [], None
            elif len(current_block) < line_limit:
                current_block.append(line)
            else:
                extract = False
                results.append({
                    "address_raw": " ".join(current_block),
                    "address_type": current_type or "other"
                })
                current_block, current_type = [], None

    # Save remaining address blocks
    if current_block:
        results.append({
            "address_raw": " ".join(current_block),
            "address_type": current_type or "other"
        })

    return results

# Main extraction process
def main():
    rows_out = []

    for ex in exhibits:
        if ex.get("exhibit_type") != "ex3":
            continue

        ticker = ex.get("ticker")
        cik10 = ex.get("cik10")
        accession = ex.get("accession")
        year = ex.get("year")
        source_path = os.path.abspath(os.path.join(BASE_DIR, ex.get("localPath", "")))

        if not os.path.exists(source_path):
            print(f"File not found, skipping: {source_path}")
            continue

        print(f"Parsing EX-3 for {ticker} {accession}")

        # Parse based on file type
        if source_path.endswith((".htm", ".html")):
            parsed_lines = parse_html_ex3(source_path)
            confidence = "high" if parsed_lines else "low"
        elif source_path.endswith(".pdf"):
            parsed_lines = parse_pdf_ex3(source_path)
            confidence = "medium" if parsed_lines else "low"
        else:
            parsed_lines, confidence = [], "ocr_needed"

        # Extract and store addresses
        for addr in extract_addresses(parsed_lines):
            rows_out.append({
                "parent_ticker": ticker,
                "parent_cik10": cik10,
                "accession": accession,
                "exhibit_year": year,
                "address_raw": addr["address_raw"],
                "address_type": addr["address_type"],
                "source_path": source_path,
                "parse_confidence": confidence,
            })

    # Write output
    csv_path = os.path.join(DATA_DIR, f"charter_addresses_raw_{RUN_DATE}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "parent_ticker", "parent_cik10", "accession", "exhibit_year",
            "address_raw", "address_type", "source_path", "parse_confidence"
        ])
        writer.writeheader()
        writer.writerows(rows_out)

    print(f"Wrote {len(rows_out)} address rows to {csv_path}")

if __name__ == "__main__":
    main()
