import os, re, json, csv
from datetime import datetime
from bs4 import BeautifulSoup
import pdfplumber

# Code taken from 06 and edited slightly
# Directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data", "intermediate")
os.makedirs(DATA_DIR, exist_ok=True)

# Latest exhibits_index file provided by task 5
def get_latest_exhibits_index():
    files = [f for f in os.listdir(DATA_DIR) if f.startswith("exhibits_index_") and f.endswith(".json")]
    if not files:
        raise FileNotFoundError("No exhibits_index_*.json found in data/intermediate")

    dated_files = []
    for f in files:
        m = re.search(r"exhibits_index_(\d{8})\.json", f)
        if m:
            run_date = datetime.strptime(m.group(1), "%Y%m%d")
            dated_files.append((run_date, f))
    if not dated_files:
        raise ValueError("No valid exhibits_index_*.json with YYYYMMDD in filename")

    latest_date, latest_file = max(dated_files, key=lambda x: x[0])
    return latest_date.strftime("%Y%m%d"), os.path.join(DATA_DIR, latest_file)

RUN_DATE, INPUT_FILE = get_latest_exhibits_index()

print(f"Using exhibits_index for run_date={RUN_DATE}: {INPUT_FILE}")

# Load json from task 5
with open(INPUT_FILE, "r") as f:
    exhibits = json.load(f)

# Helper functions

def clean_text(txt):
    return re.sub(r"\s+", " ", txt).strip()

def parse_html_ex3(filepath):
    rows = []
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        soup = BeautifulSoup(f, "html.parser")

    text = soup.get_text("\n")
    for line in text.split("\n"):
        line = clean_text(line)
        if line:
            rows.append(line)
    return rows

def parse_pdf_ex3(filepath):
    rows = []
    try:
        with pdfplumber.open(filepath) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    for line in text.split("\n"):
                        line = clean_text(line)
                        if line:
                            rows.append(line)
    except Exception as e:
        print(f"[Error] PDF parse failed for {filepath}: {e}")
    return rows

# Get addresses
def extract_addresses(lines):
    results = []
    current_address_block = []
    current_type = None

    # Case-insensitive
    for line in lines:
        lowercase = line.lower()

        # Start new block
        if "registered office" in lowercase:
            if current_address_block:
                results.append({"address_raw": "\n".join(current_address_block), "address_type": current_type})
                current_address_block = []
            current_type = "registered_office"
            current_address_block.append(line)

        elif "principal executive" in lowercase or "principal office" in lowercase:
            if current_address_block:
                results.append({"address_raw": "\n".join(current_address_block), "address_type": current_type})
                current_address_block = []
            current_type = "principal_office"
            current_address_block.append(line)

        elif "c/o" in lowercase or "agent" in lowercase:
            if current_address_block:
                results.append({"address_raw": "\n".join(current_address_block), "address_type": current_type})
                current_address_block = []
            current_type = "agent_address"
            current_address_block.append(line)

        else:
            if current_type:
                current_address_block.append(line)

    # Append to results & clear
    if current_address_block:
        results.append({"address_raw": "\n".join(current_address_block), "address_type": current_type or "other"})

    return results

# Main
rows_out = []

for ex in exhibits:
    if ex["exhibit_type"] != "ex3":
        continue

    ticker = ex["ticker"]
    cik10 = ex["cik10"]
    accession = ex["accession"]
    year = ex["year"]
    label = ex["exhibit_label"]
    source_path = os.path.join(BASE_DIR, ex["localPath"])
    source_path = os.path.abspath(source_path)

    if not os.path.exists(source_path):
        print(f"[Error] File not found, skipping: {source_path}")
        continue

    print(f"Parsing EX-3 for {ticker} {accession} ({source_path})")

    # Confidence levels
    parsed_lines = []
    if source_path.endswith((".htm", ".html")):
        parsed_lines = parse_html_ex3(source_path)
        confidence = "high" if parsed_lines else "low"
    elif source_path.endswith(".pdf"):
        parsed_lines = parse_pdf_ex3(source_path)
        confidence = "medium" if parsed_lines else "low"
    else:
        confidence = "ocr_needed" # need to impkemnent OCR

    addresses = extract_addresses(parsed_lines)
    for addr in addresses:

        # Append
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

# Outputs
csv_file = os.path.join(DATA_DIR, f"charter_addresses_raw_{RUN_DATE}.csv")
with open(csv_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=[
        "parent_ticker","parent_cik10","accession","exhibit_year",
        "address_raw","address_type","source_path","parse_confidence"
    ])
    writer.writeheader()
    writer.writerows(rows_out)

print(f"Wrote {len(rows_out)} address rows to {csv_file}")
