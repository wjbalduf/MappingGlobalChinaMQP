import os
import re
import json
import csv
from datetime import datetime
from bs4 import BeautifulSoup
import pdfplumber
import spacy
from collections import defaultdict
import string

# Load English models for spacy
# python -m spacy download en_core_web_sm
# Not used because it's too lenient in what it can consider an address, find another purpose for spacy
nlp_en = spacy.load("en_core_web_sm")

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
        # Extract both normal text and <u> text
        text_blocks = [clean_text(t) for t in soup.get_text("\n").split("\n") if clean_text(t)]
        u_blocks = [clean_text(u.get_text()) for u in soup.find_all("u") if clean_text(u.get_text())]
        rows = list(dict.fromkeys(text_blocks + u_blocks)) 
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
                rows.extend([clean_text(l) for l in text.split("\n") if clean_text(l)])
        rows = list(dict.fromkeys(rows))
    except Exception as e:
        print(f"Failed to parse PDF: {filepath} — {e}")
    return rows

# Helper to detect address type based on context
def classify_address_type(line):
    line_lower = line.lower()

    if "registered office" in line_lower or "registered in" in line_lower or "incorporated in" in line_lower:
        return "registered_office"
    elif "principal executive office" in line_lower or "principal office" in line_lower:
        return "principal_office"
    elif "agent" in line_lower or "representative" in line_lower:
        return "agent_address"
    else:
        return "other"

# Find potential addresses in the files
def potential_address(line):
    line = line.strip()
    line_lower = line.lower()

    # Exclude some legal/company keywords, probably not addresses 
    exclusions = [
        "identity card", "cik", "ticker", "prc identity", "prc id", 
        "section", "act", "agreement", "party",
        "shareholder", "company", "director", "member",
        "notice", "hereby", "therefore", "shall", "exhibit"
    ]
    if any(kw in line_lower for kw in exclusions):
        return False
    
    # Common address patterns to catch longer lines
    address_patterns = [
        r'address at .*',
        r'registered in .*',
        r'located at .*',
        r'no\.?\s*\d+',
    ]
    if any(re.search(pat, line_lower) for pat in address_patterns):
        return True

    # Common address patterns
    has_number = bool(re.search(r'\b(?:no\.?|room|suite|unit|floor|building|apt|#)\s*\d+', line_lower))
    has_postal = bool(re.search(r'\b\d{5,6}(-\d{4})?\b', line))

    # Street keywords
    street_keywords = [
        "street", "st.", "road", "rd.", "avenue", "ave", "lane", "ln",
        "boulevard", "blvd", "drive", "dr.", "court", "ct.", "square", "sq",
        "circle", "plaza", "terrace", "trail", "way"
    ]
    has_street = any(kw in line_lower for kw in street_keywords)

    # City/region keywords, add on if needed
    city_keywords = [
        "beijing", "shanghai", "hangzhou", "ningbo", "hong kong", "singapore",
        "cayman", "ky1", "prc"
    ]
    has_city = any(kw in line_lower for kw in city_keywords)

    # Requirements to be an address
    if (has_number or has_postal) and (has_street or has_city):
        return True
    if has_city and len(line.split()) <= 6: # potentially address if its a city thats a short line
        return True

    return False

def extract_addresses(lines):
    results = []
    seen = set()  # Keep track to avoid duplicates
    current_block = [] # Store address lines

    for line in lines:
        line_clean = clean_text(line)
        if not line_clean:
            continue

        # If line looks like an address, add to current block
        if potential_address(line_clean):
            current_block.append(line_clean)
        else:
            # End of a block
            if current_block:
                block_text = " ".join(current_block)  # combine into one block
                if block_text.lower() not in seen:
                    addr_type = classify_address_type(block_text)
                    results.append({"address_raw": block_text, "address_type": addr_type})
                    seen.add(block_text.lower())
                current_block = []

    # Leftover blocks at the end of file are extracted
    if current_block:
        block_text = " ".join(current_block)
        if block_text.lower() not in seen:
            addr_type = classify_address_type(block_text)
            results.append({"address_raw": block_text, "address_type": addr_type})
            seen.add(block_text.lower())

    return results

# Very specific to the results from our EX3s, potentially add on? or we keep the duplicates
canonical_map = {
    "the cayman islands": "Cayman Islands",
    "territory of the cayman islands": "Cayman Islands",
    "hong kong, people's republic of china": "Hong Kong"
}

# Helper for deduplication
def normalize_address(addr):
    addr_clean = addr.lower().strip().translate(str.maketrans("", "", string.punctuation))
    return canonical_map.get(addr_clean, addr.strip())

# Main extraction process
def main():
    company_seen_addresses = {} 
    rows_out = []

    for ex in exhibits:
        if ex.get("exhibit_type") != "ex3":
            continue

        ticker = ex.get("ticker")
        cik10 = ex.get("cik10")
        accession = ex.get("accession")
        exhibit_label = ex.get("exhibit_label")
        year = ex.get("year")
        source_path = os.path.abspath(os.path.join(BASE_DIR, ex.get("localPath", "")))

        if not os.path.exists(source_path):
            print(f"File not found, skipping: {source_path}")
            continue

        print(f"Parsing EX-3 for {ticker} {accession} {exhibit_label}")

        # Confidence
        if source_path.endswith((".htm", ".html")):
            parsed_lines = parse_html_ex3(source_path)
            file_type = "html" if parsed_lines else "ocr_needed"
        elif source_path.endswith(".pdf"):
            parsed_lines = parse_pdf_ex3(source_path)
            file_type = "html" if parsed_lines else "ocr_needed"
        else:
            parsed_lines, file_type = [], "ocr_needed"

        # Extract
        addresses = extract_addresses(parsed_lines)
        if not addresses:
            addresses = [{"address_raw": "", "address_type": "other"}]

        # Track addresses per company
        company_key = (ticker, cik10)
        if company_key not in company_seen_addresses:
            company_seen_addresses[company_key] = set()

        for addr in addresses:
            addr_lower = addr["address_raw"].lower()
            if addr_lower not in company_seen_addresses[company_key]:
                company_seen_addresses[company_key].add(addr_lower)
                rows_out.append({
                    "parent_ticker": ticker,
                    "parent_cik10": cik10,
                    "accession": accession,
                    "exhibit_label": exhibit_label,
                    "exhibit_year": year,
                    "address_raw": addr["address_raw"],
                    "address_type": addr["address_type"],
                    "source_path": source_path,
                    "parse_confidence": file_type,
                })

    # Deduplicate addresses per company
    deduped_rows = []
    seen_addresses = defaultdict(set)

    for row in rows_out:
        ticker = row["parent_ticker"]
        norm_addr = normalize_address(row["address_raw"])
        if norm_addr.lower() not in seen_addresses[ticker]:
            row["address_raw"] = norm_addr  # overwrite with normalized address
            deduped_rows.append(row)
            seen_addresses[ticker].add(norm_addr.lower())

    # Write to csv
    csv_path = os.path.join(DATA_DIR, f"charter_addresses_raw_{RUN_DATE}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "parent_ticker", "parent_cik10", "accession", "exhibit_label", "exhibit_year",
            "address_raw", "address_type", "source_path", "parse_confidence"
        ])
        writer.writeheader()
        writer.writerows(deduped_rows)

    print(f"Wrote {len(deduped_rows)} address rows to {csv_path}")

if __name__ == "__main__":
    main()
