"""
Usage:
    python scripts/06_parse_ex21_ex8_subs.py
"""
import os, re, json, time, hashlib, requests
from datetime import datetime
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import pandas as pd

# Config
DATA_DIR = "data/intermediate"
OUTPUT_DIR = "companies"
LOG_DIR = "logs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS = {"User-Agent": "First-Name Last-Name email@email.com"} #Enter your information

# Detect latest exhibits_index file + RUN_DATE
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

print(f"[INFO] Using exhibits_index for run_date={RUN_DATE}: {INPUT_FILE}")

with open(INPUT_FILE, "r") as f:
    reports = json.load(f)

# Helpers
def download_file(url):
    time.sleep(0.2)
    session = requests.session()
    r = session.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.content

def clean_text(text):
    return re.sub(r"\s+", " ", text).strip()

def parse_table_in_html(html_text):
    soup = BeautifulSoup(html_text, "html.parser")
    table = soup.find("table")
    subsidiaries = []

    if not table:
        return subsidiaries

    rows = table.find_all("tr")

    # Parse first row for headers
    first_row = [cell.get_text(strip=True).lower() for cell in rows[0].find_all(["th", "td"])]

    fields_keywords = {
        "subsidiary": ("subsidiar", "company", "name"),
        "jurisdiction": ("jurisdiction", "country", "place", "state"),
        "owner": ("owner", "owned")
    }

    field_columns = {
        "subsidiary": [],
        "jurisdiction": [],
        "owner": []
    }

    # Look through first row, record index of headers
    for i, cell in enumerate(first_row):
        if not cell:
            continue

        for field in fields_keywords:
            if any(k in cell for k in fields_keywords[field]):
                field_columns[field].append(i)

    # Need to add conflict resolution for possible column name overlap

    # Add spacy as backup

    for row in rows[1:]:

        values = []

        cols = [clean_text(cell.get_text(strip=True)) for cell in row.find_all(["th", "td"])]

        if not any(col for col in cols):
            continue

        # Get value corresponding to header column index
        for field in field_columns:

            if not field_columns[field]:
                values.append("")
                continue

            for index in field_columns[field]: # This will break if multiple columns are detected for one header
                values.append(cols[index]) # Needs to be fixed by resolving conflicting columns
                
        if values:
            subsidiaries.append(values)

    return (subsidiaries)


# Main Loop
exhibits_index = []
errors_index = []

for entry in reports:
    if entry["exhibit_type"] in ("ex21", "ex8"):
        ticker = entry["ticker"]
        cik10 = entry["cik10"]
        accession = entry["accession"]
        exhibit_label = entry["exhibit_label"]
        year = entry["year"]
        path = entry["localPath"]
        href = entry["href"]

        print(f"[INFO] Processing {ticker} {accession} {exhibit_label}")

        try:
            with open(path, "r", encoding="utf-8") as f:
                html = f.read()
        except OSError as e:
            print(f"[WARN] Failed to read local file {path}: {e}")
            errors_index.append({
                "parent_ticker": ticker,
                "parent_cik10": cik10,
                "accession": accession,
                "exhibit_label": exhibit_label,
                "exhibit_year": year,
                "href": href,
                "source_path": path,
                "error" : f"Failed to read local file {path}: {e}"
            })
            continue

        try:
            subsidiaries = parse_table_in_html(html)
            if not subsidiaries:
                print(f"[INFO] No subsidiaries found in {ticker} {accession} {exhibit_label}")
                continue

            for subsidiary, jurisdiction, ownership in subsidiaries:
                if subsidiary:
                    exhibits_index.append({
                        "parent_ticker": ticker,
                        "parent_cik10": cik10,
                        "accession": accession,
                        "exhibit_label": exhibit_label,
                        "exhibit_year": year,
                        "subsidiary_name_raw": subsidiary,
                        "jurisdiction_raw": jurisdiction,
                        "ownership_raw": ownership,
                        "footnote_marker": "",
                        "source_path": path,
                        "parse_confidence": ""
                    })

        except Exception as e:
            print(f"[Error] An error occurred: {e}")
            errors_index.append({
                "parent_ticker": ticker,
                "parent_cik10": cik10,
                "accession": accession,
                "exhibit_label": exhibit_label,
                "exhibit_year": year,
                "href": href,
                "source_path": path,
                "error" : f"An error occurred: {e}"
            })

exhibit_file = os.path.join(DATA_DIR, f"subs_ex21_ex8_raw_{RUN_DATE}.csv")
df = pd.DataFrame(exhibits_index)
df.to_csv(exhibit_file, index=False, encoding="utf-8")

errors_index_file = os.path.join(LOG_DIR, f"06_errors_{RUN_DATE}.json")
with open(errors_index_file, "w", encoding="utf-8") as f:
    json.dump(errors_index, f, indent=2, ensure_ascii=False)

print(f"[INFO] Wrote {len(exhibits_index)} subidiaries to {exhibit_file}")