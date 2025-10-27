"""
Usage:
    python scripts/06_parse_ex21_subs.py
"""
import os, re, json, time, hashlib, requests
from datetime import datetime
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import pandas as pd

# Config
DATA_DIR = "data/intermediate"
OUTPUT_DIR = "companies"
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

    if not rows:
        return subsidiaries

    # Parse first row for headers
    first_row = [cell.get_text(strip=True).lower() for cell in rows[0].find_all(["th", "td"])]

    fields_keywords = {
        "subsidiary": ("subsidiar", "company", "name"),
        "jurisdiction": ("jurisdiction", "country", "place", "state", "incorporated"),
        "owner": ("owner", "owned", "percentage", "%")
    }

    # Score each column to determine best match for each field
    def score_column(header_text, field):
        score = 0
        for keyword in fields_keywords[field]:
            if keyword in header_text:
                # Give higher score for more specific matches
                if field == "subsidiary" and "subsidiar" in header_text:
                    score += 10
                elif field == "jurisdiction" and ("jurisdiction" in header_text or "incorporated" in header_text):
                    score += 10
                elif field == "owner" and ("owner" in header_text or "%" in header_text):
                    score += 10
                else:
                    score += 1
        return score

    # Find best column for each field
    field_column_map = {}
    for field in fields_keywords:
        best_col = -1
        best_score = 0
        for i, header in enumerate(first_row):
            if header:
                score = score_column(header, field)
                if score > best_score:
                    best_score = score
                    best_col = i
        if best_col >= 0:
            field_column_map[field] = best_col

    # Parse data rows
    for row in rows[1:]:
        cols = [clean_text(cell.get_text(strip=True)) for cell in row.find_all(["th", "td"])]

        if not any(col for col in cols):
            continue

        # Extract values based on identified columns
        subsidiary_name = ""
        jurisdiction = ""
        ownership = ""

        if "subsidiary" in field_column_map and field_column_map["subsidiary"] < len(cols):
            subsidiary_name = cols[field_column_map["subsidiary"]]

        if "jurisdiction" in field_column_map and field_column_map["jurisdiction"] < len(cols):
            jurisdiction = cols[field_column_map["jurisdiction"]]

        if "owner" in field_column_map and field_column_map["owner"] < len(cols):
            ownership = cols[field_column_map["owner"]]

        # Only add if we have at least a subsidiary name
        if subsidiary_name:
            subsidiaries.append({
                "subsidiary": subsidiary_name,
                "jurisdiction": jurisdiction,
                "ownership": ownership
            })

    return subsidiaries


# Main Loop
ex21_index = []

for entry in reports:
    if entry["exhibit_type"] == "ex21":
        ticker = entry["ticker"]
        cik10 = entry["cik10"]
        accession = entry["accession"]
        exhibit_label = entry["exhibit_label"]
        year = entry["year"]
        path = entry["localPath"]

        print(f"[INFO] Processing {ticker} {accession} {exhibit_label}")

        # First try to use local file if it exists
        html_content = None
        local_path = entry.get("localPath", "")

        if local_path and os.path.exists(local_path):
            try:
                with open(local_path, "rb") as f:
                    html_content = f.read()
                print(f"[INFO] Using local file: {local_path}")
            except Exception as e:
                print(f"[WARN] Failed to read local file {local_path}: {e}")

        # If local file doesn't exist or failed to read, download from web
        if html_content is None:
            try:
                html_content = download_file("https://www.sec.gov" + entry["href"])
                print(f"[INFO] Downloaded from web: {entry['href']}")
            except requests.RequestException as e:
                print(f"[WARN] Failed to download exhibit {entry['href']}: {e}")
                continue

        # Parse the HTML content
        if isinstance(html_content, bytes):
            html_content = html_content.decode("utf-8", errors="ignore")

        subsidiaries = parse_table_in_html(html_content)
        if not subsidiaries:
            print(f"[INFO] No subsidiaries found in {ticker} {accession} {exhibit_label}")
            continue

        for sub_data in subsidiaries:
            ex21_index.append({
                "parent_ticker": ticker,
                "parent_cik10": cik10,
                "accession": accession,
                "exhibit_label": exhibit_label,
                "exhibit_year": year,
                "subsidiary_name_raw": sub_data.get("subsidiary", ""),
                "jurisdiction_raw": sub_data.get("jurisdiction", ""),
                "ownership_raw": sub_data.get("ownership", ""),
                "footnote_marker": "",
                "source_path": path,
            })

ex21_file = os.path.join(DATA_DIR, f"subs_ex21_raw_{RUN_DATE}.csv")
df = pd.DataFrame(ex21_index)
df.to_csv(ex21_file, index=False, encoding="utf-8")

print(f"[INFO] Wrote {len(ex21_index)} exhibits to {ex21_file}")