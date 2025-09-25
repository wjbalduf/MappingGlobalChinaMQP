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

    first_row = rows[0].find_all(["th", "td"])

    fields_keywords = {
        "subsidiary": ("subsidiary", "subsidiaries", "name"),
        "jurisdiction": ("jurisdiction", "country", "place", "state"),
        "observer": ("observer", "observed")
    }

    has_sub = any(k in cell for cell in first_row for k in fields_keywords["subsidiary"])
    has_jur = any(k in cell for cell in first_row for k in fields_keywords["jurisdiction"])
    has_owner = any(k in cell for cell in first_row for k in fields_keywords["observer"])



    for row in rows:

        cols = row.find_all("td")

        subsidiary = clean_text(cols[0].get_text(strip=True)) if len(cols) > 0 else ""
        jurisdiction = clean_text(cols[1].get_text(strip=True)) if len(cols) > 1 else ""
        ownership = clean_text(cols[2].get_text(strip=True)) if len(cols) > 2 else ""

        if not any([subsidiary, jurisdiction, ownership]):
            continue

        subsidiaries.append((subsidiary, jurisdiction, ownership))

    return subsidiaries

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

        try:
            html = download_file("https://www.sec.gov" + entry["href"])
        except requests.RequestException as e:
            print(f"[WARN] Failed to download exhibit {entry["href"]}: {e}")
            continue

        subsidiaries =  parse_table_in_html(html)
        if not subsidiaries:
            print(f"[INFO] No subsidiaries found in {ticker} {accession} {exhibit_label}")
            continue

        for subsidiary, jurisdiction, ownership in subsidiaries:
            ex21_index.append({
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

ex21_file = os.path.join(DATA_DIR, f"subs_ex21_raw_{RUN_DATE}.csv")
df = pd.DataFrame(ex21_index)
df.to_csv(ex21_file, index=False, encoding="utf-8")

print(f"[INFO] Wrote {len(ex21_index)} exhibits to {ex21_file}")