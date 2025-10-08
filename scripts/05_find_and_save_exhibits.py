"""
Usage:
    python scripts/05_find_and_save_exhibits.py
"""
import os, re, json, time, hashlib, requests
from datetime import datetime
from urllib.parse import urljoin
from bs4 import BeautifulSoup

# Config
DATA_DIR = "data/intermediate"
OUTPUT_DIR = "companies"
os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS = {"User-Agent": "William Balduf silly12billy@gmail.com"} #Enter your information

# Detect latest annual_reports_index file + RUN_DATE
def get_latest_annual_reports_index():
    latest_file = os.path.join(DATA_DIR, "annual_reports_index.json")
    if not os.path.exists(latest_file):
        raise FileNotFoundError("No annual_reports_index.json found in data/intermediate")
    # Use today's date or "unknown" as RUN_DATE since the file has no date
    run_date = datetime.today().strftime("%Y%m%d")
    return run_date, latest_file


RUN_DATE, INPUT_FILE = get_latest_annual_reports_index()

print(f"[INFO] Using annual_reports_index for run_date={RUN_DATE}: {INPUT_FILE}")

with open(INPUT_FILE, "r") as f:
    reports = json.load(f)

# Helpers
def sha256_bytes(content):
    return hashlib.sha256(content).hexdigest()

def download_file(url):
    time.sleep(0.2)
    session = requests.session()
    r = session.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.content

def find_exhibits_in_html(html_text):
    soup = BeautifulSoup(html_text, "html.parser")
    table = soup.find("table")
    exhibits = []

    if not table:
        return exhibits
    
    ex_pattern = re.compile(r"EX-(21|3)(\.\d+)?$", re.IGNORECASE)

    for row in table.find_all("tr")[1:]:
        cols = row.find_all("td")
        if len(cols) < 4:
            continue

        doc_type = cols[3].get_text(strip=True)
        href_tag = cols[2].find("a")
        href = href_tag["href"] if href_tag else None
        if not href:
            continue

        if ex_pattern.match(doc_type):
            exhibits.append((href, doc_type))

    return exhibits

exhibits_index = []

# Main Loop
for report in reports:
    ticker = report["ticker"]
    cik10 = report["cik10"]
    accession = report["accession"]
    year = report["year"]
    form = report["form"]
    filing_url = report["filing_url"]

    print(f"[INFO] Processing {ticker} {accession}")

    # Download primary doc
    try:
        index_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik10)}/{accession.replace('-', '')}/{accession}-index.htm"
        content = download_file(index_url)
    except requests.RequestException as e:
        print(f"[WARN] Failed to download {index_url}: {e}")
        continue

    html_text = content.decode("utf-8", errors="ignore")

    # Look for EX-21 or EX-3
    found_exhibits = find_exhibits_in_html(html_text)
    if not found_exhibits:
        print(f"[INFO] No EX-21 or EX-3 found in {ticker} {accession}")
        continue

    for href, label in found_exhibits:
        file_url = urljoin(index_url, href)
        try:
            file_content = download_file(file_url)
        except requests.RequestException as e:
            print(f"[WARN] Failed to download exhibit {href}: {e}")
            continue

        ext = os.path.splitext(href)[1]
        if ext not in (".htm", ".html", ".pdf"):
            ext = ".html"

        local_dir = os.path.join(OUTPUT_DIR, ticker, "exhibits", accession)
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, f"{label}{ext}")

        with open(local_path, "wb") as f:
            f.write(file_content)

        exhibits_index.append({
            "ticker": ticker,
            "cik10": cik10,
            "year": year,
            "accession": accession,
            "exhibit_type": "ex21" if "EX-21" in label.upper() else "ex3",
            "exhibit_label": label,
            "href": href,
            "localPath": local_path,
            "sha256": sha256_bytes(file_content),
            "bytes": len(file_content),
            "discovered_in": form
        })

exhibits_index_file = os.path.join(DATA_DIR, f"exhibits_index_{RUN_DATE}.json")
with open(exhibits_index_file, "w", encoding="utf-8") as f:
    json.dump(exhibits_index, f, indent=2, ensure_ascii=False)

print(f"[INFO] Wrote {len(exhibits_index)} exhibits to {exhibits_index_file}")