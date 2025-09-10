import os
import re
from bs4 import BeautifulSoup
import pandas as pd

# Base folder where all company folders live
BASE_DIR = r"C:\Users\silly\OneDrive\School\2025_Fall\MQP\MappingGlobalChinaMQP\Task_2_3\companies"

def extract_text_from_html(filepath):
    """Extracts clean text from a single HTML file."""
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        soup = BeautifulSoup(f, "html.parser")
    # Remove scripts, styles, and tables
    for tag in soup(["script", "style", "table"]):
        tag.decompose()
    return soup.get_text(separator=" ")

records = []

# Walk through subfolders
for root, _, files in os.walk(BASE_DIR):
    for file in files:
        if file.endswith(".html"):
            filepath = os.path.join(root, file)

            # Try to infer metadata from folder and filename
            company = os.path.basename(root)  # folder = company ticker/name
            # Extract year from filename if possible
            match = re.search(r"(20\d{2})", file)
            year = match.group(1) if match else None

            text = extract_text_from_html(filepath)
            records.append({
                "company": company,
                "year": year,
                "file": file,
                "text": text
            })

# Save into DataFrame
df = pd.DataFrame(records)
print(df.head())

# Optional: save to CSV for reference
df.to_csv("all_filings_extracted.csv", index=False)
