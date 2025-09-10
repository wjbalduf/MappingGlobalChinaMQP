import pandas as pd
from bertopic import BERTopic
from sentence_transformers import SentenceTransformer
import matplotlib.pyplot as plt
import seaborn as sns
import re
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

# ------------------------------
# 1. Load CSV
# ------------------------------
df = pd.read_csv(r"C:\Users\silly\OneDrive\School\2025_Fall\MQP\MappingGlobalChinaMQP\Task_2_3\companies\all_filings_extracted.csv")

# Check columns
print("Columns in CSV:", df.columns)
if 'text' not in df.columns:
    raise ValueError("'text' column not found in CSV!")

# ------------------------------
# 2. Clean text to remove stopwords and noise
# ------------------------------
def clean_text(text):
    text = str(text).lower()                          # lowercase
    text = re.sub(r'\s+', ' ', text)                  # remove extra spaces/newlines
    text = re.sub(r'\d+', '', text)                   # remove numbers
    text = re.sub(r'[^a-z ]', '', text)               # remove punctuation/symbols
    text = ' '.join([w for w in text.split() if w not in ENGLISH_STOP_WORDS])  # remove stopwords
    return text

df['clean_text'] = df['text'].fillna("").apply(clean_text)
docs = df['clean_text'].tolist()

# ------------------------------
# 3. Embed documents using SentenceTransformer
# ------------------------------
emb_model = SentenceTransformer("all-MiniLM-L6-v2")
embeddings = emb_model.encode(docs, show_progress_bar=True)

# ------------------------------
# 4. Train BERTopic with cleaned text
# ------------------------------
topic_model = BERTopic(language="english")
topics, probs = topic_model.fit_transform(docs, embeddings)

# ------------------------------
# 5. Attach topics to DataFrame
# ------------------------------
df["topic"] = topics

# ------------------------------
# 6. Inspect topic info
# ------------------------------
print(topic_model.get_topic_info().head(10))

# ------------------------------
# 7. Count topics by company
# ------------------------------
topic_counts = df.groupby(["company", "topic"]).size().reset_index(name="count")
top_topics = topic_counts.sort_values(["company","count"], ascending=[True,False])
print(top_topics.head(20))

# ------------------------------
# 8. Bar plot: Top topics for one company
# ------------------------------
company = "BABA"  # Change to your company
subset = topic_counts[topic_counts["company"] == company].nlargest(5, "count")

plt.figure(figsize=(8,5))
plt.barh(subset["topic"].astype(str), subset["count"])
plt.title(f"Top Topics for {company}")
plt.xlabel("Count")
plt.ylabel("Topic")
plt.show()

# ------------------------------
# 9. Heatmap: Compare topics across all companies
# ------------------------------
pivot = topic_counts.pivot(index="company", columns="topic", values="count").fillna(0)

plt.figure(figsize=(12,6))
sns.heatmap(pivot, cmap="Blues", annot=True, fmt="g")
plt.title("Topic Distribution by Company")
plt.show()

# ------------------------------
# 10. Define geopolitical keywords
# ------------------------------
geopolitical_keywords = {
    "China": ["china", "prc", "chinese"],
    "United States": ["united states", "us", "usa", "america", "american"],
    "Europe": ["europe", "eu", "european"],
    "Russia": ["russia", "russian"],
    "India": ["india", "indian"],
    "Latin America": ["latin america", "brazil", "mexico", "argentina"],
    "Africa": ["africa", "nigeria", "south africa", "ghana"],
    
    "Trade & Policy": ["tariff", "sanction", "export", "import", "trade", "regulation", "policy", "government", "compliance"],
    "Risk & Security": ["geopolitical", "political", "sovereignty", "national security", "conflict", "war", "military", "restriction", "censorship"],
    "Hot Topics": ["supply chain", "decoupling", "technology transfer", "intellectual property", "data privacy", "cybersecurity"]
}

# ------------------------------
# 11. Count keyword occurrences per company
# ------------------------------
def keyword_count(text, keywords):
    counts = {}
    for category, words in keywords.items():
        total = 0
        for w in words:
            total += len(re.findall(rf"\b{re.escape(w)}\b", text))
        counts[category] = total
    return counts

keyword_results = []

for idx, row in df.iterrows():
    text = row["clean_text"]
    company = row.get("company", "Unknown")  # fallback if company column missing
    year = row.get("year", None)             # optional if you have filing years
    
    counts = keyword_count(text, geopolitical_keywords)
    counts["company"] = company
    counts["year"] = year
    keyword_results.append(counts)

geo_df = pd.DataFrame(keyword_results)

# ------------------------------
# 12. Save results for Tableau
# ------------------------------
geo_df.to_csv("20F_geopolitical_keywords.csv", index=False)
print("Saved keyword counts to 20F_geopolitical_keywords.csv")

# ------------------------------
# 13. Quick heatmap in Python
# ------------------------------
pivot = geo_df.groupby("company")[list(geopolitical_keywords.keys())].sum()

plt.figure(figsize=(12,6))
sns.heatmap(pivot, cmap="Reds", annot=True, fmt="g")
plt.title("Geopolitical Keyword Mentions by Company")
plt.show()
