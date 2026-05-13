import json
from datetime import datetime

today = datetime.today().strftime("%Y-%m-%d")

file_path = f"data/lake/silver/hespress/{today}/articles_clean.json"

with open(file_path, "r", encoding="utf-8") as f:
    articles = json.load(f)

missing_title = 0
missing_date = 0
short_content = 0

for article in articles:
    title = article.get("title", "").strip()
    published_at = article.get("published_at", "").strip()
    content = article.get("content", "").strip()

    # Test 1 : article sans titre
    if not title or title == "Titre non trouvé":
        missing_title += 1

    # Test 2 : date manquante
    if not published_at or published_at == "Date non trouvée":
        missing_date += 1

    # Test 3 : contenu trop court
    if len(content) < 50:
        short_content += 1

print("=== QUALITÉ DES DONNÉES ===")
print(f"Articles sans titre : {missing_title}")
print(f"Articles sans date : {missing_date}")
print(f"Articles avec contenu court : {short_content}")