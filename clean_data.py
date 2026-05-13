import json
import os
from datetime import datetime

today = datetime.today().strftime("%Y-%m-%d")

# Sources Bronze possibles
bronze_batch_file = f"data/lake/bronze/hespress/{today}/articles.json"
bronze_stream_file = f"data/lake/bronze/hespress_stream/{today}/articles_stream.json"

# Destination Silver
silver_path = f"data/lake/silver/hespress/{today}"
os.makedirs(silver_path, exist_ok=True)

silver_file = f"{silver_path}/articles_clean.json"

articles = []

# Lire les données batch si elles existent
if os.path.exists(bronze_batch_file):
    with open(bronze_batch_file, "r", encoding="utf-8") as f:
        batch_articles = json.load(f)
        articles.extend(batch_articles)
        print(f"{len(batch_articles)} articles batch chargés")

# Lire les données streaming si elles existent
if os.path.exists(bronze_stream_file):
    with open(bronze_stream_file, "r", encoding="utf-8") as f:
        stream_articles = json.load(f)
        articles.extend(stream_articles)
        print(f"{len(stream_articles)} articles streaming chargés")

# Si aucune donnée trouvée
if not articles:
    print("Aucune donnée Bronze trouvée")
    exit()

clean_articles = []
seen_urls = set()

for article in articles:
    title = article.get("title", "").strip()
    published_at = article.get("published_at", "").strip()
    content = article.get("content", "").strip()
    url = article.get("url", "").strip()

    # Règles de nettoyage / qualité
    if not title or title == "Titre non trouvé":
        continue

    if not published_at or published_at == "Date non trouvée":
        continue

    if not content or content == "Contenu non trouvé":
        continue

    if len(content) < 100:
        continue

    if not url:
        continue

    if url in seen_urls:
        continue

    # Nettoyage texte
    content = " ".join(content.split())

    clean_article = {
        "title": title,
        "published_at": published_at,
        "content": content,
        "url": url,
        "source": article.get("source", "Hespress"),
        "mode": article.get("mode", "batch"),
        "cleaning_date": today
    }

    clean_articles.append(clean_article)
    seen_urls.add(url)

# Sauvegarder dans Silver
with open(silver_file, "w", encoding="utf-8") as f:
    json.dump(clean_articles, f, ensure_ascii=False, indent=2)

print("Nettoyage terminé")
print(f"{len(clean_articles)} articles propres sauvegardés")
print(f"Fichier sauvegardé dans : {silver_file}")