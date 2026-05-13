import json
import os
import re
from datetime import datetime
from collections import Counter

today = datetime.today().strftime("%Y-%m-%d")

# fichier Silver
silver_file = f"data/lake/silver/hespress/{today}/articles_clean.json"

# dossier Gold
gold_path = f"data/lake/gold/hespress/{today}"
os.makedirs(gold_path, exist_ok=True)

gold_file = f"{gold_path}/stats.json"

with open(silver_file, "r", encoding="utf-8") as f:
    articles = json.load(f)

# 1. nombre total
total_articles = len(articles)

# 2. articles par jour
dates = [a["published_at"][:10] for a in articles if a["published_at"] != "Date non trouvée"]
articles_per_day = Counter(dates)

# 3. mots fréquents (version améliorée)
stop_words = {
    "le", "la", "les", "de", "des", "du", "un", "une", "et", "à", "a",
    "en", "dans", "sur", "au", "aux", "pour", "par", "avec", "ce", "cet",
    "cette", "ces", "qui", "que", "quoi", "dont", "ou", "où", "se", "sa",
    "son", "ses", "leur", "leurs", "il", "elle", "ils", "elles", "nous",
    "vous", "je", "tu", "ne", "pas", "plus", "moins", "est", "sont",
    "été", "être", "fait", "faites", "comme", "après", "avant"
}

words = []

for article in articles:
    title = article["title"].lower()
    # garder seulement lettres, chiffres et espaces
    title = re.sub(r"[^\w\sÀ-ÿ]", " ", title)

    for word in title.split():
        if len(word) > 2 and word not in stop_words and not word.isdigit():
            words.append(word)

top_words = Counter(words).most_common(10)

# 4. longueur moyenne des articles
lengths = [len(a["content"]) for a in articles if a["content"] != "Contenu non trouvé"]
average_length = round(sum(lengths) / len(lengths), 2) if lengths else 0

stats = {
    "total_articles": total_articles,
    "articles_per_day": dict(articles_per_day),
    "top_words": top_words,
    "average_length": average_length,
    "analysis_date": today
}

with open(gold_file, "w", encoding="utf-8") as f:
    json.dump(stats, f, ensure_ascii=False, indent=2)

print("Analyse terminée")
print(f"Fichier sauvegardé dans : {gold_file}") 