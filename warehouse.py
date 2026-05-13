import json
import os
from datetime import datetime
import csv

today = datetime.today().strftime("%Y-%m-%d")

# fichier Gold
gold_file = f"data/lake/gold/hespress/{today}/stats.json"

# fichier Silver
silver_file = f"data/lake/silver/hespress/{today}/articles_clean.json"

# dossier warehouse
warehouse_path = "data/warehouse/hespress/latest"
os.makedirs(warehouse_path, exist_ok=True)

# lire données Gold
with open(gold_file, "r", encoding="utf-8") as f:
    stats = json.load(f)

# -------------------------
# 1. Articles par jour
# -------------------------
file1 = f"{warehouse_path}/articles_per_day.csv"

with open(file1, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["date", "nombre_articles"])

    for date, count in stats["articles_per_day"].items():
        writer.writerow([date, count])

# -------------------------
# 2. Articles par thème
# -------------------------
file2 = f"{warehouse_path}/articles_by_theme.csv"

with open(file2, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["theme", "frequence"])

    for word, count in stats["top_words"]:
        writer.writerow([word, count])

# -------------------------
# 3. Articles par pays
# -------------------------

# lire les articles Silver
with open(silver_file, "r", encoding="utf-8") as f:
    articles = json.load(f)

countries = {
    "maroc": "Maroc",
    "france": "France",
    "espagne": "Espagne",
    "algérie": "Algérie",
    "algerie": "Algérie",
    "tunisie": "Tunisie",
    "usa": "USA",
    "états-unis": "USA",
    "etats-unis": "USA",
    "chine": "Chine",
    "russie": "Russie",
    "ukraine": "Ukraine",
    "palestine": "Palestine",
    "israël": "Israël",
    "israel": "Israël"
}

country_count = {}

for article in articles:
    title = article["title"].lower()

    for key, country in countries.items():
        if key in title:
            country_count[country] = country_count.get(country, 0) + 1

file3 = f"{warehouse_path}/articles_by_country.csv"

with open(file3, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["country", "nombre_articles"])

    for country, count in country_count.items():
        writer.writerow([country, count])

print("Data Warehouse créé")
print(f"Fichiers sauvegardés dans : {warehouse_path}")