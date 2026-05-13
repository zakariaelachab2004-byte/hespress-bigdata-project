import json
import os
import time
import subprocess
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

TOPIC = "articles-stream"
today = datetime.today().strftime("%Y-%m-%d")

# Dossier Bronze Stream
path = f"data/lake/bronze/hespress_stream/{today}"
os.makedirs(path, exist_ok=True)

file_path = f"{path}/articles_stream.json"

consumer = None

# Attendre Kafka
while consumer is None:
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers="kafka:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="hespress-group-v5",
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )

        print("✅ Consumer connecté à Kafka", flush=True)

    except NoBrokersAvailable:
        print("⏳ Kafka pas encore prêt, nouvelle tentative dans 5 secondes...", flush=True)
        time.sleep(5)

articles = []

print("📡 Consumer Kafka en attente...", flush=True)

# Temps du dernier ETL
last_etl_time = time.time()

for message in consumer:

    article = message.value

    # Vérifier doublons
    article_url = article.get("url")

    if not any(a.get("url") == article_url for a in articles):

        articles.append(article)

        # Sauvegarde Bronze Stream
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(articles, f, ensure_ascii=False, indent=2)

        print(f"📥 Reçu depuis Kafka : {article.get('title', 'No title')}", flush=True)

    # Lancer ETL toutes les 60 secondes
    current_time = time.time()

    if current_time - last_etl_time >= 60:

        print("🧹 Nettoyage des données...", flush=True)
        subprocess.run(["python", "clean_data.py"])

        print("📊 Analyse des données...", flush=True)
        subprocess.run(["python", "analyze_data.py"])

        print("🏗️ Mise à jour Data Warehouse...", flush=True)
        subprocess.run(["python", "warehouse.py"])

        print("✅ Pipeline ETL terminé", flush=True)

        # Reset timer
        last_etl_time = current_time