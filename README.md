Hespress Big Data Project
📌 Description

Ce projet consiste à construire une architecture Big Data complète autour des actualités du site Hespress.

Le pipeline permet de :

récupérer automatiquement les actualités (Web Scraping)
traiter les données avec un pipeline ETL
gérer les données Batch et Streaming
stocker les données dans un Data Lake
analyser les données
alimenter un Data Warehouse
visualiser les résultats dans Power BI

Le projet utilise Docker et Kafka afin de simuler une architecture Big Data professionnelle proche des environnements réels.

🏗️ Architecture du Projet
Hespress Website
       ↓
Web Scraping (Python)
       ↓
Kafka Streaming
       ↓
Bronze Layer (Raw Data)
       ↓
Silver Layer (Clean Data)
       ↓
Gold Layer (Analytics)
       ↓
Data Warehouse
       ↓
Power BI Dashboard
⚙️ Technologies utilisées
Python
Kafka
Docker
Docker Compose
BeautifulSoup
JSON
Power BI
Git / GitHub
📂 Structure du projet
hespress-bigdata-project/
│
├── data/
│   ├── lake/
│   │   ├── bronze/
│   │   ├── silver/
│   │   ├── gold/
│   │
│   ├── warehouse/
│   │
│   └── governance/
│       └── lineage.json
│
├── scraper.py
├── clean_data.py
├── analyze_data.py
├── warehouse.py
├── quality_checks.py
├── pipeline.py
├── producer_stream.py
├── consumer_stream.py
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
🔄 Fonctionnement du Pipeline
1️⃣ Web Scraping

Le script scraper.py récupère automatiquement les actualités Hespress :

titre
date
contenu
URL
source

Les données brutes sont stockées dans :

data/lake/bronze/
🧹 Nettoyage des données

Le script clean_data.py :

supprime les doublons
vérifie la qualité des données
supprime les articles incomplets
nettoie le texte

Les données nettoyées sont stockées dans :

data/lake/silver/
📊 Analyse des données

Le script analyze_data.py génère :

nombre d’articles
répartition par thème
statistiques globales

Résultats :

data/lake/gold/
🏢 Data Warehouse

Le script warehouse.py transforme les données analytiques en fichiers CSV exploitables dans Power BI.

Résultats :

data/warehouse/
⚡ Streaming Kafka

Le projet utilise Kafka pour simuler un flux temps réel.

Producer

producer_stream.py

récupère régulièrement les nouvelles actualités
envoie les données dans Kafka
Consumer

consumer_stream.py

consomme les messages Kafka
stocke les données dans Bronze Stream
déclenche automatiquement l’ETL
met à jour Silver, Gold et Warehouse
🐳 Docker

Le projet fonctionne entièrement avec Docker Compose.

Lancer le projet
docker compose up --build
📈 Power BI

Power BI est connecté au Data Warehouse.

Après actualisation :

les nouveaux articles apparaissent
les statistiques sont mises à jour automatiquement
✅ Qualité des données

Le script quality_checks.py vérifie :

articles sans titre
dates manquantes
contenus trop courts
cohérence des données
🧾 Gouvernance des données

Le fichier :

data/governance/lineage.json

permet de documenter :

la provenance des données
les transformations effectuées
la traçabilité du pipeline
🚀 Fonctionnalités du projet

✔ Web Scraping automatique
✔ Pipeline ETL
✔ Architecture Data Lake
✔ Streaming temps réel avec Kafka
✔ Dockerisation complète
✔ Contrôle qualité des données
✔ Gouvernance et traçabilité
✔ Data Warehouse
✔ Dashboard Power BI
✔ GitHub Versioning

👨‍💻 Auteur

Projet réalisé dans le cadre d’un projet Big Data / Data Engineering.

Auteur : Zakaria El Achab/ Adnane Benatik/ Med Reda faiz
