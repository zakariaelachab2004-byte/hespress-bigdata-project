import json
import time
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from streaming_monitoring import update_streaming_metrics
from pathlib import Path

HEADERS = {"User-Agent": "Mozilla/5.0"}
HOME_URL = "https://fr.hespress.com/"
TOPIC = "articles-stream"

# Fichier qui mémorise les articles déjà envoyés à Kafka
SEEN_URLS_FILE = Path("data/warehouse/hespress/latest/seen_urls.json")


def load_seen_urls():
    """
    Charge les URLs déjà traitées depuis un fichier JSON.
    Cela évite de renvoyer les mêmes articles après redémarrage Docker.
    """
    if SEEN_URLS_FILE.exists():
        try:
            with open(SEEN_URLS_FILE, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception as e:
            print(f"Erreur lecture seen_urls.json : {e}", flush=True)
            return set()

    return set()


def save_seen_urls(seen_urls):
    """
    Sauvegarde les URLs déjà traitées.
    """
    SEEN_URLS_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(SEEN_URLS_FILE, "w", encoding="utf-8") as f:
        json.dump(list(seen_urls), f, ensure_ascii=False, indent=2)


seen_urls = load_seen_urls()

producer = None

while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers="kafka:9092",
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
        )
        print("Producer connecté à Kafka", flush=True)
    except NoBrokersAvailable:
        print("Kafka pas encore prêt, nouvelle tentative dans 5 secondes...", flush=True)
        time.sleep(5)


def get_article_links():
    response = requests.get(HOME_URL, headers=HEADERS, timeout=20)
    soup = BeautifulSoup(response.text, "html.parser")

    links = []

    for a in soup.find_all("a", href=True):
        link = a["href"]
        text = a.get_text(strip=True)

        if "https://fr.hespress.com/" in link and len(text) > 30:
            if link not in links:
                links.append(link)

    return links[:30]


def scrape_article(link):
    article_response = requests.get(link, headers=HEADERS, timeout=20)
    article_soup = BeautifulSoup(article_response.text, "html.parser")

    title_tag = article_soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else "Titre non trouvé"

    published_at = "Date non trouvée"
    time_tag = article_soup.find("time")

    if time_tag:
        published_at = time_tag.get("datetime") or time_tag.get_text(strip=True)

    paragraphs = article_soup.find_all("p")
    content_list = []

    for p in paragraphs:
        text = p.get_text(strip=True)
        if len(text) > 40:
            content_list.append(text)

    content = " ".join(content_list) if content_list else "Contenu non trouvé"

    return {
        "title": title,
        "published_at": published_at,
        "content": content,
        "url": link,
        "source": "Hespress",
        "mode": "streaming"
    }


print("Streaming temps réel démarré...", flush=True)
print(f"{len(seen_urls)} URL(s) déjà connues chargées.", flush=True)

while True:
    try:
        links = get_article_links()

        new_articles = 0
        errors_count = 0

        for link in links:
            if link not in seen_urls:
                try:
                    article = scrape_article(link)

                    producer.send(TOPIC, article)
                    producer.flush()

                    seen_urls.add(link)
                    save_seen_urls(seen_urls)

                    new_articles += 1

                    print(f"Nouvel article envoyé à Kafka : {article['title']}", flush=True)

                except Exception as e:
                    errors_count += 1
                    print(f"Erreur lors de l'envoi de l'article à Kafka : {e}", flush=True)

        if new_articles == 0:
            print("Aucun nouvel article détecté", flush=True)
            update_streaming_metrics(
                new_articles_count=0,
                errors_count=0
            )
        else:
            print(f"{new_articles} nouvel(s) article(s) envoyé(s) à Kafka", flush=True)
            update_streaming_metrics(
                new_articles_count=new_articles,
                errors_count=errors_count
            )

        time.sleep(60)

    except Exception as e:
        print(f"Erreur streaming : {e}", flush=True)

        update_streaming_metrics(
            new_articles_count=0,
            errors_count=1
        )

        time.sleep(10)