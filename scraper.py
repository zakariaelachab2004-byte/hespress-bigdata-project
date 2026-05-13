import requests
from bs4 import BeautifulSoup
import json
import time
import os
from datetime import datetime


HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

HOME_URL = "https://fr.hespress.com/"
MAX_ARTICLES = 30


def get_article_links():
    """
    Récupère les liens des articles depuis la page d'accueil Hespress.
    """
    response = requests.get(HOME_URL, headers=HEADERS, timeout=20)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    links = []

    for a in soup.find_all("a", href=True):
        link = a["href"]
        text = a.get_text(strip=True)

        if (
            link.startswith("https://fr.hespress.com/")
            and len(text) > 30
            and link not in links
        ):
            links.append(link)

    return links


def extract_published_date(article_soup):
    """
    Extrait la date de publication avec plusieurs méthodes.
    """
    published_at = "Date non trouvée"

    # Méthode 1 : balise <time>
    time_tag = article_soup.find("time")
    if time_tag:
        published_at = time_tag.get("datetime") or time_tag.get_text(strip=True)

    # Méthode 2 : meta property="article:published_time"
    if published_at == "Date non trouvée":
        meta_date = article_soup.find("meta", property="article:published_time")
        if meta_date and meta_date.get("content"):
            published_at = meta_date.get("content")

    # Méthode 3 : meta itemprop="datePublished"
    if published_at == "Date non trouvée":
        meta_date = article_soup.find("meta", itemprop="datePublished")
        if meta_date and meta_date.get("content"):
            published_at = meta_date.get("content")

    # Méthode 4 : JSON-LD
    if published_at == "Date non trouvée":
        scripts = article_soup.find_all("script", type="application/ld+json")

        for script in scripts:
            try:
                if not script.string:
                    continue

                data = json.loads(script.string)

                if isinstance(data, dict):
                    if "datePublished" in data:
                        published_at = data["datePublished"]
                        break

                    if "@graph" in data:
                        for item in data["@graph"]:
                            if isinstance(item, dict) and "datePublished" in item:
                                published_at = item["datePublished"]
                                break

                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and "datePublished" in item:
                            published_at = item["datePublished"]
                            break

            except Exception:
                continue

    return published_at


def scrape_article(link):
    """
    Scrape un article Hespress.
    """
    article_response = requests.get(link, headers=HEADERS, timeout=20)
    article_response.raise_for_status()

    article_soup = BeautifulSoup(article_response.text, "html.parser")

    # Titre
    title_tag = article_soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else "Titre non trouvé"

    # Date
    published_at = extract_published_date(article_soup)

    # Contenu
    paragraphs = article_soup.find_all("p")
    content_list = []

    for p in paragraphs:
        p_text = p.get_text(strip=True)
        if len(p_text) > 40:
            content_list.append(p_text)

    content = " ".join(content_list) if content_list else "Contenu non trouvé"

    return {
        "title": title,
        "published_at": published_at,
        "content": content,
        "url": link,
        "source": "Hespress",
        "mode": "batch",
        "scraping_date": datetime.today().strftime("%Y-%m-%d")
    }


def is_valid_article(article):
    """
    Filtre les faux articles ou les pages incomplètes.
    """
    if article["title"] == "Titre non trouvé":
        return False

    if article["content"] == "Contenu non trouvé":
        return False

    if len(article["content"]) < 100:
        return False

    return True


def save_bronze_articles(articles, today):
    """
    Sauvegarde les articles dans la couche Bronze du Data Lake.
    """
    path = f"data/lake/bronze/hespress/{today}"
    os.makedirs(path, exist_ok=True)

    file_path = f"{path}/articles.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)

    return file_path


def main():
    today = datetime.today().strftime("%Y-%m-%d")

    print("Démarrage du scraping Hespress...")

    try:
        links = get_article_links()
        print(f"{len(links)} liens trouvés")
    except Exception as e:
        print(f"Erreur lors de la récupération des liens : {e}")
        return

    articles = []

    for link in links[:MAX_ARTICLES]:
        try:
            article = scrape_article(link)

            if is_valid_article(article):
                articles.append(article)
                print(f"Article récupéré : {article['title']}")
            else:
                print(f"Article ignoré car invalide : {link}")

            time.sleep(1)

        except Exception as e:
            print(f"Erreur sur {link} : {e}")

    file_path = save_bronze_articles(articles, today)

    print("Scraping terminé")
    print(f"{len(articles)} articles sauvegardés")
    print(f"Fichier sauvegardé dans : {file_path}")


if __name__ == "__main__":
    main()