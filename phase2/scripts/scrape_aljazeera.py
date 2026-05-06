import json
import logging
import os
import time
from datetime import datetime
from typing import List

import requests
from bs4 import BeautifulSoup
from langdetect import detect, DetectorFactory

DetectorFactory.seed = 0

BASE_URL = "https://www.aljazeera.com"
SECTIONS = ["/news/", "/economy/", "/sports/", "/features/"]
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}
OUTPUT_DIR = os.getenv("DATA_DIR", "/opt/airflow/data/bronze")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def get_article_links(section_url: str) -> List[str]:
    links = []
    try:
        response = requests.get(section_url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")
        for tag in soup.find_all("a", href=True):
            href = tag["href"]
            if href.startswith("/") and any(href.startswith(s) for s in SECTIONS):
                full_url = BASE_URL + href
                if full_url not in links:
                    links.append(full_url)
        logging.info(f"{len(links)} liens trouvés dans {section_url}")
    except Exception as e:
        logging.error(f"Erreur récupération liens {section_url}: {e}")
    return links


def scrape_article(url: str) -> dict | None:
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")

        title_tag = soup.find("h1")
        title = title_tag.get_text(strip=True) if title_tag else None

        author_tag = soup.find("a", class_=lambda c: c and "author" in c.lower())
        if not author_tag:
            author_tag = soup.find("span", class_=lambda c: c and "author" in c.lower())
        author = author_tag.get_text(strip=True) if author_tag else "Al Jazeera"

        date_tag = soup.find("time")
        date_published = None
        if date_tag:
            date_published = date_tag.get("datetime") or date_tag.get_text(strip=True)

        category = "general"
        for section in SECTIONS:
            if section.strip("/") in url:
                category = section.strip("/")
                break

        content_tag = soup.find("div", class_=lambda c: c and "wysiwyg" in c.lower())
        if not content_tag:
            content_tag = soup.find("article")
        content = content_tag.get_text(separator=" ", strip=True) if content_tag else ""

        language = "unknown"
        if content:
            try:
                language = detect(content[:500])
            except Exception:
                language = "unknown"

        if not title or not content or len(content) < 100:
            logging.warning(f"Article ignoré (incomplet) : {url}")
            return None

        return {
            "title": title,
            "author": author,
            "date_published": date_published,
            "category": category,
            "content": content,
            "source": "Al Jazeera",
            "url": url,
            "language": language,
            "scraped_at": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logging.error(f"Erreur scraping article {url}: {e}")
        return None


def save_articles(articles: List[dict], output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"aljazeera_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)
    logging.info(f"{len(articles)} articles sauvegardés dans {filepath}")


def run():
    all_articles = []
    all_links = []
    for section in SECTIONS:
        section_url = BASE_URL + section
        all_links.extend(get_article_links(section_url))
        time.sleep(1)

    all_links = list(dict.fromkeys(all_links))
    logging.info(f"Total liens uniques : {len(all_links)}")

    for index, url in enumerate(all_links, start=1):
        logging.info(f"[{index}/{len(all_links)}] Scraping {url}")
        article = scrape_article(url)
        if article:
            all_articles.append(article)
        time.sleep(1.5)

    if all_articles:
        save_articles(all_articles, OUTPUT_DIR)
    else:
        logging.warning("Aucun article valide collecté.")


if __name__ == "__main__":
    run()
