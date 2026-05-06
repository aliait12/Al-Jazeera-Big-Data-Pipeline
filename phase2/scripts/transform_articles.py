import glob
import json
import logging
import os
import re
from datetime import datetime

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
BRONZE_DIR = os.path.join(DATA_DIR, "bronze")
SILVER_DIR = os.path.join(DATA_DIR, "silver")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Patterns publicitaires et de navigation à supprimer du contenu
AD_PATTERNS = [
    r"advertisement",
    r"sponsored\s+content",
    r"promoted\s+content",
    r"paid\s+content",
    r"skip\s+to\s+(main\s+)?content",
    r"cookie\s+policy",
    r"privacy\s+policy",
    r"terms\s+of\s+(use|service)",
    r"subscribe\s+now",
    r"sign\s+up\s+for",
    r"newsletter",
    r"follow\s+us\s+on",
    r"share\s+this\s+article",
    r"more\s+from\s+al\s+jazeera",
    r"read\s+more",
    r"continue\s+reading",
    r"click\s+here",
    r"javascript\s+is\s+(not\s+)?required",
    r"enable\s+javascript",
]

AD_REGEX = re.compile(
    "|".join(AD_PATTERNS),
    re.IGNORECASE
)


def normalize_text(text: str) -> str:
    if not text:
        return text
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def remove_ad_content(text: str) -> str:
    """Supprime les blocs publicitaires et de navigation du contenu scraped."""
    if not text:
        return text

    # Supprimer les phrases/segments contenant des patterns publicitaires
    sentences = re.split(r"(?<=[.!?])\s+|\n", text)
    clean_sentences = []
    removed = 0
    for sentence in sentences:
        if AD_REGEX.search(sentence):
            removed += 1
            continue
        # Ignorer les fragments très courts (< 20 chars) qui sont souvent de la nav
        if len(sentence.strip()) < 20:
            continue
        clean_sentences.append(sentence.strip())

    if removed:
        logging.debug(f"Supprimé {removed} segment(s) publicitaire(s)")

    return " ".join(clean_sentences).strip()


def clean_article(article: dict) -> dict:
    cleaned = article.copy()
    cleaned["title"] = normalize_text(cleaned.get("title", ""))
    # Nettoyer le contenu : supprimer pubs et navigation AVANT normalisation
    raw_content = cleaned.get("content", "")
    cleaned["content"] = normalize_text(remove_ad_content(raw_content))
    cleaned["category"] = normalize_text(cleaned.get("category", "")).lower() or "general"
    cleaned["author"] = normalize_text(cleaned.get("author", "")) or "Unknown"
    cleaned["language"] = normalize_text(cleaned.get("language", "")) or "unknown"

    published = cleaned.get("date_published")
    try:
        if published:
            cleaned["date_published"] = datetime.fromisoformat(published).isoformat()
    except Exception:
        cleaned["date_published"] = published

    if not cleaned.get("date_published"):
        cleaned["date_published"] = cleaned.get("scraped_at")

    cleaned["cleaned_at"] = datetime.utcnow().isoformat()
    cleaned["layer"] = "silver"
    return cleaned


def load_bronze_files() -> list[dict]:
    articles = []
    files = sorted(glob.glob(os.path.join(BRONZE_DIR, "aljazeera_*.json")))
    for filepath in files:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                content = json.load(f)
                articles.extend(content)
                logging.info(f"Chargé {len(content)} articles depuis {os.path.basename(filepath)}")
        except Exception as exc:
            logging.error(f"Impossible de charger {filepath}: {exc}")
    return articles


def save_silver_file(cleaned_articles: list[dict]):
    os.makedirs(SILVER_DIR, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"aljazeera_silver_{timestamp}.json"
    filepath = os.path.join(SILVER_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(cleaned_articles, f, ensure_ascii=False, indent=2)
    logging.info(f"{len(cleaned_articles)} articles nettoyés sauvegardés dans {filepath}")


def run():
    raw_articles = load_bronze_files()
    if not raw_articles:
        logging.warning("Aucun fichier bronze trouvé. Arrêt de la transformation.")
        return

    # Déduplication par URL avant nettoyage
    seen_urls: set = set()
    unique_articles = []
    for article in raw_articles:
        url = article.get("url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_articles.append(article)
        elif not url:
            unique_articles.append(article)
    logging.info(f"Après déduplication : {len(unique_articles)}/{len(raw_articles)} articles uniques")

    cleaned = [clean_article(article) for article in unique_articles if article.get("content")]
    # Filtrer les articles dont le contenu est vide après nettoyage pub
    cleaned = [a for a in cleaned if len(a.get("content", "")) >= 100]
    logging.info(f"{len(cleaned)} articles valides après nettoyage")
    save_silver_file(cleaned)


if __name__ == "__main__":
    run()
