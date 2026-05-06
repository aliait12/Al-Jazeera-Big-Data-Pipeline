import glob
import json
import logging
import os
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
SILVER_DIR = os.path.join(DATA_DIR, "silver")

REQUIRED_FIELDS = [
    "title",
    "author",
    "category",
    "content",
    "source",
    "url",
    "language",
    "scraped_at",
    "cleaned_at",
    "layer",
]


def find_latest_file(pattern: str):
    files = sorted(glob.glob(pattern))
    return files[-1] if files else None


def validate_article(article: dict) -> list[str]:
    missing = [field for field in REQUIRED_FIELDS if article.get(field) in (None, "", [])]
    if article.get("layer") != "silver":
        missing.append("layer != silver")
    return missing


def run():
    filepath = find_latest_file(os.path.join(SILVER_DIR, "aljazeera_silver_*.json"))
    if not filepath:
        logging.warning("Aucun fichier silver trouvé pour validation de schéma.")
        return

    logging.info(f"Validation du schéma pour {filepath}")
    errors = []
    with open(filepath, "r", encoding="utf-8") as f:
        articles = json.load(f)

    for index, article in enumerate(articles, 1):
        missing = validate_article(article)
        if missing:
            errors.append((index, missing))
            if len(errors) >= 20:
                break

    if errors:
        logging.error(f"{len(errors)} articles invalides détectés.")
        for idx, missing in errors[:10]:
            logging.error(f"Article {idx} : champs manquants/invalides = {missing}")
        raise SystemExit(1)

    logging.info("Validation de schéma réussie. Tous les articles silver respectent le contrat.")


if __name__ == "__main__":
    run()