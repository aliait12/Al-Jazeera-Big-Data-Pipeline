import json
import os
import time
import glob
import shutil
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = "aljazeera-articles"
DATA_DIR = os.getenv("DATA_DIR", "./data")


def wait_for_kafka(retries=10, delay=5):
    """Attend que Kafka soit prêt."""
    for i in range(retries):
        try:
            producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            producer.close()
            logging.info("Kafka est prêt !")
            return True
        except NoBrokersAvailable:
            logging.warning(f"Kafka pas encore prêt, tentative {i+1}/{retries}...")
            time.sleep(delay)
    return False


def load_articles(data_dir: str) -> tuple[list[dict], list[str]]:
    """Charge tous les articles depuis les fichiers JSON."""
    all_articles = []
    json_files = sorted(glob.glob(os.path.join(data_dir, "aljazeera_*.json")))
    
    for filepath in json_files:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                articles = json.load(f)
                all_articles.extend(articles)
                logging.info(f"Chargé {len(articles)} articles depuis {filepath}")
        except Exception as e:
            logging.error(f"Erreur lecture {filepath}: {e}")
    
    seen_urls = set()
    unique_articles = []
    for article in all_articles:
        url = article.get("url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_articles.append(article)
    
    logging.info(f"Total articles uniques : {len(unique_articles)}")
    return unique_articles, json_files


def archive_files(json_files: list[str], archive_dir: str):
    processed_dir = os.path.join(archive_dir, "processed")
    os.makedirs(processed_dir, exist_ok=True)
    for filepath in json_files:
        try:
            basename = os.path.basename(filepath)
            destination = os.path.join(processed_dir, basename)
            shutil.move(filepath, destination)
            logging.info(f"Fichier archivé : {basename}")
        except Exception as e:
            logging.error(f"Impossible d'archiver {filepath}: {e}")


def send_articles_to_kafka(articles: list[dict]):
    """Envoie chaque article comme événement Kafka."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    sent = 0
    for article in articles:
        try:
            key = article.get("url", "")
            producer.send(TOPIC, key=key, value=article)
            sent += 1
            if sent % 10 == 0:
                logging.info(f"Envoyé {sent}/{len(articles)} articles...")
            time.sleep(0.1)
        except Exception as e:
            logging.error(f"Erreur envoi article: {e}")

    producer.flush()
    producer.close()
    logging.info(f"Streaming terminé : {sent} articles envoyés vers le topic '{TOPIC}'")


def run():
    if not wait_for_kafka():
        logging.error("Impossible de se connecter à Kafka. Arrêt.")
        return

    while True:
        logging.info("Chargement des articles...")
        articles, json_files = load_articles(DATA_DIR)

        if articles:
            logging.info(f"Envoi de {len(articles)} articles vers Kafka...")
            send_articles_to_kafka(articles)
            archive_files(json_files, DATA_DIR)
        else:
            logging.warning("Aucun article trouvé dans le dossier data/")

        logging.info("Prochain envoi dans 1 heure...")
        time.sleep(3600)


if __name__ == "__main__":
    run()