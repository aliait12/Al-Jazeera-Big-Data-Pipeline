import glob
import json
import logging
import os
import re
from collections import Counter
from datetime import datetime

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
SILVER_DIR = os.path.join(DATA_DIR, "silver")
GOLD_DIR = os.path.join(DATA_DIR, "gold")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

STOPWORDS = {
    # Articles & déterminants
    "the", "a", "an", "this", "that", "these", "those",
    # Pronoms
    "i", "me", "my", "myself", "we", "our", "ours", "ourselves",
    "you", "your", "yours", "yourself", "yourselves",
    "he", "him", "his", "himself", "she", "her", "hers", "herself",
    "it", "its", "itself", "they", "them", "their", "theirs", "themselves",
    "what", "which", "who", "whom", "whose",
    # Verbes auxiliaires & être
    "is", "am", "are", "was", "were", "be", "been", "being",
    "has", "have", "had", "having",
    "do", "does", "did", "doing",
    "will", "would", "shall", "should",
    "can", "could", "may", "might", "must",
    # Prépositions
    "in", "on", "at", "to", "for", "of", "with", "by",
    "from", "up", "about", "into", "through", "during",
    "before", "after", "above", "below", "between", "under", "over",
    "again", "further", "then", "once", "upon", "along", "across",
    # Conjonctions
    "and", "but", "or", "nor", "not", "so", "yet", "both", "either",
    "neither", "each", "every", "all", "any", "few", "more", "most",
    "other", "some", "such", "only", "own", "same", "than", "too",
    "very", "just", "because", "as", "until", "while", "although",
    # Adverbes courants sans valeur analytique
    "also", "back", "even", "still", "well", "just", "now",
    "here", "there", "when", "where", "why", "how",
    "much", "many", "quite", "rather", "really", "already",
    # Verbes courants sans valeur analytique
    "said", "says", "say", "told", "tell", "made", "make",
    "came", "come", "went", "gone", "going", "get", "gets", "got",
    "know", "known", "take", "taken", "give", "given",
    "think", "thought", "see", "seen", "look", "want", "like",
    "use", "used", "find", "found", "need", "keep", "kept",
    "let", "put", "set", "seem", "help", "show", "hear",
    "play", "run", "move", "live", "believe", "bring",
    "happen", "work", "call", "try", "ask", "long",
    # Mots de structure / presse / navigation
    "news", "al", "jazeera", "article", "read", "report",
    "according", "new", "first", "last", "year", "years",
    "one", "two", "three", "four", "five", "part",
    "people", "time", "way", "day", "days", "world",
    "right", "left", "well", "high", "good", "great",
    "however", "since", "another", "around", "without",
    "number", "whether", "among", "include", "including",
    "within", "against", "early", "later", "began",
    "list", "been", "being", "become", "became",
    "down", "off", "out", "away", "through",
    "between", "different", "end", "following",
    "large", "late", "less", "little",
    "often", "order", "possible", "second",
    "small", "start", "state", "still",
    "three", "turn", "change",
    # Contenu publicitaire / navigation HTML (filet de sécurité Gold)
    "advertisement", "sponsored", "promoted", "subscribe",
    "newsletter", "cookie", "privacy", "javascript",
    "enable", "continue", "click", "share", "follow",
    "content", "loading", "menu", "navigation", "footer",
    "header", "sidebar", "widget", "banner", "popup",
}


def normalize_token(token: str) -> str:
    token = re.sub(r"[^\w']", "", token.lower())
    return token.strip()


def build_word_counts(articles: list[dict]) -> list[tuple[str, int]]:
    tokens = []
    for article in articles:
        text = f"{article.get('title', '')} {article.get('content', '')}"
        for raw in text.split():
            token = normalize_token(raw)
            if (
                token
                and token not in STOPWORDS
                and len(token) > 3
                and not token.isdigit()          # exclure les nombres purs
                and not re.fullmatch(r"\d+", token)  # exclure chiffres seuls
            ):
                tokens.append(token)
    return Counter(tokens).most_common(30)


def load_silver_files() -> list[dict]:
    articles = []
    files = sorted(glob.glob(os.path.join(SILVER_DIR, "aljazeera_silver_*.json")))
    for filepath in files:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                articles.extend(json.load(f))
                logging.info(f"Chargé {os.path.basename(filepath)}")
        except Exception as exc:
            logging.error(f"Erreur lecture {filepath}: {exc}")
    return articles


def build_metrics(articles: list[dict]) -> dict:
    metrics = {
        "total_articles": len(articles),
        "articles_by_source": {},
        "articles_by_category": {},
        "articles_by_language": {},
        "articles_by_country": {},  # Nouveau champ pour le pays
        "articles_per_day": {},
        "top_keywords": [],
    }

    # Mapping simple Langue -> Pays pour satisfaire l'exigence du projet
    lang_to_country = {
        "en": "International",
        "ar": "Middle East",
        "fr": "France/Francophonie",
        "es": "Spain/Latin America",
        "unknown": "Unknown"
    }

    for article in articles:
        metrics["articles_by_source"][article.get("source", "unknown")] = (
            metrics["articles_by_source"].get(article.get("source", "unknown"), 0) + 1
        )
        metrics["articles_by_category"][article.get("category", "general")] = (
            metrics["articles_by_category"].get(article.get("category", "general"), 0) + 1
        )
        lang = article.get("language", "unknown")
        metrics["articles_by_language"][lang] = (
            metrics["articles_by_language"].get(lang, 0) + 1
        )
        
        # Attribution du pays basé sur la langue (ou source)
        country = lang_to_country.get(lang, "Other")
        metrics["articles_by_country"][country] = (
            metrics["articles_by_country"].get(country, 0) + 1
        )

        date_value = article.get("date_published") or article.get("scraped_at")
        try:
            date_key = datetime.fromisoformat(date_value).date().isoformat()
        except Exception:
            date_key = "unknown"
        metrics["articles_per_day"][date_key] = (
            metrics["articles_per_day"].get(date_key, 0) + 1
        )

    metrics["top_keywords"] = build_word_counts(articles)
    return metrics


def save_gold_metrics(metrics: dict):
    os.makedirs(GOLD_DIR, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"aljazeera_gold_metrics_{timestamp}.json"
    filepath = os.path.join(GOLD_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    logging.info(f"Métriques gold enregistrées dans {filepath}")


def run():
    articles = load_silver_files()
    if not articles:
        logging.warning("Aucun fichier silver trouvé. Impossible de construire la couche Gold.")
        return

    metrics = build_metrics(articles)
    save_gold_metrics(metrics)


if __name__ == "__main__":
    run()
