"""Run optional live sentiment-provider smoke requests for both regions."""

import json
import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_env_file(path):
    """Load KEY=VALUE pairs without replacing values already in the environment."""
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        if not os.environ.get(name.strip()):
            os.environ[name.strip()] = value.strip()


def configure_ca_bundle():
    """Configure certifi as Python's TLS bundle when no bundle is already configured."""
    if os.environ.get("SSL_CERT_FILE") or os.environ.get("SSL_CERT_DIR"):
        return
    try:
        import certifi
    except ImportError:
        return
    os.environ["SSL_CERT_FILE"] = certifi.where()


def main():
    """Score example headlines using the configured external providers."""
    load_env_file(PROJECT_ROOT / ".env")
    load_env_file(PROJECT_ROOT / ".env.cloudflare")
    configure_ca_bundle()

    from sentiment_models import score_sentiment_batch
    from sentiment_pipeline import score_article_records

    headlines = [
        "A new community clinic opened, giving families access to free care.",
        "A major landslide destroyed homes and killed several residents.",
        "The council will meet on Tuesday to discuss the annual budget.",
        "नयाँ योजनाले गाउँमा स्वच्छ खानेपानी पुर्‍याएको छ।",
        "पहिरोले घर बगाउँदा तीन जनाको मृत्यु भयो।",
    ]
    articles = [{"title": headline, "text": ""} for headline in headlines]
    results = score_article_records(articles, score_sentiment_batch)
    nepali_articles = [
        {"title": "नयाँ योजनाले गाउँमा स्वच्छ खानेपानी पुर्‍याएको छ।", "region": "Nepali"},
        {"title": "पहिरोले घर बगाउँदा तीन जनाको मृत्यु भयो।", "region": "Nepali"},
    ]
    nepali_results = score_article_records(nepali_articles, score_sentiment_batch)
    print(json.dumps({
        "five_headline_batch": results,
        "nepali_provider_batch": nepali_results,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
