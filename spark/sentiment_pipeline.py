import re

from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from sentiment_models import NO_TEXT_MODEL


_ANALYSIS_SCHEMA = StructType([
    StructField("sentiment", StringType()),
    StructField("sentiment_score", DoubleType()),
    StructField("sentiment_confidence", DoubleType()),
    StructField("sentiment_language", StringType()),
    StructField("sentiment_model", StringType()),
])
MAX_HEADLINES_PER_REQUEST = 5


def deduplicate_article_records(articles, existing_titles=()):
    seen_titles = set(existing_titles)
    unique_articles = []
    for article in articles:
        title = article.get("title")
        if title in seen_titles:
            continue
        seen_titles.add(title)
        unique_articles.append(article)
    return unique_articles


def score_article_records(articles, batch_scorer):
    scored_records = [None] * len(articles)
    pending_by_region = {}

    for index, article in enumerate(articles):
        headline = (article.get("title") or "").strip()
        if not headline:
            record = dict(article)
            record.update({
                "sentiment": "Unscored",
                "sentiment_score": None,
                "sentiment_confidence": None,
                "sentiment_language": "Unknown",
                "sentiment_model": NO_TEXT_MODEL,
            })
            scored_records[index] = record
            continue

        region = (article.get("region") or "").strip()
        language = "Nepali" if re.search(r"[\u0900-\u097F]", headline) else "English"
        pending_by_region.setdefault(region, []).append((index, headline, language))

    for region, pending in pending_by_region.items():
        for start in range(0, len(pending), MAX_HEADLINES_PER_REQUEST):
            batch = pending[start:start + MAX_HEADLINES_PER_REQUEST]
            results = batch_scorer(
                [headline for _, headline, _ in batch],
                region=region or None,
            )
            if len(results) != len(batch):
                raise RuntimeError("Sentiment scorer returned the wrong number of results.")

            for (index, _, language), result in zip(batch, results):
                record = dict(articles[index])
                record.update({
                    "sentiment": result["sentiment"],
                    "sentiment_score": float(result["score"]),
                    "sentiment_confidence": None,
                    "sentiment_language": language,
                    "sentiment_model": result["sentiment_model"],
                })
                scored_records[index] = record

    return scored_records
