import unittest
from unittest.mock import patch

import sentiment_models
from sentiment_pipeline import deduplicate_article_records, score_article_records


class SentimentPipelineTest(unittest.TestCase):
    """Verify deduplication, batching, regional isolation, and pacing."""

    def test_existing_and_same_batch_duplicates_are_removed_before_scoring(self):
        """Remove persisted and same-batch duplicates before scoring."""
        calls = []

        def score_batch(headlines, region=None):
            """Record inputs and return deterministic neutral test scores."""
            calls.append((region, list(headlines)))
            return [
                {
                    "sentiment": "Neutral",
                    "score": 0,
                    "sentiment_model": "test-model",
                }
                for _ in headlines
            ]

        articles = [
            {"title": "Already stored", "text": "old body", "region": "International"},
            {"title": "Repeated headline", "text": "first body", "region": "International"},
            {"title": "Repeated headline", "text": "second body", "region": "International"},
            {"title": "New headline", "text": "new body", "region": "International"},
        ]
        unique_articles = deduplicate_article_records(articles, {"Already stored"})
        scored = score_article_records(unique_articles, score_batch)

        self.assertEqual(
            [article["title"] for article in unique_articles],
            ["Repeated headline", "New headline"],
        )
        self.assertEqual([record["title"] for record in scored], [
            "Repeated headline", "New headline"
        ])
        self.assertEqual(
            calls,
            [("International", ["Repeated headline", "New headline"])],
        )

    def test_batches_at_five_and_preserves_score_to_headline_mapping(self):
        """Split batches at five headlines and preserve headline-score mapping."""
        scores = {
            "headline 0": -100,
            "headline 1": -60,
            "headline 2": -10,
            "headline 3": 20,
            "headline 4": 50,
            "headline 5": 80,
            "headline 6": 100,
        }
        calls = []

        def score_batch(headlines, region=None):
            """Return deterministic scores for the requested headlines."""
            calls.append(list(headlines))
            return [
                {
                    "sentiment": (
                        "Positive" if scores[headline] >= 20
                        else "Negative" if scores[headline] <= -20
                        else "Neutral"
                    ),
                    "score": scores[headline],
                    "sentiment_model": "test-model",
                }
                for headline in headlines
            ]

        articles = [
            {"title": f"headline {index}", "text": f"body {index}", "region": "International"}
            for index in range(7)
        ]
        scored = score_article_records(articles, score_batch)

        self.assertEqual([len(batch) for batch in calls], [5, 2])
        self.assertEqual([record["title"] for record in scored], [
            f"headline {index}" for index in range(7)
        ])
        self.assertEqual(
            [record["sentiment_score"] for record in scored],
            [float(scores[f"headline {index}"]) for index in range(7)],
        )
        self.assertTrue(all(record["region"] == "International" for record in scored))

    def test_articles_are_batched_separately_by_region(self):
        """Keep Nepali and international headlines in separate batches."""
        calls = []

        def score_batch(headlines, region=None):
            """Record regional inputs and return neutral test scores."""
            calls.append((region, list(headlines)))
            return [
                {
                    "sentiment": "Neutral",
                    "score": 0,
                    "sentiment_model": "test-model",
                }
                for _ in headlines
            ]

        articles = [
            {"title": "International one", "text": "", "region": "International"},
            {"title": "नेपाली एक", "text": "", "region": "Nepali"},
            {"title": "International two", "text": "", "region": "International"},
            {"title": "नेपाली दुई", "text": "", "region": "Nepali"},
        ]
        scored = score_article_records(articles, score_batch)

        self.assertEqual(
            [(region, len(headlines)) for region, headlines in calls],
            [("International", 2), ("Nepali", 2)],
        )
        self.assertEqual([record["title"] for record in scored], [
            article["title"] for article in articles
        ])

    def test_only_title_is_sent_and_missing_titles_are_unscored(self):
        """Send only titles and leave records without a title unscored."""
        calls = []

        def score_batch(headlines, region=None):
            """Record requested headlines and return one deterministic score."""
            calls.append((region, list(headlines)))
            return [{
                "sentiment": "Positive",
                "score": 40,
                "sentiment_model": "test-model",
            }]

        scored = score_article_records([
            {
                "title": "नयाँ विद्यालय खुल्यो",
                "text": "long body that must not reach the model",
                "region": "Nepali",
            },
            {"title": " ", "text": "body-only story", "region": "Nepali"},
            {"title": "", "text": "", "region": "Nepali"},
        ], score_batch)

        self.assertEqual(calls, [("Nepali", ["नयाँ विद्यालय खुल्यो"])])
        self.assertEqual(scored[0]["sentiment_language"], "Nepali")
        self.assertEqual(scored[0]["sentiment_score"], 40.0)
        self.assertEqual(scored[1]["sentiment"], "Unscored")
        self.assertEqual(scored[2]["sentiment"], "Unscored")

    def test_request_pacing_enforces_configured_interval(self):
        """Wait only for the remaining request interval."""
        previous_started_at = sentiment_models._last_request_started_at
        previous_interval = sentiment_models.MIN_REQUEST_INTERVAL_SECONDS
        try:
            sentiment_models._last_request_started_at = None
            sentiment_models.MIN_REQUEST_INTERVAL_SECONDS = 6.0
            with patch.object(
                sentiment_models.time,
                "monotonic",
                side_effect=[10.0, 10.0, 13.0, 16.0],
            ):
                with patch.object(sentiment_models.time, "sleep") as sleep:
                    sentiment_models._wait_for_request_slot()
                    sentiment_models._wait_for_request_slot()
            sleep.assert_called_once_with(3.0)
        finally:
            sentiment_models._last_request_started_at = previous_started_at
            sentiment_models.MIN_REQUEST_INTERVAL_SECONDS = previous_interval


if __name__ == "__main__":
    unittest.main()
