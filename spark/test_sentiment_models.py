import unittest
from unittest.mock import patch

import sentiment_models


class SentimentProviderTest(unittest.TestCase):
    def test_cloudflare_scores_a_batch_as_the_primary_provider(self):
        headlines = ["one", "two", "three", "four", "five"]
        response = {
            "results": [
                {"id": 5, "score": -80},
                {"id": 4, "score": 70},
                {"id": 3, "score": 0},
                {"id": 2, "score": -40},
                {"id": 1, "score": 60},
            ]
        }
        with patch.object(
            sentiment_models, "_cloudflare_response", return_value=response
        ) as cloudflare, patch.object(sentiment_models, "_gemini_response") as gemini:
            results = sentiment_models.score_sentiment_batch(headlines)

        cloudflare.assert_called_once_with(headlines, None)
        gemini.assert_not_called()
        self.assertEqual([item["score"] for item in results], [60, -40, 0, 70, -80])
        self.assertTrue(
            all(item["sentiment_model"] == sentiment_models.CLOUDFLARE_MODEL_ID for item in results)
        )

    def test_gemini_handles_cloudflare_failure_for_same_region(self):
        headlines = ["A severe storm damaged homes."]
        with patch.object(
            sentiment_models,
            "_cloudflare_response",
            side_effect=RuntimeError("Cloudflare HTTP 503"),
        ) as cloudflare, patch.object(
            sentiment_models,
            "_gemini_response",
            return_value='{"results":[{"id":1,"score":-55}]}',
        ) as gemini:
            result = sentiment_models.score_sentiment_batch(headlines, region="Nepali")

        cloudflare.assert_called_once_with(headlines, "Nepali")
        gemini.assert_called_once_with(headlines, "Nepali")
        self.assertEqual(result[0]["sentiment"], "Negative")
        self.assertEqual(result[0]["sentiment_model"], sentiment_models.GEMINI_MODEL_ID)

    def test_provider_credentials_are_selected_by_region(self):
        cloudflare_response = {
            "success": True,
            "result": {"response": '{"results":[{"id":1,"score":10}]}'},
        }
        gemini_response = {
            "candidates": [{
                "content": {"parts": [{"text": '{"results":[{"id":1,"score":20}]}'}]}
            }]
        }
        environment = {
            "CLOUDFLARE_ACCOUNT_ID": "international-account",
            "CLOUDFLARE_API_TOKEN": "international-token",
            "GEMINI_API_KEY": "international-gemini",
            "CLOUDFLARE_ACCOUNT_ID_NEPALI": "nepali-account",
            "CLOUDFLARE_API_TOKEN_NEPALI": "nepali-token",
            "GEMINI_API_KEY_NEPALI": "nepali-gemini",
        }
        with patch.dict(sentiment_models.os.environ, environment), patch.object(
            sentiment_models, "_wait_for_request_slot"
        ), patch.object(
            sentiment_models, "_post_json",
            side_effect=[cloudflare_response, cloudflare_response, gemini_response],
        ) as post:
            sentiment_models._cloudflare_response(["headline"])
            sentiment_models._cloudflare_response(["headline"], region="Nepali")
            sentiment_models._gemini_response(["headline"], region="Nepali")

        international_call, nepali_cloudflare_call, nepali_gemini_call = post.call_args_list
        self.assertIn("international-account", international_call.args[0])
        self.assertEqual(
            international_call.args[2]["Authorization"],
            "Bearer international-token",
        )
        self.assertIn("nepali-account", nepali_cloudflare_call.args[0])
        self.assertEqual(
            nepali_cloudflare_call.args[2]["Authorization"],
            "Bearer nepali-token",
        )
        self.assertEqual(
            nepali_gemini_call.args[2]["x-goog-api-key"],
            "nepali-gemini",
        )



if __name__ == "__main__":
    unittest.main()
