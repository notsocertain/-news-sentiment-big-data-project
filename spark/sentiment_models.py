"""Call external providers for validated, region-specific headline sentiment batches."""

import json
import os
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# Smallest Workers AI model on the documented JSON Mode support list.
CLOUDFLARE_MODEL_ID = os.environ.get(
    "CLOUDFLARE_MODEL_ID", "@cf/meta/llama-3.1-8b-instruct"
)
GEMINI_MODEL_ID = "gemini-3.1-flash-lite"
NO_TEXT_MODEL = "unscored:no-headline@1"
MIN_REQUEST_INTERVAL_SECONDS = float(
    os.environ.get("GEMINI_MIN_REQUEST_INTERVAL_SECONDS", "6")
)
CLOUDFLARE_MIN_REQUEST_INTERVAL_SECONDS = float(
    os.environ.get("CLOUDFLARE_MIN_REQUEST_INTERVAL_SECONDS", "6")
)
_last_request_started_at = None
_GEMINI_BATCH_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "results": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "id": {"type": "INTEGER"},
                    "score": {"type": "INTEGER", "minimum": -100, "maximum": 100},
                },
                "required": ["id", "score"],
            },
        }
    },
    "required": ["results"],
}
_CLOUDFLARE_BATCH_SCHEMA = {
    "type": "object",
    "properties": {
        "results": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "score": {"type": "integer", "minimum": -100, "maximum": 100},
                },
                "required": ["id", "score"],
            },
        }
    },
    "required": ["results"],
}


def _wait_for_request_slot(interval_seconds=MIN_REQUEST_INTERVAL_SECONDS):
    """Enforce the process-local minimum interval between provider requests."""
    global _last_request_started_at
    now = time.monotonic()
    if _last_request_started_at is not None:
        delay = interval_seconds - (now - _last_request_started_at)
        if delay > 0:
            time.sleep(delay)
    _last_request_started_at = time.monotonic()


def _post_json(url, payload, headers, provider):
    """POST JSON to a provider and decode its response."""
    request = Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urlopen(request, timeout=30) as response:
            return json.load(response)
    except HTTPError as error:
        raise RuntimeError(f"{provider} sentiment API returned HTTP {error.code}.") from error
    except URLError as error:
        raise RuntimeError(
            f"{provider} sentiment API request failed: {error.reason}"
        ) from error


def _is_nepali_region(region):
    """Return whether a region uses Nepali prompts and credentials."""
    return (region or "").strip().casefold() == "nepali"


def _system_prompt(region):
    """Build provider-level output-format and language instructions."""
    if _is_nepali_region(region):
        return (
            "Return only valid JSON matching the requested results schema. "
            "Headlines may be in Nepali written in Devanagari or Romanized Nepali, "
            "English, or a mixture of these languages."
        )
    return "Return only valid JSON matching the requested results schema."


def _batch_prompt(headlines, region=None):
    """Build a region-specific JSON scoring request with stable headline IDs."""
    items = [
        {"id": index, "headline": headline}
        for index, headline in enumerate(headlines, start=1)
    ]
    if _is_nepali_region(region):
        instruction = (
            "Headlines may be in Nepali written in Devanagari script, Romanized Nepali, "
            "English, or a mixture of these languages. These instructions are in English; "
            "interpret the headline's original meaning, not its script or language. "
            "Score the stated impact of the event or outcome on people or communities. "
            "Use only the headline; do not infer the article body, external context, or "
            "unstated facts. A meeting, discussion, political contest, appointment, or "
            "event announcement is not inherently positive or negative; score 0 unless "
            "the headline states a clear benefit or harm. Do not assign polarity based "
            "only on a topic or dramatic wording. For mixed impact, use a moderate score "
            "based on the net stated impact, not an extreme. "
            "Scale: -100 exceptionally widespread or severe harm; -60 major harm; "
            "-30 moderate harm; -10 to 10 neutral, unclear, or balanced; +30 moderate "
            "benefit; +60 major benefit; +100 exceptionally broad benefit. Reserve "
            "-100 and +100 for exceptional outcomes. Return an integer from -100 to 100, "
            "usually in steps of 10; do not default to only 0, 50, or 100. Scores from "
            "-19 to 19 are neutral, unclear, or balanced. "
            "Examples: 'Parliament will discuss the budget today' -> 0; "
            "'A hotel association plans an expo' -> 0; "
            "'A landslide killed three people and destroyed homes' -> -80; "
            "'Flooding destroyed a bridge; relief distribution began' -> -40; "
            "'A new plan brought clean drinking water to 5,000 families' -> 60. "
            "Return only this JSON shape: "
            '{"results":[{"id":1,"score":0}]}. Include every id exactly once.\n'
            "Headlines:\n"
        )
    else:
        instruction = (
            "Classify sentiment from each news headline alone. Do not infer the article "
            "body, external context, or facts not stated in the headline. Evaluate each "
            "headline independently and consider only its stated event outcome. For each "
            "id, return one integer score from -100 (strongly negative) to 100 "
            "(strongly positive). Scores from -19 to 19 are neutral, mixed, or unclear. "
            "Return only this JSON shape: "
            '{"results":[{"id":1,"score":0}]}. Include every id exactly once.\n'
            "Headlines:\n"
        )
    return instruction + json.dumps(items, ensure_ascii=False)


def _credential_value(name, region):
    """Read an international or Nepali credential from the environment."""
    suffix = "_NEPALI" if _is_nepali_region(region) else ""
    return os.environ.get(f"{name}{suffix}", "").strip()


def _cloudflare_response(headlines, region=None):
    """Call Cloudflare Workers AI and return its generated JSON response."""
    account_id = _credential_value("CLOUDFLARE_ACCOUNT_ID", region)
    api_token = _credential_value("CLOUDFLARE_API_TOKEN", region)
    if not account_id or not api_token:
        raise RuntimeError("Cloudflare account id and API token are required.")

    payload = {
        "messages": [
            {
                "role": "system",
                "content": _system_prompt(region),
            },
            {"role": "user", "content": _batch_prompt(headlines, region)},
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": _CLOUDFLARE_BATCH_SCHEMA,
        },
        "temperature": 0,
        "max_tokens": max(128, len(headlines) * 64),
    }
    url = (
        "https://api.cloudflare.com/client/v4/accounts/"
        f"{account_id}/ai/run/{CLOUDFLARE_MODEL_ID}"
    )
    _wait_for_request_slot(CLOUDFLARE_MIN_REQUEST_INTERVAL_SECONDS)
    response = _post_json(
        url,
        payload,
        {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        },
        "Cloudflare Workers AI",
    )
    if response.get("success") is not True:
        raise RuntimeError("Cloudflare Workers AI reported an inference failure.")
    try:
        return response["result"]["response"]
    except (KeyError, TypeError) as error:
        raise RuntimeError("Cloudflare Workers AI returned an unexpected response.") from error


def _gemini_response(headlines, region=None):
    """Call Gemini and return its generated JSON response."""
    api_key = _credential_value("GEMINI_API_KEY", region)
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is required for the Gemini fallback.")

    payload = {
        "systemInstruction": {
            "parts": [{
                "text": _system_prompt(region)
            }]
        },
        "contents": [{"parts": [{"text": _batch_prompt(headlines, region)}]}],
        "generationConfig": {
            "temperature": 0,
            "maxOutputTokens": max(256, len(headlines) * 64),
            "responseMimeType": "application/json",
            "responseSchema": _GEMINI_BATCH_SCHEMA,
        },
    }
    _wait_for_request_slot(MIN_REQUEST_INTERVAL_SECONDS)
    response = _post_json(
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"{GEMINI_MODEL_ID}:generateContent",
        payload,
        {"x-goog-api-key": api_key, "Content-Type": "application/json"},
        "Gemini",
    )
    try:
        return response["candidates"][0]["content"]["parts"][0]["text"]
    except (IndexError, KeyError, TypeError) as error:
        raise RuntimeError("Gemini returned an unexpected batch response.") from error


def _parse_batch_scores(response_data, headline_count, provider):
    """Validate provider scores and restore the original headline order."""
    try:
        parsed_response = (
            json.loads(response_data) if isinstance(response_data, str) else response_data
        )
        results = parsed_response["results"]
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise RuntimeError(f"{provider} returned invalid sentiment JSON.") from error
    if not isinstance(results, list) or len(results) != headline_count:
        raise RuntimeError(f"{provider} returned the wrong number of sentiment scores.")

    scores_by_id = {}
    for result in results:
        if not isinstance(result, dict):
            raise RuntimeError(f"{provider} returned an invalid sentiment result.")
        item_id = result.get("id")
        score = result.get("score")
        if (
            type(item_id) is not int
            or type(score) is not int
            or not 1 <= item_id <= headline_count
            or not -100 <= score <= 100
            or item_id in scores_by_id
        ):
            raise RuntimeError(f"{provider} returned an invalid sentiment result.")
        scores_by_id[item_id] = score
    if set(scores_by_id) != set(range(1, headline_count + 1)):
        raise RuntimeError(f"{provider} omitted a sentiment result.")
    return [scores_by_id[item_id] for item_id in range(1, headline_count + 1)]


def _label_score(score):
    """Map a numeric score to its positive, neutral, or negative label."""
    return "Positive" if score >= 20 else "Negative" if score <= -20 else "Neutral"


def score_sentiment_batch(headlines, region=None):
    """Score a headline batch with Cloudflare first and Gemini as fallback."""
    if not headlines:
        return []

    try:
        scores = _parse_batch_scores(
            _cloudflare_response(headlines, region),
            len(headlines),
            "Cloudflare Workers AI",
        )
        model_id = CLOUDFLARE_MODEL_ID
    except Exception as cloudflare_error:
        try:
            scores = _parse_batch_scores(
                _gemini_response(headlines, region), len(headlines), "Gemini"
            )
            model_id = GEMINI_MODEL_ID
        except Exception as gemini_error:
            raise RuntimeError(
                "Cloudflare primary and Gemini fallback both failed: "
                f"{cloudflare_error}; {gemini_error}"
            ) from gemini_error

    return [
        {
            "id": index,
            "score": score,
            "sentiment": _label_score(score),
            "sentiment_model": model_id,
        }
        for index, score in enumerate(scores, start=1)
    ]
