# Repository guidance for coding agents

## Objective

Maintain Newsroom Pulse: RSS collection for international and Nepali headlines, Kafka buffering, Spark micro-batch processing, title-only sentiment scoring, MongoDB persistence, and a refreshing Streamlit dashboard. See `README.md` for the full architecture, setup, data contract, and operational notes.

## Code map

- `producer/news_producer.py` — active Compose producer; feed list, polling, record construction, and Kafka publishing.
- `producer/rss_feed.py` — RSS parsing and malformed-feed recovery.
- `spark/sentiment_stream.py` — Structured Streaming entry point and MongoDB persistence.
- `spark/sentiment_pipeline.py` — duplicate filtering, region grouping, five-headline batches, and result mapping.
- `spark/sentiment_models.py` — English prompts, Cloudflare primary, Gemini fallback, request pacing, and JSON validation.
- `dashboard/app.py` — MongoDB reads and Streamlit/Plotly presentation.
- `producer/test.py` is a legacy standalone publisher, not the Compose entry point.

## Behavioral invariants

- Check MongoDB and same-microbatch exact titles **before** calling the sentiment provider.
- Send headline titles only; do not send the RSS `text` excerpt to the model.
- Keep batches at five headlines or fewer and separate batches by region. Preserve ID-to-headline mapping even if provider results arrive out of order.
- Keep Nepali and international prompt/credential selection separate. Nepali instructions are in English and accept Devanagari Nepali, Romanized Nepali, English, or mixed-language headlines.
- Preserve the `newsdb.sentiments` document fields and the score boundaries: `Positive` >= 20, `Negative` <= -20, otherwise `Neutral`; missing titles are `Unscored`.
- Keep API calls paced and keep provider credentials out of source, tests, logs, and documentation.
- Add a concise docstring to every function. Comments should explain non-obvious intent or constraints, not restate the next line. Update `README.md` when user-visible behavior or operational instructions change.

## Working safely

- Never read, print, stage, or commit `.env`, `.env.cloudflare`, or `.env.nepali`; use the `*.example` files for variable names and placeholders.
- Do not start/stop Docker services, delete volumes, or make live model API calls unless the task explicitly requires it. `docker compose down --volumes` destroys the local MongoDB and Spark checkpoint data.
- Prefer the existing Python `unittest` suites. Run the smallest relevant suite, then report the exact command and outcome. The Spark pipeline tests require PySpark; the Docker base image provides it.
- Keep changes scoped. Do not change feed cadence, scoring prompts, deduplication semantics, provider behavior, or service topology as part of documentation-only work.
