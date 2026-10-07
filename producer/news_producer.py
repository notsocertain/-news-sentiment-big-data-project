"""Poll configured RSS feeds and publish article records to Kafka."""

import time
import json
import html
import re
from rss_feed import parse_feed_response
from kafka import KafkaProducer
from urllib.request import Request, urlopen
from urllib.error import URLError
import ssl


def clean_feed_text(value):
    """Remove markup and normalize whitespace in an RSS field."""
    value = html.unescape(re.sub(r"<[^>]*>", " ", value or ""))
    return re.sub(r"\s+", " ", value).strip()


def article_text(entry):
    """Choose an RSS excerpt and cap its stored length."""
    for block in entry.get("content", []):
        text = clean_feed_text(block.get("value", ""))
        if text:
            return text[:6000]
    return clean_feed_text(entry.get("summary") or entry.get("description", ""))[:6000]


producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9093'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            request_timeout_ms=60000
        )
        print("✅ Kafka Producer connected!")
    except Exception as e:
        print(f"❌ Kafka connection failed: {e}. Retrying in 10 seconds...")
        time.sleep(10)

# Interleave regions so both receive early processing during a fresh RSS backfill.
FEED_URLS = [
    ("https://feeds.bbci.co.uk/news/rss.xml", "BBC", "International"),
    ("https://www.onlinekhabar.com/feed", "Onlinekhabar", "Nepali"),
    ("https://www.cbsnews.com/latest/rss/main", "CBS", "International"),
    ("https://www.setopati.com/feed", "Setopati", "Nepali"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml", "NY Times", "International"),
    ("https://nepalnews.com/feed", "Nepal News", "Nepali"),
    ("https://news.google.com/rss?hl=en-US&gl=US&ceid=US:en", "Google News", "International"),
    ("https://www.ratopati.com/feed", "Ratopati", "Nepali"),
    ("https://feeds.npr.org/1003/rss.xml", "NPR", "International"),
    ("https://www.nepalpress.com/feed/", "Nepal Press", "Nepali"),
    ("https://www.theguardian.com/world/rss", "The Guardian", "International"),
    ("https://www.thehimalayantimes.com/rssFeed/15", "The Himalayan Times", "Nepali"),
    ("https://www.aljazeera.com/xml/rss/all.xml", "Al Jazeera", "International"),
    ("https://kathmandupost.com/rss/", "The Kathmandu Post", "Nepali"),
    ("https://rss.dw.com/rdf/rss-en-all", "DW", "International"),
    ("https://www.france24.com/en/rss", "France 24", "International"),
    ("https://www.cnbc.com/id/100003114/device/rss/rss.html", "CNBC", "International"),
    ("https://feeds.nbcnews.com/nbcnews/public/news", "NBC News", "International"),
    ("https://abcnews.go.com/abcnews/topstories", "ABC News", "International"),
]

sent_titles = set()

while True:
    try:
        context = ssl.create_default_context()

        for url in FEED_URLS:
            req = Request(url[0], headers={'User-Agent': 'Mozilla/5.0'})

            try:
                with urlopen(req, context=context, timeout=30) as response:
                    feed, entries = parse_feed_response(response)

                if feed.bozo:
                    print(
                        f"⚠️ Warning: Malformed feed XML from {url[0]}. "
                        f"Exception: {feed.bozo_exception}"
                    )
                    if not entries:
                        continue
                    print(
                        f"⚠️ Recovered {len(entries)} entries from malformed feed: "
                        f"{url[0]}"
                    )

                if not entries:
                    print(f"🤔 No entries found in feed: {url[0]}")

                for entry in entries:
                    title = getattr(entry, 'title', 'No Title Provided').strip()
                    link = getattr(entry, 'link', 'No Link Provided')
                    source = url[1]
                    region = url[2]

                    if title not in sent_titles and title != 'No Title Provided':
                        published = getattr(entry, 'published_parsed', None) or getattr(entry, 'updated_parsed', None)
                        published_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", published) if published else None
                        news = {
                            "title": title,
                            "text": article_text(entry),
                            "link": link,
                            "source": source,
                            "region": region,
                            "published_at": published_at,
                            "ingested_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        }
                        try:
                            future = producer.send('news_topic', value=news)
                            future.get(timeout=10)
                            sent_titles.add(title)
                            print("✅ Sent:", title)
                        except Exception as kafka_err:
                            print(f"❌ Kafka send error: {kafka_err}")
                    elif title in sent_titles:
                        print("⏩ Skipped duplicate:", title)
                    else:
                        print("⏩ Skipped entry with no title.")
            except URLError as e:
                print(f"❌ Network Error fetching feed from {url[0]}: {e}")
                if isinstance(e.reason, ssl.SSLError):
                    print(f"   SSL Error details: {e.reason}")
            except Exception as e:
                print(f"❌ An unexpected error occurred while fetching feed {url[0]}: {e}")
                import traceback
                traceback.print_exc()

    except URLError as e:
        print(f"❌ Network Error fetching feed: {e}")
        if isinstance(e.reason, ssl.SSLError):
            print(f"   SSL Error details: {e.reason}")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

    print(f"--- Sleeping for 60 seconds ---")
    time.sleep(60)
