"""Legacy standalone RSS/Kafka publisher; Docker Compose uses news_producer.py."""

import time
import json
import feedparser
from kafka import KafkaProducer
from urllib.request import Request, urlopen
from urllib.error import URLError
import ssl

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

FEED_URLS = [
    ("https://feeds.bbci.co.uk/news/rss.xml", "BBC"),
    ("https://www.cbsnews.com/latest/rss/main", "CBS"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml", "NY Times"),
    ("https://news.google.com/rss?hl=en-US&gl=US&ceid=US:en", "Yahoo"),
    ("https://feeds.npr.org/1003/rss.xml", "NPR")
]

sent_titles = set()

while True:
    try:
        context = ssl.create_default_context()

        for url in FEED_URLS:
            req = Request(url[0], headers={'User-Agent': 'Mozilla/5.0'})

            try:
                feed = feedparser.parse(urlopen(req, context=context, timeout=30))  # Timeout after 30 seconds

                if feed.bozo:
                    print(f"⚠️ Warning: Malformed feed XML from {url[0]}. Exception: {feed.bozo_exception}")

                if not feed.entries:
                    print(f"🤔 No entries found in feed: {url[0]}")

                for entry in feed.entries:
                    title = getattr(entry, 'title', 'No Title Provided').strip()
                    link = getattr(entry, 'link', 'No Link Provided')
                    source = url[1]

                    if title not in sent_titles and title != 'No Title Provided':
                        news = {"title": title, "link": link, "source": source}
                        try:
                            future = producer.send('news_topic', value=news)
                            future.get(timeout=10) # Wait max 10s for ack
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
                # Log the specific SSL error if available
                if isinstance(e.reason, ssl.SSLError):
                    print(f"   SSL Error details: {e.reason}")
            except Exception as e:
                print(f"❌ An unexpected error occurred while fetching feed {url[0]}: {e}")
                import traceback
                traceback.print_exc()

    except URLError as e:
        print(f"❌ Network Error fetching feed: {e}")
        # Log the specific SSL error if available
        if isinstance(e.reason, ssl.SSLError):
            print(f"   SSL Error details: {e.reason}")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

    print(f"--- Sleeping for 60 seconds ---")
    time.sleep(60)
