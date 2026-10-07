"""Parse RSS responses and recover valid entries from malformed feeds."""

import feedparser


def parse_feed_response(response):
    """Parse an RSS response and retain valid entries when its XML is malformed."""
    feed = feedparser.parse(response)
    entries = list(feed.entries or [])
    if feed.bozo:
        entries = [
            entry
            for entry in entries
            if isinstance(getattr(entry, "title", None), str)
            and entry.title.strip()
            and isinstance(getattr(entry, "link", None), str)
            and entry.link.startswith(("http://", "https://"))
        ]
    return feed, entries
