import unittest

from rss_feed import parse_feed_response


class FeedRecoveryTest(unittest.TestCase):
    """Exercise recovery of valid entries from malformed RSS responses."""

    def test_recovers_entries_before_trailing_markup(self):
        """Retain a valid item that precedes trailing malformed markup."""
        response = (
            b"<rss version='2.0'><channel><title>Test</title>"
            b"<item><title>Story survives</title>"
            b"<link>https://example.test/story</link></item>"
            b"</channel></rss><script>trailing content</script>"
        )

        feed, entries = parse_feed_response(response)

        self.assertTrue(feed.bozo)
        self.assertEqual([entry.title for entry in entries], ["Story survives"])

    def test_returns_no_entries_for_unrecoverable_feed(self):
        """Return no entries when malformed XML has no recoverable article."""
        feed, entries = parse_feed_response(b"<rss><channel><item></rss>")

        self.assertTrue(feed.bozo)
        self.assertEqual(entries, [])


if __name__ == "__main__":
    unittest.main()
