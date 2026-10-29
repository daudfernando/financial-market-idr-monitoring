import json
import unittest

from scripts.streaming.kontrak_event import make_event, validate

PAYLOAD = {
    "symbol": "JPM",
    "bar_start_utc": "2026-09-02T13:30:00+00:00",
    "price_usd": 358.28,
    "currency": "USD",
    "source": "yfinance",
    "record_type": "historical_bar",
    "source_interval": "1m",
}
INGESTED = "2026-09-16T12:00:00+00:00"


class MarketContractTest(unittest.TestCase):
    def test_replay_preserves_source_time_and_stable_identity(self):
        first = make_event(PAYLOAD, "replay", INGESTED)
        second = make_event(PAYLOAD, "replay", "2026-09-16T12:01:00+00:00")
        self.assertEqual(first["event_timestamp"], PAYLOAD["bar_start_utc"])
        self.assertEqual(first["event_id"], second["event_id"])
        self.assertNotEqual(first["ingested_at"], second["ingested_at"])
        self.assertEqual(json.loads(first["raw_payload"]), PAYLOAD)

    def test_invalid_prices_and_currency_rejected(self):
        for field, value in [
            ("price_usd", float("nan")),
            ("price_usd", 0),
            ("price_usd", -1),
            ("price_usd", True),
            ("currency", "EUR"),
        ]:
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                make_event({**PAYLOAD, field: value}, "replay", INGESTED)

    def test_naive_and_future_timestamp_rejected(self):
        for value in ["2026-09-02T13:30:00", "2027-01-01T00:00:00+00:00"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                make_event({**PAYLOAD, "bar_start_utc": value}, "replay", INGESTED)

    def test_modified_identity_rejected(self):
        event = make_event(PAYLOAD, "replay", INGESTED)
        event["price_usd"] += 1
        with self.assertRaises(ValueError):
            validate(event)

    def test_live_requires_verified_currency_and_millisecond_time(self):
        event = make_event(
            {"id": "JPM", "price": 358.28, "currency": "USD", "time": 1788960600000},
            "live_websocket",
            INGESTED,
        )
        self.assertEqual(event["record_type"], "price_update")
        self.assertIsNone(event["replayed_at"])
        with self.assertRaises(ValueError):
            make_event(
                {"id": "JPM", "price": 358.28, "time": 1788960600000},
                "live_websocket",
                INGESTED,
            )


if __name__ == "__main__":
    unittest.main()
