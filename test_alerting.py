from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qs
from urllib.error import HTTPError

import duckdb

from alerting import (
    AlreadyRunning, Api, ApiError, Config, DataError, Mailer, Monitor,
    NotSubmitted, Store, deliver, job_lock, main,
)


UTC = timezone.utc
NOW = datetime(2026, 11, 13, 9, 0, tzinfo=UTC)


def settings(root: Path) -> Config:
    return Config({
        "state_dir": "data", "token_file": "token",
        "timezone": "Europe/Paris", "daily_check_time": "01:00",
        "earnings_event_types": [13],
        "price_alerts_enabled": True,
        "smtp": {
            "host": "smtp.example.test", "port": 587, "security": "starttls",
            "sender": "alerts@example.test", "recipients": ["team@example.test"],
        },
    }, root)


class FakeApi:
    def __init__(self):
        self.positions = {1: {"name": "Stock", "portfolios": ["europe", "europaone"]}}
        self.moves = {1: 0.0}
        self.articles = {}
        self.calendar = {}
        self.news_calls = []
        self.event_calls = []
        self.news_failure = False
        self.events_failure = False

    def holdings(self):
        return self.positions

    def variations(self, codes, today):
        return {code: value for code, value in self.moves.items() if code in codes}

    def news(self, code, start, end):
        self.news_calls.append((code, start, end))
        if self.news_failure:
            raise ApiError("offline")
        return [item for item in self.articles.get(code, [])
                if start <= datetime.fromisoformat(item["published_at"]) <= end]

    def events(self, code, today, end):
        self.event_calls.append((code, today, end))
        if self.events_failure:
            raise ApiError("offline")
        return self.calendar.get(code, [])


class MonitorTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.config = settings(self.root)
        self.path = self.root / "state.duckdb"
        self.store = Store(self.path)
        self.api = FakeApi()
        self.monitor = Monitor(self.config, self.store, self.api)

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()

    def messages(self, kind):
        return self.store.db.execute(
            "SELECT subject, body, status FROM outbox WHERE kind = ? ORDER BY created_at", [kind],
        ).fetchall()

    def test_exact_price_boundaries_directions_and_next_day(self):
        for value in (4.999, -4.999):
            self.api.moves[1] = value
            self.assertTrue(self.monitor.collect(NOW))
        self.assertEqual(self.messages("price"), [])
        for index, value in enumerate((5, 5, 10, 15, 16, -5, -10, -15, -16)):
            self.api.moves[1] = value
            self.assertTrue(self.monitor.collect(NOW + timedelta(minutes=index)))
        self.assertEqual(len(self.messages("price")), 6)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM alert_keys").fetchone()[0], 6)
        self.monitor.collect(NOW + timedelta(days=1))
        self.assertEqual(len(self.messages("price")), 7)
        self.assertIn("-5 %, -10 %, -15 %", self.messages("price")[-1][1])

    def test_jump_groups_levels_and_multiple_portfolios(self):
        self.api.moves[1] = 16
        self.monitor.collect(NOW)
        self.assertEqual(len(self.messages("price")), 1)
        body = self.messages("price")[0][1]
        self.assertIn("+5 %, +10 %, +15 %", body)
        self.assertIn("Portefeuille Europe", body)
        self.assertIn("Fonds Europa One", body)
        self.api.moves[1] = 5
        self.monitor.collect(NOW + timedelta(minutes=1))
        self.assertEqual(len(self.messages("price")), 1)

    def test_news_startup_dedup_restart_and_common_article(self):
        self.api.positions[2] = {"name": "Other", "portfolios": ["usa"]}
        self.api.moves[2] = 0
        old = {"news_id": 100, "title": "Old", "published_at": (NOW - timedelta(seconds=1)).isoformat()}
        self.api.articles = {1: [old]}
        self.monitor.collect(NOW)
        self.assertEqual(self.messages("news"), [])
        article = {"news_id": 101, "title": "New", "published_at": (NOW + timedelta(seconds=10)).isoformat()}
        self.api.articles = {1: [old, article], 2: [article]}
        self.monitor.collect(NOW + timedelta(minutes=1))
        self.assertEqual(len(self.messages("news")), 1)
        self.assertIn("Other", self.messages("news")[0][1])
        self.assertIn("Portefeuille USA", self.messages("news")[0][1])
        self.store.close()
        self.store = Store(self.path)
        self.monitor = Monitor(self.config, self.store, self.api)
        self.monitor.collect(NOW + timedelta(minutes=2))
        self.assertEqual(len(self.messages("news")), 1)

    def test_news_failure_preserves_cursor_and_catches_up(self):
        self.monitor.collect(NOW)
        self.api.news_failure = True
        with self.assertLogs("zb_alerting", level="ERROR"):
            self.assertFalse(self.monitor.collect(NOW + timedelta(days=2)))
        cursor = self.store.db.execute("SELECT checked_at FROM news_cursors WHERE code = 1").fetchone()[0]
        self.assertEqual(cursor, NOW.isoformat())
        self.api.news_failure = False
        self.api.articles = {1: [{
            "news_id": 123, "title": "During outage",
            "published_at": (NOW + timedelta(days=1)).isoformat(),
        }]}
        self.assertTrue(self.monitor.collect(NOW + timedelta(days=3)))
        self.assertEqual(len(self.messages("news")), 1)

    def test_daily_results_four_reminders_and_restart(self):
        event_day = NOW.date() + timedelta(days=7)
        self.api.calendar = {1: [{
            "date": event_day.isoformat(), "type": 13, "title": "Quarterly Results",
            "earning": {"fiscal-year": 2026, "fiscal-quarter": 4},
        }]}
        for offset in range(8):
            day = NOW + timedelta(days=offset)
            self.assertTrue(self.monitor.collect(day))
            calls = len(self.api.event_calls)
            self.monitor.collect(day + timedelta(minutes=1))
            self.assertEqual(len(self.api.event_calls), calls)
        messages = self.messages("earnings")
        self.assertEqual(len(messages), 4)
        self.assertIn("J-7", messages[0][0])
        self.assertIn("J-3", messages[1][0])
        self.assertIn("J-1", messages[2][0])
        self.assertIn("Jour J", messages[3][0])

    def test_daily_check_at_one_paris_and_retry_failure(self):
        before = NOW.replace(day=12, hour=23, minute=59)  # 00:59 Paris on November 13
        self.monitor.collect(before)
        self.assertEqual(self.api.event_calls, [])
        self.api.events_failure = True
        with self.assertLogs("zb_alerting", level="ERROR"):
            self.assertFalse(self.monitor.collect(before + timedelta(minutes=1)))
        self.assertIsNone(self.store.get("earnings_checked"))
        self.api.events_failure = False
        self.monitor.collect(before + timedelta(minutes=2))
        self.assertEqual(self.store.get("earnings_checked"), "2026-11-13")

    def test_calendar_all_types_distinct_events_and_dedup(self):
        self.config.raw["earnings_event_types"] = []
        self.api.calendar = {1: [
            {"date": "2026-11-14", "type": 13, "title": "Quarterly Results",
             "earning": {"fiscal-year": 2026, "fiscal-quarter": 4}},
            {"date": "2026-11-14", "type": 51, "title": "Morning conference"},
            {"date": "2026-11-14", "type": 51, "title": "Evening conference"},
            {"date": "2026-11-14", "type": 51, "title": "Morning conference"},
        ]}
        self.assertTrue(self.monitor.collect(NOW))
        messages = self.messages("earnings")
        self.assertEqual(len(messages), 3)
        self.assertTrue(all("[ZB Agenda J-1]" in message[0] for message in messages))
        self.assertTrue(any("Evening conference" in message[1] for message in messages))
        self.store.set("earnings_checked", "")
        self.assertTrue(self.monitor.collect(NOW + timedelta(minutes=1)))
        self.assertEqual(len(self.messages("earnings")), 3)

    def test_one_am_uses_paris_day_and_summer_time(self):
        self.api.moves[1] = 5
        self.monitor.collect(datetime(2026, 7, 1, 22, 59, tzinfo=UTC))
        self.monitor.collect(datetime(2026, 7, 1, 23, 0, tzinfo=UTC))
        keys = self.store.db.execute("SELECT key FROM alert_keys ORDER BY key").fetchall()
        self.assertEqual([k[0] for k in keys], ["price:1:2026-07-01:+:5", "price:1:2026-07-02:+:5"])

    def test_missing_quote_is_visible_failure(self):
        self.api.moves = {}
        self.assertFalse(self.monitor.collect(NOW))
        self.assertEqual(self.messages("price"), [])

    def test_monitoring_day_boundary_in_winter_and_dst_changes(self):
        for moment, expected in (
            ("2026-11-12T23:59:00+00:00", "2026-11-12"),
            ("2026-11-13T00:00:00+00:00", "2026-11-13"),
            ("2026-03-28T23:59:00+00:00", "2026-03-28"),
            ("2026-03-29T00:00:00+00:00", "2026-03-29"),
            ("2026-10-24T22:59:00+00:00", "2026-10-24"),
            ("2026-10-24T23:00:00+00:00", "2026-10-25"),
        ):
            with self.subTest(moment=moment):
                self.assertEqual(self.config.monitoring_date(datetime.fromisoformat(moment)).isoformat(), expected)

    def test_daily_holdings_cache_survives_restart_and_refreshes_next_day(self):
        self.api.holdings = Mock(wraps=self.api.holdings)
        first = self.monitor.daily_holdings("2026-11-13")
        self.store.close()
        self.store = Store(self.path)
        self.monitor = Monitor(self.config, self.store, self.api)
        self.assertEqual(self.monitor.daily_holdings("2026-11-13"), first)
        self.api.holdings.assert_called_once()
        self.api.positions = {}
        self.assertEqual(self.monitor.daily_holdings("2026-11-14"), {})
        self.assertEqual(self.monitor.daily_holdings("2026-11-14"), {})
        self.assertEqual(self.api.holdings.call_count, 2)

    def test_failed_holdings_refresh_preserves_snapshot_and_retries(self):
        previous = self.monitor.daily_holdings("2026-11-13")
        self.api.holdings = Mock(side_effect=ApiError("HTTP 502"))
        with self.assertRaises(ApiError):
            self.monitor.daily_holdings("2026-11-14")
        self.assertEqual(self.store.get("holdings_checked"), "2026-11-13")
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM holdings").fetchone()[0], len(previous))
        self.api.holdings.side_effect = None
        self.api.holdings.return_value = {}
        self.assertEqual(self.monitor.daily_holdings("2026-11-14"), {})
        self.assertEqual(self.api.holdings.call_count, 2)

    def test_holdings_scope_change_invalidates_same_day_cache(self):
        self.monitor.daily_holdings("2026-11-13")
        self.api.holdings = Mock(return_value={})
        self.config.raw["portfolios"] = ["europe"]
        self.assertEqual(self.monitor.daily_holdings("2026-11-13"), {})
        self.api.holdings.assert_called_once()

    def test_reference_cache_survives_restart_and_live_quotes_keep_refreshing(self):
        api = Api(self.config, token="not-a-real-token")
        api.cache_store = self.store
        api.daily_quotes = Mock(return_value={1: [("2026-11-12", 100), ("2026-11-13", 999)]})
        api.request = Mock(return_value={
            "instrument": {"quotes": {"last": 110, "last_date": "2026-11-13"}},
        })
        self.assertEqual(api.variations([1], "2026-11-13"), {1: 10})
        self.store.close()
        self.store = Store(self.path)
        api.cache_store = self.store
        api.request.return_value = {"instrument": {"quotes": {"last": 115, "last_date": "2026-11-13"}}}
        self.assertEqual(api.variations([1], "2026-11-13"), {1: 15})
        api.daily_quotes.assert_called_once_with([1], "2026-11-13")
        self.assertEqual(api.request.call_count, 2)
        api.daily_quotes.return_value = {1: [("2026-11-13", 120)]}
        api.request.return_value = {"instrument": {"quotes": {"last": 132, "last_date": "2026-11-14"}}}
        self.assertEqual(api.variations([1], "2026-11-14"), {1: 10})
        self.assertEqual(api.daily_quotes.call_count, 2)
        self.assertEqual(self.store.db.execute("SELECT quote_date FROM quote_references").fetchall(),
                         [("2026-11-13",)])

    def test_reference_cache_only_retries_missing_instruments(self):
        api = Api(self.config, token="not-a-real-token")
        api.cache_store = self.store
        api.daily_quotes = Mock(return_value={1: [("2026-11-12", 100)], 2: []})
        with self.assertLogs("zb_alerting", level="ERROR"):
            self.assertEqual(api.reference_quotes([1, 2], "2026-11-13"), {1: [("2026-11-12", 100)]})
        api.daily_quotes.return_value = {2: [("2026-11-12", 200)]}
        self.assertEqual(api.reference_quotes([1, 2], "2026-11-13"), {
            1: [("2026-11-12", 100)], 2: [("2026-11-12", 200)],
        })
        api.daily_quotes.assert_called_with([2], "2026-11-13")

    def test_prices_can_be_disabled_without_disabling_news_calendar(self):
        self.config.raw["price_alerts_enabled"] = False
        self.api.moves[1] = 15
        self.api.variations = Mock(side_effect=AssertionError("prices must not be queried"))
        with self.assertLogs("zb_alerting", level="INFO"):
            self.assertTrue(self.monitor.collect(NOW))
        self.assertEqual(self.messages("price"), [])
        self.assertEqual(len(self.api.news_calls), 1)
        self.assertEqual(len(self.api.event_calls), 1)

    def test_price_alerts_enabled_by_default(self):
        self.config.raw.pop("price_alerts_enabled")
        self.api.moves[1] = 5
        self.assertTrue(self.monitor.collect(NOW))
        self.assertEqual(len(self.messages("price")), 1)

    def test_old_quote_suppresses_price_without_consuming_threshold(self):
        self.api.moves[1] = None
        self.assertTrue(self.monitor.collect(NOW))
        self.assertEqual(self.messages("price"), [])
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM alert_keys").fetchone()[0], 0)
        self.assertEqual(len(self.api.news_calls), 1)
        self.assertEqual(len(self.api.event_calls), 1)
        self.api.moves[1] = 15
        self.assertTrue(self.monitor.collect(NOW + timedelta(minutes=1)))
        self.assertEqual(len(self.messages("price")), 1)

    def test_quote_date_failure_does_not_stop_other_titles(self):
        self.api.moves = {2: -10}
        self.api.positions[2] = {"name": "Other", "portfolios": ["usa"]}
        self.assertFalse(self.monitor.collect(NOW))
        self.assertEqual(len(self.messages("price")), 1)
        self.assertIn("Other", self.messages("price")[0][0])

    def test_transactions_and_parquet(self):
        with self.assertRaises(RuntimeError):
            with self.store.transaction():
                self.store.queue("news", ["key"], "subject", "body", NOW)
                raise RuntimeError("interruption")
        self.assertEqual(self.store.unseen(["key"]), ["key"])
        self.api.moves[1] = 15
        self.monitor.collect(NOW)
        self.store.export(self.root / "parquet")
        result = duckdb.sql(
            "SELECT count(*) FROM read_parquet(?)", params=[str(self.root / "parquet" / "alert_keys.parquet")],
        ).fetchone()
        self.assertEqual(result[0], 3)
        self.assertEqual(len(list((self.root / "parquet").glob("*.parquet"))), 6)
        self.assertEqual(list((self.root / "parquet").glob("*.tmp")), [])
        self.store.export(self.root / "parquet")

    def test_delivery_success_failure_uncertain_and_recovery(self):
        with self.store.transaction():
            identity = self.store.queue("news", ["n"], "subject", "body", NOW)
        mailer = Mock()
        mailer.send.side_effect = NotSubmitted("connection down")
        with self.assertLogs("zb_alerting", level="ERROR"):
            self.assertFalse(deliver(self.store, mailer, NOW))
        self.assertEqual(self.messages("news")[0][2], "pending")
        mailer.send.side_effect = RuntimeError("connection lost during DATA")
        with self.assertLogs("zb_alerting", level="ERROR"):
            self.assertFalse(deliver(self.store, mailer, NOW))
        self.assertEqual(self.messages("news")[0][2], "uncertain")
        mailer.reset_mock()
        with self.assertLogs("zb_alerting", level="ERROR"):
            deliver(self.store, mailer, NOW)
        mailer.send.assert_not_called()
        self.store.set_status(identity, "sending", NOW)
        with self.assertLogs("zb_alerting", level="ERROR"):
            deliver(self.store, mailer, NOW)
        self.assertEqual(self.messages("news")[0][2], "uncertain")
        self.store.set_status(identity, "pending", NOW)
        mailer.send.side_effect = None
        self.assertTrue(deliver(self.store, mailer, NOW))
        self.assertEqual(self.messages("news")[0][2], "sent")
        self.assertTrue(deliver(self.store, mailer, NOW))
        mailer.send.assert_called_once()


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.api = Api(settings(Path(".")), token="not-a-real-token")

    def test_portfolio_and_fund_sources_merge(self):
        self.api.request = Mock(side_effect=[
            {"open_trades": [{"codezb": "1", "name": "stock_name"}]},
            {"open_trades": []}, {"open_trades": []},
            {"holdings": [{"codeZB": 1, "nom": "STOCK"}, {"codeZB": 2, "nom": "OTHER"}]},
        ])
        holdings = self.api.holdings()
        self.assertEqual(holdings[1]["portfolios"], ["europe", "europaone"])
        self.assertEqual(holdings[1]["name"], "stock name")
        self.assertEqual(self.api.request.call_args.args[0], "fonds/composition")

    def test_quote_date_compared_without_timezone_conversion(self):
        for value in ("2026-11-13", "2026-11-13 10:00:00", "2026-11-13T23:59:00-10:00",
                      {"date": "2026-11-13 07:30:00.000000", "timezone": "Europe/Paris"}):
            with self.subTest(value=value):
                self.api.daily_quotes = Mock(return_value={1: [("2026-11-12", 100), ("2026-11-13", 999)]})
                self.api.request = Mock(return_value={"instrument": {"quotes": {"last": 115, "last_date": value}}})
                self.assertEqual(self.api.variations([1], "2026-11-13"), {1: 15})
                self.api.daily_quotes.assert_called_once_with([1], "2026-11-13")
                self.api.request.assert_called_once_with("instrument/1")
        for value in ("2026-11-12T23:59:00-10:00", "2026-11-12"):
            with self.subTest(value=value), self.assertNoLogs("zb_alerting", level="INFO"):
                self.api.request = Mock(return_value={"instrument": {"quotes": {"last": 115, "last_date": value}}})
                self.assertEqual(self.api.variations([1], "2026-11-13"), {1: None})
        for value in ("", "invalid", "2026-02-30", None, {}):
            with self.subTest(value=value), self.assertLogs("zb_alerting", level="ERROR"):
                self.api.request = Mock(return_value={"instrument": {"quotes": {"last": 115, "last_date": value}}})
                self.assertEqual(self.api.variations([1], "2026-11-13"), {})

    def test_summary_invalid_price_does_not_stop_other_instruments(self):
        self.api.daily_quotes = Mock(return_value={1: [("2026-11-12", 100)], 2: [("2026-11-12", 200)]})
        def summary(route):
            return {"instrument": {"quotes": {
                "last": "NaN" if route == "instrument/1" else 180,
                "last_date": {"date": "2026-11-13 10:00:00"},
            }}}
        self.api.request = Mock(side_effect=summary)
        with self.assertLogs("zb_alerting", level="ERROR"):
            self.assertEqual(self.api.variations([1, 2], "2026-11-13"), {2: -10})

    def test_summary_api_failure_is_concise_isolated_and_retried_next_run(self):
        self.api.daily_quotes = Mock(return_value={
            68944: [("2026-11-12", 100)], 2: [("2026-11-12", 200)],
        })
        def summary(route):
            if route == "instrument/68944":
                raise ApiError("API HTTP 502 on instrument/68944")
            return {"instrument": {"quotes": {"last": 180, "last_date": "2026-11-13"}}}
        self.api.request = Mock(side_effect=summary)
        with self.assertLogs("zb_alerting", level="ERROR") as logs:
            self.assertEqual(self.api.variations([68944, 2], "2026-11-13"), {2: -10})
        self.assertIn("68944", logs.output[0])
        self.assertIn("502", logs.output[0])
        self.assertTrue(all(record.exc_info is None for record in logs.records))
        self.api.request.side_effect = lambda route: {
            "instrument": {"quotes": {"last": 110, "last_date": "2026-11-13"}},
        }
        self.assertEqual(self.api.variations([68944, 2], "2026-11-13")[68944], 10)

    def test_http_502_retries_three_times_before_failure(self):
        self.api.base = "https://api.example.test/v1/internal"
        self.api.token = "not-a-real-token"
        errors = [HTTPError(self.api.base, 502, "Bad Gateway", {}, None) for _ in range(3)]
        with patch("alerting.urlopen", side_effect=errors) as opener, \
             patch.object(self.api, "pace") as pace, patch("alerting.time.sleep") as sleep:
            with self.assertRaisesRegex(ApiError, "HTTP 502"):
                self.api.request("instrument/68944")
        self.assertEqual(opener.call_count, 3)
        self.assertEqual(pace.call_count, 3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [1, 2])

    def test_news_time_split_dedup_and_filter(self):
        start = NOW
        end = NOW + timedelta(seconds=10)
        article = {"news_id": 5, "title": "News", "published_at": (NOW + timedelta(seconds=5)).isoformat()}
        old = {"news_id": 6, "title": "Old", "published_at": (NOW - timedelta(seconds=1)).isoformat()}
        self.api.request = Mock(side_effect=[
            {"nb_results": 3, "results": [article]},
            {"nb_results": 2, "results": [old, article]},
            {"nb_results": 1, "results": [article]},
        ])
        self.assertEqual(self.api.news(1, start, end), [article])
        self.assertEqual(self.api.request.call_count, 3)
        body = self.api.request.call_args.args[1]
        self.assertNotIn("fPmin", body)
        self.assertNotIn("fR", body)

    def test_saturated_second_fails_instead_of_skipping(self):
        self.api.request = Mock(return_value={"nb_results": 2, "results": []})
        with self.assertRaises(DataError):
            self.api.news(1, NOW, NOW + timedelta(seconds=1))

    def test_naive_api_news_uses_paris(self):
        article = {"news_id": 5, "title": "News", "published_at": "2026-11-13 10:00:00"}
        self.api.request = Mock(return_value={"nb_results": 1, "results": [article]})
        self.assertEqual(self.api.news(1, NOW, NOW), [article])

    def test_variation_previous_trading_day_and_invalid_close(self):
        history = {"quotes": {
            "2026-11-06": {"1": {"close": "100"}, "2": {"close": 100}},
            "2026-11-07": {"2": {"close": None},
                           "3": {"close": "NaN"}, "4": {"close": 0}, "5": {"close": 100}},
        }}
        self.api.request = Mock(side_effect=lambda route, body=None: history if route == "instrument/quotes" else {
            "instrument": {"quotes": {"last": 90, "last_date": "2026-11-09"}},
        })
        with self.assertLogs("zb_alerting", level="ERROR"):
            self.assertEqual(self.api.variations([1, 2, 3, 4, 6], "2026-11-09"), {1: -10})
        body = self.api.request.call_args_list[0].args[1]
        self.assertEqual(body["interval"], 3600)
        self.assertFalse(body["dividend_adjustment"])
        self.assertEqual(body["end"], "2026-11-09")

    def test_event_query_types_and_pagination(self):
        event = {"date": "2026-11-13", "type": 13, "title": "Results"}
        self.api.request = Mock(side_effect=[{"events": [event] * 500}, {"events": []}])
        self.assertEqual(len(self.api.events(1, "2026-11-13", "2026-11-20")), 500)
        self.assertIn("types=13", self.api.request.call_args.args[0])
        self.assertIn("offset=500", self.api.request.call_args.args[0])

    def test_calendar_without_filter_requests_all_types(self):
        for value in ([], None):
            if value is None:
                self.api.config.raw.pop("earnings_event_types", None)
            else:
                self.api.config.raw["earnings_event_types"] = value
            self.api.config.validate()
            self.api.request = Mock(return_value={"events": []})
            self.assertEqual(self.api.events(1, "2026-11-13", "2026-11-20"), [])
            self.assertNotIn("types=", self.api.request.call_args.args[0])

    def test_expired_token_error_and_headers(self):
        self.api.base = "https://api.example.test/v1/internal"
        self.api.token = "not-a-real-token"
        error = HTTPError(self.api.base, 401, "Unauthorized", {}, None)
        with patch("alerting.urlopen", side_effect=error) as opener:
            with self.assertRaisesRegex(ApiError, "renew the token file"):
                self.api.request("fonds/composition")
        request = opener.call_args.args[0]
        self.assertEqual(request.get_header("Authorization"), "Bearer not-a-real-token")
        self.assertEqual(opener.call_count, 1)


class OperationalTests(unittest.TestCase):
    def test_api_pacing_serializes_concurrent_request_starts(self):
        api = Api(settings(Path(".")), token="not-a-real-token")
        clock = [100.0]
        waits = []
        def wait(seconds):
            waits.append(seconds)
            clock[0] += seconds
        with patch("alerting.time.monotonic", side_effect=lambda: clock[0]), \
             patch("alerting.time.sleep", side_effect=wait):
            threads = [threading.Thread(target=api.pace) for _ in range(4)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        self.assertEqual(waits, [0.5, 0.5, 0.5])
        self.assertEqual(api._next_call, 102.0)

    def test_api_load_settings_validation(self):
        config = settings(Path("."))
        self.assertEqual(config.api_workers, 2)
        self.assertEqual(config.api_min_interval_seconds, 0.5)
        for value in (0, -1, True, float("nan"), "0.5"):
            config.raw["api_min_interval_seconds"] = value
            with self.assertRaises(ValueError):
                config.validate()
        config.raw["api_min_interval_seconds"] = 0.5
        for value in (0, 9, True, 1.5):
            config.raw["api_workers"] = value
            with self.assertRaises(ValueError):
                config.validate()

    def test_quote_diagnostic_needs_no_smtp_and_creates_no_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "config.json"
            path.write_text(json.dumps({"token_file": "token"}), encoding="utf-8")
            api = Mock()
            api.quote_comparisons.return_value = {4941: {
                "last": 115, "last_date": "2026-11-13", "previous_close": 100,
                "previous_date": "2026-11-12", "is_today": True, "variation_percent": 15,
            }}
            with patch("alerting.Api", return_value=api), patch("builtins.print") as output:
                self.assertEqual(main(["--config", str(path), "--quote-info", "4941"]), 0)
            result = json.loads(output.call_args.args[0])
            self.assertEqual(result["last_date"], "2026-11-13")
            self.assertEqual(result["variation_percent"], 15)
            api.request.assert_not_called()
            self.assertEqual(list(root.iterdir()), [path])

    def test_disabled_price_delivery_leaves_pending_alerts_untouched(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = Store(Path(temporary) / "state.duckdb")
            try:
                with store.transaction():
                    store.queue("price", ["price"], "Price", "Body", NOW)
                mailer = Mock()
                self.assertTrue(deliver(store, mailer, NOW, include_prices=False))
                mailer.send.assert_not_called()
                from alerting import display
                with patch("builtins.print") as output:
                    self.assertTrue(display(store, NOW, include_prices=False))
                    output.assert_not_called()
                self.assertEqual(store.db.execute("SELECT status FROM outbox").fetchone()[0], "pending")
            finally:
                store.close()

    def test_interactive_login_creates_token_without_other_configuration(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = settings(root)
            config.raw["earnings_event_types"] = []
            config.raw["smtp"] = {}
            path = root / "config.json"
            path.write_text(json.dumps(config.raw), encoding="utf-8")
            response = Mock()
            response.read.return_value = b'{"error":false,"token":"temporary-test-token"}'
            response.__enter__ = Mock(return_value=response)
            response.__exit__ = Mock(return_value=False)
            with patch("alerting.sys.stdin.isatty", return_value=True), \
                 patch("builtins.input", return_value="user@example.test"), \
                 patch("alerting.getpass", side_effect=["test-password", "123456"]), \
                 patch("alerting.urlopen", return_value=response) as opener:
                self.assertEqual(main(["--config", str(path), "--login"]), 0)
            self.assertEqual((root / "token").read_text(), "temporary-test-token")
            request = opener.call_args.args[0]
            self.assertTrue(request.full_url.endswith("/login"))
            self.assertEqual(parse_qs(request.data.decode()), {
                "login": ["user@example.test"], "password": ["test-password"], "otp": ["123456"],
            })
            self.assertIsNone(request.get_header("Authorization"))
            self.assertEqual(list(root.glob(".zb-token-*")), [])
            self.assertFalse((root / "data").exists())
            if os.name != "nt":
                self.assertEqual((root / "token").stat().st_mode & 0o777, 0o600)
            self.assertEqual(Api(config).token, "temporary-test-token")

    def test_login_refusal_and_invalid_payload_preserve_old_token(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = settings(root)
            (root / "token").write_text("previous-token")
            for data in (b'{"error":true}', b'{"token":""}', b'{"token":"bad token"}', b'not-json'):
                response = Mock()
                response.read.return_value = data
                response.__enter__ = Mock(return_value=response)
                response.__exit__ = Mock(return_value=False)
                with patch("alerting.sys.stdin.isatty", return_value=True), \
                     patch("builtins.input", return_value="user@example.test"), \
                     patch("alerting.getpass", side_effect=["test-password", "123456"]), \
                     patch("alerting.urlopen", return_value=response):
                    with self.assertRaises(DataError):
                        Api.login(config)
                self.assertEqual((root / "token").read_text(), "previous-token")
            with patch("alerting.sys.stdin.isatty", return_value=True), \
                 patch("builtins.input", return_value="user@example.test"), \
                 patch("alerting.getpass", side_effect=["test-password", "123456"]), \
                 patch("alerting.urlopen", side_effect=HTTPError("https://example.test", 401, "", {}, None)):
                with self.assertRaisesRegex(ApiError, "HTTP 401"):
                    Api.login(config)
            self.assertEqual((root / "token").read_text(), "previous-token")

    def test_login_without_terminal_or_invalid_otp_does_not_call_api(self):
        with patch("alerting.sys.stdin.isatty", return_value=False), \
             patch("alerting.urlopen") as opener:
            with self.assertRaisesRegex(ValueError, "interactive terminal"):
                Api.login(settings(Path(".")))
            opener.assert_not_called()
        with patch("alerting.sys.stdin.isatty", return_value=True), \
             patch("builtins.input", return_value="user@example.test"), \
             patch("alerting.getpass", side_effect=["test-password", "123"]), \
             patch("alerting.urlopen") as opener:
            with self.assertRaises(ValueError):
                Api.login(settings(Path(".")))
            opener.assert_not_called()

    def test_lock_excludes_second_run(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "cron.lock"
            with job_lock(path):
                with self.assertRaises(AlreadyRunning):
                    with job_lock(path):
                        self.fail("overlapping run acquired the lock")
            with job_lock(path):
                pass

    def test_configuration_accepts_all_event_types_and_rejects_invalid_values(self):
        config = settings(Path("."))
        config.validate()
        config.raw["earnings_event_types"] = []
        config.validate()
        config.raw["earnings_event_types"] = ["invalid"]
        with self.assertRaisesRegex(ValueError, "earnings_event_types"):
            config.validate()
        config.raw["earnings_event_types"] = [13]
        config.raw["smtp"]["recipients"] = []
        with self.assertRaisesRegex(ValueError, "recipient"):
            config.validate()

    def test_smtp_tls_message_id_and_partial_delivery(self):
        config = settings(Path("."))
        with patch("alerting.smtplib.SMTP") as smtp:
            server = smtp.return_value
            server.send_message.return_value = {}
            mailer = Mailer(config.raw["smtp"])
            mailer.send("stable-id", "News", "Body")
            server.starttls.assert_called_once()
            message = server.send_message.call_args.args[0]
            self.assertEqual(message["Message-ID"], "<zb-alert-stable-id@example.test>")
            server.close.assert_called_once()
            server.send_message.return_value = {"team@example.test": (550, b"rejected")}
            with self.assertRaisesRegex(RuntimeError, "partially"):
                mailer.send("stable-id", "News", "Body")

    def test_dry_run_does_not_send_or_persist_monitoring_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = settings(root)
            config_path = root / "config.json"
            config_path.write_text(json.dumps(config.raw), encoding="utf-8")
            fake = FakeApi()
            fake.moves[1] = 15
            with patch("alerting.Api", return_value=fake), patch("alerting.Mailer") as mailer:
                with patch("builtins.print") as output:
                    self.assertEqual(main(["--config", str(config_path), "--dry-run"]), 0)
                self.assertIn("+15.00", output.call_args.args[0])
                mailer.assert_not_called()
            self.assertFalse((root / "data" / "state.duckdb").exists())

    def test_main_persistent_run_outbox_and_resolution(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = settings(root)
            config_path = root / "config.json"
            config_path.write_text(json.dumps(config.raw), encoding="utf-8")
            fake = FakeApi()
            fake.moves[1] = 5
            with patch("alerting.Api", return_value=fake), patch("alerting.Mailer") as mailer:
                self.assertEqual(main(["--config", str(config_path)]), 0)
                self.assertEqual(main(["--config", str(config_path)]), 0)
                mailer.return_value.send.assert_called_once()
            store = Store(root / "data" / "state.duckdb")
            identity = store.db.execute("SELECT id FROM outbox").fetchone()[0]
            store.set_status(identity, "uncertain", NOW)
            store.close()
            with patch("builtins.print") as output:
                self.assertEqual(main(["--config", str(config_path), "--outbox"]), 0)
                self.assertIn("uncertain", output.call_args.args[0])
            self.assertEqual(main(["--config", str(config_path), "--resolve", identity, "sent"]), 0)
            store = Store(root / "data" / "state.duckdb")
            self.assertEqual(store.db.execute("SELECT status FROM outbox").fetchone()[0], "sent")
            store.close()

    def test_console_display_persists_without_smtp_or_replay(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = settings(root)
            config.raw["delivery"] = "console"
            config.raw.pop("smtp")
            config_path = root / "config.json"
            config_path.write_text(json.dumps(config.raw), encoding="utf-8")
            fake = FakeApi()
            fake.moves[1] = 15
            with patch("alerting.Api", return_value=fake), patch("alerting.Mailer") as mailer:
                with patch("builtins.print") as output:
                    self.assertEqual(main(["--config", str(config_path)]), 0)
                    self.assertIn("+15.00", output.call_args.args[0])
                    self.assertEqual(main(["--config", str(config_path)]), 0)
                    output.assert_called_once()
                mailer.assert_not_called()
            store = Store(root / "data" / "state.duckdb")
            self.assertEqual(store.db.execute("SELECT status FROM outbox").fetchone()[0], "displayed")
            store.close()
            config.raw["delivery"] = "smtp"
            config.raw["smtp"] = settings(root).raw["smtp"]
            config_path.write_text(json.dumps(config.raw), encoding="utf-8")
            with patch("alerting.Api", return_value=fake), patch("alerting.Mailer") as mailer:
                self.assertEqual(main(["--config", str(config_path)]), 0)
                mailer.return_value.send.assert_not_called()


if __name__ == "__main__":
    unittest.main()
