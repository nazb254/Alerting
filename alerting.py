"""One-shot portfolio monitoring job, intended to run every minute."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time as day_time, timedelta, timezone
from email.message import EmailMessage
from email.utils import parseaddr
from getpass import getpass
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import shutil
import smtplib
import ssl
import stat
import sys
import tempfile
import threading
import time
from typing import Any, Iterator
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import duckdb


LOG = logging.getLogger("zb_alerting")
UTC = timezone.utc
PORTFOLIOS = ("europe", "usa", "asie", "europaone")
LABELS = {
    "europe": "Portefeuille Europe", "usa": "Portefeuille USA",
    "asie": "Portefeuille Asie", "europaone": "Fonds Europa One",
}


class DataError(ValueError):
    pass


class ApiError(RuntimeError):
    pass


class AlreadyRunning(RuntimeError):
    pass


class NotSubmitted(RuntimeError):
    """SMTP failed before any message content was submitted."""


def utc_now() -> datetime:
    return datetime.now(UTC)


def timestamp(value: str, tz: ZoneInfo) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=tz).astimezone(UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def rows(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    result = payload.get(key)
    if not isinstance(result, list) or any(not isinstance(item, dict) for item in result):
        raise DataError(f"API: expected an array of objects in {key}")
    return result


def codezb(value: Any) -> int:
    if isinstance(value, bool) or not str(value).isdigit() or int(value) <= 0:
        raise DataError("API: invalid codezb")
    return int(value)


@dataclass(frozen=True)
class Config:
    raw: dict[str, Any]
    root: Path

    @classmethod
    def load(cls, path: Path) -> Config:
        with path.open(encoding="utf-8") as handle:
            raw = json.load(handle)
        if not isinstance(raw, dict):
            raise ValueError("Configuration must be a JSON object")
        return cls(raw, path.resolve().parent)

    def path(self, key: str) -> Path:
        path = Path(self.raw[key])
        return path if path.is_absolute() else self.root / path

    @property
    def tz(self) -> ZoneInfo:
        return ZoneInfo(self.raw.get("timezone", "Europe/Paris"))

    @property
    def daily_time(self) -> day_time:
        value = self.raw.get("daily_check_time", "01:00")
        if len(value) != 5:
            raise ValueError("daily_check_time must use HH:MM")
        return datetime.strptime(value, "%H:%M").time()

    def monitoring_date(self, now: datetime) -> date:
        local = now.astimezone(self.tz)
        return (local - timedelta(hours=1)).date()

    @property
    def delivery(self) -> str:
        return self.raw.get("delivery", "smtp")

    @property
    def price_alerts_enabled(self) -> bool:
        value = self.raw.get("price_alerts_enabled", True)
        if not isinstance(value, bool):
            raise ValueError("price_alerts_enabled must be a boolean")
        return value

    @property
    def api_workers(self) -> int:
        value = self.raw.get("api_workers", 2)
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 8:
            raise ValueError("api_workers must be an integer between 1 and 8")
        return value

    @property
    def api_min_interval_seconds(self) -> float:
        value = self.raw.get("api_min_interval_seconds", 0.5)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError("api_min_interval_seconds must be a positive finite number")
        return float(value)

    def validate(self, *, require_mail_password: bool = True, require_smtp: bool = True) -> None:
        self.tz
        self.daily_time
        self.price_alerts_enabled
        self.api_workers
        self.api_min_interval_seconds
        portfolios = self.raw.get("portfolios", list(PORTFOLIOS))
        if not isinstance(portfolios, list) or not portfolios or any(p not in PORTFOLIOS for p in portfolios):
            raise ValueError("portfolios must contain supported portfolio names")
        types = self.raw.get("earnings_event_types", [])
        if not isinstance(types, list) or any(
            isinstance(t, bool) or not isinstance(t, int) or t <= 0 for t in types
        ):
            raise ValueError("earnings_event_types must be a list of positive integer IDs, or [] for all events")
        if self.delivery not in ("console", "smtp"):
            raise ValueError("delivery must be console or smtp")
        if not require_smtp or self.delivery == "console":
            return
        smtp = self.raw["smtp"]
        if not smtp.get("host") or smtp.get("security") not in ("starttls", "ssl"):
            raise ValueError("SMTP host and TLS security (starttls or ssl) are required")
        if not isinstance(smtp.get("recipients"), list) or not smtp["recipients"]:
            raise ValueError("At least one SMTP recipient is required")
        for address in [smtp.get("sender", ""), *smtp["recipients"]]:
            if not isinstance(address, str) or "\n" in address or "\r" in address:
                raise ValueError("Invalid mail address")
            parsed = parseaddr(address)[1]
            if not parsed or "@" not in parsed:
                raise ValueError("Invalid mail address")
        port = smtp.get("port", 587)
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError("Invalid SMTP port")
        if require_mail_password and smtp.get("username") and not os.environ.get(
            smtp.get("password_env", "ZB_ALERT_SMTP_PASSWORD")
        ):
            raise ValueError("SMTP password environment variable is missing")


@contextmanager
def job_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        if os.name == "nt":
            import msvcrt
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as error:
                raise AlreadyRunning("Another run holds the job lock") from error
        else:
            import fcntl
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise AlreadyRunning("Another run holds the job lock") from error
        try:
            yield
        finally:
            if os.name == "nt":
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class Api:
    cache_store: Store | None = None

    def __init__(self, config: Config, *, token: str | None = None):
        self.config = config
        self._pace_lock = threading.Lock()
        self._next_call = 0.0
        self.base = config.raw.get("api_base_url", "https://api.zonebourse.com/v1/internal").rstrip("/")
        if urlsplit(self.base).scheme != "https":
            raise ValueError("The API URL must use HTTPS")
        if token is not None:
            self.token = token
            return
        path = config.path("token_file")
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("Token must be a regular file, not a symlink")
        if os.name != "nt" and info.st_mode & 0o077:
            raise ValueError("Token file must not be accessible to group/others (chmod 600)")
        self.token = path.read_text(encoding="utf-8").strip()
        if not self.token or any(c.isspace() for c in self.token):
            raise ValueError("Token file must contain only the raw JWT")

    def pace(self) -> None:
        with self._pace_lock:
            delay = self._next_call - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            self._next_call = time.monotonic() + self.config.api_min_interval_seconds

    def request(self, route: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self.token}", "x-locale": "fr_FR",
            "x-version": "1.0.0", "User-Agent": "Marketscreener/1.0.0 SURPERFORMANCE/App",
        }
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        request = Request(f"{self.base}/{route}", data=data, headers=headers)
        for attempt in range(3):
            self.pace()
            try:
                with urlopen(request, timeout=15) as response:
                    payload = json.load(response)
                if not isinstance(payload, dict) or payload.get("error"):
                    raise DataError(f"API error or invalid object on {route.split('?')[0]}")
                return payload
            except HTTPError as error:
                status = error.code
                error.close()
                if status not in (429, 500, 502, 503, 504) or attempt == 2:
                    hint = " (renew the token file)" if status == 401 else ""
                    raise ApiError(f"API HTTP {status} on {route.split('?')[0]}{hint}") from None
            except (URLError, TimeoutError) as error:
                if attempt == 2:
                    raise ApiError(f"API connection failed on {route.split('?')[0]}") from error
            time.sleep(attempt + 1)
        raise AssertionError("unreachable")

    @classmethod
    def login(cls, config: Config) -> None:
        if not sys.stdin.isatty():
            raise ValueError("Login requires an interactive terminal; never use --login in cron")
        api = cls(config, token="")
        login = input("Identifiant ZB (email) : ").strip()
        password = getpass("Mot de passe ZB : ")
        otp = getpass("Code OTP (6 chiffres) : ").strip()
        if not login or not password or len(otp) != 6 or not otp.isascii() or not otp.isdigit():
            raise ValueError("Identifiant, mot de passe et code OTP a 6 chiffres requis")
        request = Request(
            f"{api.base}/login",
            data=urlencode({"login": login, "password": password, "otp": otp}).encode(),
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "Marketscreener/1.0.0 SURPERFORMANCE/App",
            },
        )
        try:
            with urlopen(request, timeout=15) as response:
                payload = json.load(response)
        except HTTPError as error:
            status = error.code
            error.close()
            raise ApiError(f"ZB login refused (HTTP {status}); verify credentials and OTP") from None
        except (URLError, TimeoutError):
            raise ApiError("ZB login connection failed; retry manually") from None
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise DataError("ZB login returned an invalid response") from None
        token = payload.get("token") if isinstance(payload, dict) and not payload.get("error") else None
        if not isinstance(token, str) or not token or any(c.isspace() for c in token):
            raise DataError("ZB login did not return a usable token")
        path = config.path("token_file")
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.is_symlink() or (path.exists() and not path.is_file()):
            raise ValueError("Token destination must be a regular file, not a symlink")
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", prefix=".zb-token-", dir=path.parent, delete=False,
            ) as handle:
                temporary = Path(handle.name)
                if os.name != "nt":
                    os.chmod(temporary, 0o600)
                handle.write(token)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        LOG.info("Connexion ZB reussie. Jeton temporaire enregistre ; renouveler avec --login a expiration.")

    def holdings(self) -> dict[int, dict[str, Any]]:
        holdings: dict[int, dict[str, Any]] = {}
        for portfolio in self.config.raw.get("portfolios", list(PORTFOLIOS)):
            fund = portfolio == "europaone"
            route = "fonds/composition" if fund else f"portfolios/{portfolio}/summary"
            for item in rows(self.request(route), "holdings" if fund else "open_trades"):
                code = codezb(item.get("codeZB" if fund else "codezb"))
                name = str(item.get("nom" if fund else "name") or code).replace("_", " ")
                holding = holdings.setdefault(code, {"name": name, "portfolios": []})
                if portfolio not in holding["portfolios"]:
                    holding["portfolios"].append(portfolio)
        return holdings

    def daily_quotes(self, codes: list[int], today: str) -> dict[int, list[tuple[str, float]]]:
        result: dict[int, list[tuple[str, float]]] = {code: [] for code in codes}
        start_day = (datetime.strptime(today, "%Y-%m-%d").date() - timedelta(days=30)).isoformat()
        for start in range(0, len(codes), 200):
            batch = codes[start:start + 200]
            payload = self.request("instrument/quotes", {
                "codezb": batch, "start": start_day, "end": today,
                "interval": 3600, "dividend_adjustment": False,
            })
            quotes = payload.get("quotes")
            if not isinstance(quotes, dict):
                raise DataError("API: missing quotes")
            by_day: dict[int, dict[str, tuple[str, float]]] = {code: {} for code in batch}
            invalid: set[int] = set()
            for date_key, daily in sorted(quotes.items()):
                try:
                    day = datetime.fromisoformat(date_key.replace("Z", "+00:00")).date().isoformat()
                except (ValueError, TypeError):
                    raise DataError("API: invalid quote date") from None
                if day > today:
                    raise DataError("API: quote date is in the future")
                if not isinstance(daily, dict):
                    raise DataError("API: invalid daily quotes")
                for code in batch:
                    if str(code) not in daily:
                        continue
                    candle = daily[str(code)]
                    value = candle.get("close") if isinstance(candle, dict) else None
                    try:
                        if value is None or isinstance(value, bool):
                            raise ValueError
                        number = float(value)
                        if not math.isfinite(number) or number <= 0:
                            raise ValueError
                    except (TypeError, ValueError):
                        # Do not silently substitute an earlier valid candle.
                        LOG.error("Invalid close for codezb=%s on %s; price alert suppressed", code, day)
                        invalid.add(code)
                        continue
                    by_day[code][day] = (date_key, number)
            for code in batch:
                if code in invalid:
                    del result[code]
                    continue
                result[code] = [(day, candle[1]) for day, candle in sorted(by_day[code].items())]
        return result

    def reference_quotes(self, codes: list[int], today: str) -> dict[int, list[tuple[str, float]]]:
        store = self.cache_store
        if store is None:
            return self.daily_quotes(codes, today)
        cached = {
            code: [(day, close)] for code, day, close in store.db.execute(
                "SELECT code, quote_date, close FROM quote_references WHERE monitoring_day = ?", [today],
            ).fetchall() if code in codes
        }
        missing = [code for code in codes if code not in cached]
        if missing:
            loaded = self.daily_quotes(missing, today)
            with store.transaction():
                for code, series in loaded.items():
                    prior = [(day, close) for day, close in series if day < today]
                    if not prior:
                        LOG.error("No previous session close for codezb=%s; reference not cached", code)
                        continue
                    day, close = prior[-1]
                    store.db.execute(
                        "INSERT OR REPLACE INTO quote_references VALUES (?, ?, ?, ?)", [today, code, day, close],
                    )
                    cached[code] = [(day, close)]
                store.db.execute("DELETE FROM quote_references WHERE monitoring_day != ?", [today])
        return cached

    def quote_comparisons(self, codes: list[int], today: str) -> dict[int, dict[str, Any]]:
        history = self.reference_quotes(codes, today)
        result: dict[int, dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=self.config.api_workers) as executor:
            tasks = {executor.submit(self.request, f"instrument/{code}"): code for code in history}
            for future in as_completed(tasks):
                code = tasks[future]
                try:
                    payload = future.result()
                    instrument = payload.get("instrument")
                    quotes = instrument.get("quotes") if isinstance(instrument, dict) else None
                    if not isinstance(quotes, dict):
                        raise DataError("API: missing instrument.quotes")
                    raw_date = quotes.get("last_date")
                    value = raw_date.get("date") if isinstance(raw_date, dict) else raw_date
                    if not isinstance(value, str) or not value.strip():
                        raise DataError("API: missing last_date")
                    # Symfony serializes DateTime as {date, timezone_type, timezone}.
                    last_date = datetime.fromisoformat(value.strip().replace("Z", "+00:00")).date().isoformat()
                    last = quotes.get("last")
                    if last is None or isinstance(last, bool):
                        raise DataError("API: missing or invalid last")
                    last = float(last)
                    if not math.isfinite(last) or last <= 0:
                        raise DataError("API: last must be positive and finite")
                    series = [(day, close) for day, close in history[code] if day < today]
                    if not series:
                        raise DataError("API: no previous session close in the last 30 days")
                    previous_date, previous_close = series[-1]
                    variation = (last - previous_close) / previous_close * 100
                    if not math.isfinite(variation):
                        raise DataError("API: non-finite computed variation")
                    result[code] = {
                        "last": last, "last_date": last_date,
                        "previous_close": previous_close, "previous_date": previous_date,
                        "is_today": last_date == today, "variation_percent": variation,
                    }
                except ApiError as error:
                    LOG.error(
                        "Cours indisponible pour codezb=%s : %s ; aucune alerte cours "
                        "pour ce passage, nouvelle tentative au prochain lancement",
                        code, error,
                    )
                except (DataError, ValueError, TypeError, OSError):
                    LOG.exception("Quote comparison failed for codezb=%s; price alert suppressed", code)
        return result

    def variations(self, codes: list[int], today: str) -> dict[int, float | None]:
        result: dict[int, float | None] = {}
        for code, comparison in self.quote_comparisons(codes, today).items():
            if not comparison["is_today"]:
                result[code] = None
            else:
                result[code] = comparison["variation_percent"]
        return result

    def news(self, code: int, start: datetime, end: datetime) -> list[dict[str, Any]]:
        """Split saturated time windows rather than silently truncating news."""
        lower, upper = math.floor(start.timestamp()), math.ceil(end.timestamp())

        def fetch(first: int, last: int) -> list[dict[str, Any]]:
            payload = self.request("instrument/news-screener", {
                "fIC": [str(code)], "fDf": first, "fDt": last, "rows": 500,
            })
            items = rows(payload, "results")
            count = payload.get("nb_results")
            if isinstance(count, bool) or not isinstance(count, int) or count < len(items):
                raise DataError("API: invalid news count")
            if count > len(items) or len(items) >= 500:
                if last - first <= 1:
                    raise DataError(f"News window saturated for codezb={code}; cursor not advanced")
                middle = (first + last) // 2
                return fetch(first, middle) + fetch(middle, last)
            return items

        unique = {}
        for item in fetch(lower, upper):
            news_id = codezb(item.get("news_id"))
            if not isinstance(item.get("published_at"), str) or not item.get("title"):
                raise DataError("API: news is missing publication time or title")
            published = timestamp(item["published_at"], self.config.tz)
            if start <= published <= end:
                unique[news_id] = item
        return list(unique.values())

    def events(self, code: int, today: str, end: str) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        types = self.config.raw.get("earnings_event_types", [])
        offset = 0
        while True:
            parameters = {"start": today, "end": end, "limit": 500, "offset": offset}
            if types:
                parameters["types"] = ",".join(map(str, types))
            query = urlencode(parameters)
            page = rows(self.request(f"instrument/{code}/events?{query}"), "events")
            result.extend(page)
            if len(page) < 500:
                break
            offset += len(page)
            if offset >= 10000:
                raise DataError(f"Unbounded events pagination for codezb={code}")
        return result


class Store:
    def __init__(self, path: Path | str):
        self.db = duckdb.connect(str(path))
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS metadata (key VARCHAR PRIMARY KEY, value VARCHAR NOT NULL);
            CREATE TABLE IF NOT EXISTS news_cursors (code BIGINT PRIMARY KEY, checked_at VARCHAR NOT NULL);
            CREATE TABLE IF NOT EXISTS holdings (
                code BIGINT PRIMARY KEY, name VARCHAR NOT NULL, portfolios VARCHAR NOT NULL
            );
            CREATE TABLE IF NOT EXISTS quote_references (
                monitoring_day VARCHAR NOT NULL, code BIGINT NOT NULL,
                quote_date VARCHAR NOT NULL, close DOUBLE NOT NULL,
                PRIMARY KEY (monitoring_day, code)
            );
            CREATE TABLE IF NOT EXISTS outbox (
                id VARCHAR PRIMARY KEY, kind VARCHAR NOT NULL, subject VARCHAR NOT NULL,
                body VARCHAR NOT NULL, status VARCHAR NOT NULL, created_at VARCHAR NOT NULL,
                updated_at VARCHAR NOT NULL
            );
            CREATE TABLE IF NOT EXISTS alert_keys (
                key VARCHAR PRIMARY KEY, outbox_id VARCHAR NOT NULL
            );
        """)

    def close(self) -> None:
        self.db.close()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.db.execute("BEGIN")
        try:
            yield
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        else:
            self.db.execute("COMMIT")

    def get(self, key: str) -> str | None:
        row = self.db.execute("SELECT value FROM metadata WHERE key = ?", [key]).fetchone()
        return row[0] if row else None

    def set(self, key: str, value: str) -> None:
        self.db.execute("INSERT OR REPLACE INTO metadata VALUES (?, ?)", [key, value])

    def unseen(self, keys: list[str]) -> list[str]:
        return [key for key in keys if not self.db.execute(
            "SELECT 1 FROM alert_keys WHERE key = ?", [key],
        ).fetchone()]

    def queue(self, kind: str, keys: list[str], subject: str, body: str, now: datetime) -> str | None:
        keys = self.unseen(keys)
        if not keys:
            return None
        identity = hashlib.sha256("\n".join(sorted(keys)).encode()).hexdigest()
        at = now.isoformat()
        self.db.execute("INSERT INTO outbox VALUES (?, ?, ?, ?, 'pending', ?, ?)",
                        [identity, kind, subject, body, at, at])
        self.db.executemany("INSERT INTO alert_keys VALUES (?, ?)", [(key, identity) for key in keys])
        return identity

    def set_status(self, identity: str, status: str, now: datetime) -> None:
        self.db.execute("UPDATE outbox SET status = ?, updated_at = ? WHERE id = ?",
                        [status, now.isoformat(), identity])

    def export(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        for table in ("metadata", "news_cursors", "holdings", "quote_references", "outbox", "alert_keys"):
            target = directory / f"{table}.parquet"
            temporary = target.with_suffix(".parquet.tmp")
            quoted = str(temporary).replace("'", "''")
            try:
                self.db.execute(f"COPY {table} TO '{quoted}' (FORMAT PARQUET, COMPRESSION ZSTD)")
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)


class Mailer:
    def __init__(self, settings: dict[str, Any]):
        self.settings = settings

    def send(self, identity: str, subject: str, body: str) -> None:
        settings = self.settings
        message = EmailMessage()
        message["From"] = settings["sender"]
        message["To"] = ", ".join(settings["recipients"])
        message["Subject"] = subject
        domain = parseaddr(settings["sender"])[1].split("@")[1]
        message["Message-ID"] = f"<zb-alert-{identity}@{domain}>"
        message.set_content(body)
        server = None
        try:
            context = ssl.create_default_context()
            if settings["security"] == "ssl":
                server = smtplib.SMTP_SSL(settings["host"], settings["port"], timeout=15, context=context)
            else:
                server = smtplib.SMTP(settings["host"], settings["port"], timeout=15)
                server.starttls(context=context)
            if settings.get("username"):
                server.login(settings["username"], os.environ[settings.get("password_env", "ZB_ALERT_SMTP_PASSWORD")])
        except (OSError, smtplib.SMTPException) as error:
            if server is not None:
                server.close()
            raise NotSubmitted("SMTP connection/authentication failed before submission") from error
        try:
            refused = server.send_message(message)
            if refused:
                raise RuntimeError("SMTP partially refused recipients; manual resolution required")
        finally:
            server.close()


class Monitor:
    def __init__(self, config: Config, store: Store, api: Api):
        self.config, self.store, self.api = config, store, api
        self.api.cache_store = store

    def daily_holdings(self, today: str) -> dict[int, dict[str, Any]]:
        store = self.store
        scope = json.dumps(sorted(self.config.raw.get("portfolios", list(PORTFOLIOS))))
        if store.get("holdings_checked") == today and store.get("holdings_scope") == scope:
            return {
                code: {"name": name, "portfolios": json.loads(portfolios)}
                for code, name, portfolios in store.db.execute("SELECT code, name, portfolios FROM holdings").fetchall()
            }
        holdings = self.api.holdings()
        with store.transaction():
            store.db.execute("DELETE FROM holdings")
            if holdings:
                store.db.executemany("INSERT INTO holdings VALUES (?, ?, ?)", [
                    (code, item["name"], json.dumps(item["portfolios"])) for code, item in holdings.items()
                ])
            if store.get("holdings_scope") is not None and store.get("holdings_scope") != scope:
                store.set("earnings_checked", "")
            store.set("holdings_checked", today)
            store.set("holdings_scope", scope)
        return holdings

    @staticmethod
    def context(code: int, holding: dict[str, Any]) -> str:
        portfolios = ", ".join(LABELS[p] for p in sorted(holding["portfolios"]))
        return f"Titre : {holding['name']} (codezb {code})\nPortefeuilles : {portfolios}"

    def collect(self, now: datetime) -> bool:
        store = self.store
        if store.get("started_at") is None:
            store.set("started_at", now.isoformat())
        started_value = store.get("started_at")
        assert started_value is not None
        started = timestamp(started_value, self.config.tz)
        ok = True
        local = now.astimezone(self.config.tz)
        monitoring_day = self.config.monitoring_date(now)
        today = monitoring_day.isoformat()
        holdings = self.daily_holdings(today)
        daily_due = local.time() >= self.config.daily_time and store.get("earnings_checked") != today
        try:
            variations = self.api.variations(list(holdings), today) if self.config.price_alerts_enabled else {}
            if self.config.price_alerts_enabled and len(variations) != len(holdings):
                ok = False
            if not self.config.price_alerts_enabled:
                LOG.info("Price alerts disabled pending quote timestamp verification; news/calendar remain active")
            for code, variation in variations.items():
                if variation is None:
                    continue
                direction = "+" if variation >= 0 else "-"
                keys = [f"price:{code}:{today}:{direction}:{level}" for level in (5, 10, 15)
                        if abs(variation) >= level]
                keys = store.unseen(keys)
                if keys:
                    levels = ", ".join(f"{direction}{key.rsplit(':', 1)[1]} %" for key in keys)
                    body = (
                        f"{self.context(code, holdings[code])}\n"
                        f"Variation depuis la cloture precedente : {variation:+.2f} %\n"
                        f"Seuils atteints : {levels}\n"
                        f"Observation : {local.isoformat()}\n"
                        "Source : ZB instrument summary (last) / instrument/quotes (cloture precedente)\n"
                    )
                    with store.transaction():
                        store.queue("price", keys, f"[ZB Cours] {holdings[code]['name']} {variation:+.2f} %", body, now)
        except (ApiError, DataError, ValueError, OSError):
            LOG.exception("Price collection failed")
            ok = False
        daily_ok = True
        news_items: dict[int, tuple[dict[str, Any], set[int]]] = {}
        news_checked: list[int] = []
        with ThreadPoolExecutor(max_workers=self.config.api_workers) as executor:
            tasks = {}
            for code in holdings:
                cursor = store.db.execute("SELECT checked_at FROM news_cursors WHERE code = ?", [code]).fetchone()
                last = timestamp(cursor[0], self.config.tz) if cursor else started
                # Re-read a day for delayed indexing; persistent IDs suppress duplicates.
                since = max(started, last - timedelta(days=1))
                tasks[executor.submit(self.api.news, code, since, now)] = ("news", code)
                if daily_due:
                    end = (monitoring_day + timedelta(days=7)).isoformat()
                    tasks[executor.submit(self.api.events, code, today, end)] = ("earnings", code)
            for future in as_completed(tasks):
                kind, code = tasks[future]
                holding = holdings[code]
                try:
                    items = future.result()
                    if kind == "news":
                        for item in items:
                            news_id = codezb(item.get("news_id"))
                            if news_id not in news_items:
                                news_items[news_id] = (item, set())
                            news_items[news_id][1].add(code)
                        news_checked.append(code)
                        continue
                    with store.transaction():
                        for item in items:
                            event_date = datetime.strptime(item["date"], "%Y-%m-%d").date()
                            event_type = item.get("type")
                            if isinstance(event_type, bool) or not isinstance(event_type, int) or event_type <= 0:
                                raise DataError("API returned an invalid event type")
                            types = self.config.raw.get("earnings_event_types", [])
                            if types and event_type not in types:
                                raise DataError("API returned an unrequested event type")
                            days = (event_date - monitoring_day).days
                            if days not in (7, 3, 1, 0):
                                continue
                            if not item.get("title"):
                                raise DataError("API event is missing its title")
                            # Fiscal identity distinguishes publications on the same date.
                            fiscal = json.dumps(item.get("earning"), sort_keys=True, separators=(",", ":"))
                            identity = f"{event_type}:{fiscal}"
                            if item.get("earning") is None:
                                identity += f":{item['title']}"
                            event_key = hashlib.sha256(identity.encode()).hexdigest()[:16]
                            key = f"earnings:{code}:{event_date}:{event_key}:{days}"
                            label = f"J-{days}" if days else "Jour J"
                            store.queue(
                                "earnings", [key], f"[ZB Agenda {label}] {holding['name']} - {event_date}",
                                f"{self.context(code, holding)}\n\n{item['title']}\n"
                                f"Date prevue : {event_date} ({label})\n"
                                f"Statut API : {item.get('situation') or 'non precise'}\n", now,
                            )
                except (ApiError, DataError, ValueError, KeyError, TypeError, OSError):
                    LOG.exception("%s collection failed for codezb=%s; checkpoint preserved", kind, code)
                    if kind == "earnings":
                        daily_ok = False
                    ok = False
        with store.transaction():
            for news_id, (item, codes) in news_items.items():
                context = "\n\n".join(self.context(code, holdings[code]) for code in sorted(codes))
                names = ", ".join(holdings[code]["name"] for code in sorted(codes))
                store.queue(
                    "news", [f"news:{news_id}"], f"[ZB News] {names} - {item['title']}",
                    f"{context}\n\n{item['title']}\nPublication : {item['published_at']}\n"
                    f"Identifiant news ZB : {news_id}\n", now,
                )
            for code in news_checked:
                store.db.execute("INSERT OR REPLACE INTO news_cursors VALUES (?, ?)", [code, now.isoformat()])
        if daily_due and daily_ok:
            store.set("earnings_checked", today)
        return ok


def deliver(store: Store, mailer: Mailer, now: datetime, *, include_prices: bool = True) -> bool:
    # A previous process may have stopped between SMTP acceptance and local commit.
    store.db.execute("UPDATE outbox SET status = 'uncertain' WHERE status IN ('sending', 'displaying')")
    ok = True
    for identity, subject, body in store.db.execute(
        "SELECT id, subject, body FROM outbox WHERE status = 'pending' AND (? OR kind != 'price') "
        "ORDER BY created_at, id", [include_prices],
    ).fetchall():
        store.set_status(identity, "sending", now)
        try:
            mailer.send(identity, subject, body)
        except NotSubmitted:
            store.set_status(identity, "pending", now)
            LOG.exception("SMTP unavailable before submission; pending alerts retained")
            ok = False
            break
        except Exception:
            store.set_status(identity, "uncertain", now)
            LOG.exception("Mail %s has uncertain delivery; automatic retry disabled", identity)
            ok = False
        else:
            store.set_status(identity, "sent", now)
            LOG.info("Mail sent: %s", identity)
    uncertain = store.db.execute("SELECT count(*) FROM outbox WHERE status = 'uncertain'").fetchone()[0]
    if uncertain:
        LOG.error("%s uncertain deliveries require --resolve", uncertain)
        ok = False
    return ok


def display(store: Store, now: datetime, *, include_prices: bool = True) -> bool:
    store.db.execute("UPDATE outbox SET status = 'uncertain' WHERE status IN ('sending', 'displaying')")
    for identity, subject, body in store.db.execute(
        "SELECT id, subject, body FROM outbox WHERE status = 'pending' AND (? OR kind != 'price') "
        "ORDER BY created_at, id", [include_prices],
    ).fetchall():
        store.set_status(identity, "displaying", now)
        try:
            print(f"\n=== {subject} ===\n{body}\nIdentifiant alerte : {identity}\n", flush=True)
        except (OSError, UnicodeError):
            store.set_status(identity, "uncertain", now)
            LOG.exception("Alert %s could not be displayed reliably", identity)
            return False
        store.set_status(identity, "displayed", now)
    uncertain = store.db.execute("SELECT count(*) FROM outbox WHERE status = 'uncertain'").fetchone()[0]
    if uncertain:
        LOG.error("%s uncertain deliveries require --resolve", uncertain)
    return uncertain == 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path(__file__).with_name("config.json"))
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--login", action="store_true", help="Prompt for ZB credentials/OTP and save a temporary token")
    action.add_argument("--dry-run", action="store_true", help="Collect using a disposable state copy; never send")
    action.add_argument("--event-types", type=int, metavar="CODEZB", help="Print ZB event type IDs for configuration")
    action.add_argument("--quote-info", type=int, metavar="CODEZB", help="Inspect quote date and variation without alerts")
    action.add_argument("--outbox", action="store_true", help="List unresolved messages")
    action.add_argument("--resolve", nargs=2, metavar=("ID", "sent|retry"), help="Manually resolve uncertain delivery")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if os.name != "nt":
        os.umask(0o077)
    try:
        config = Config.load(args.config)
        if args.login:
            Api.login(config)
            return 0
        if args.event_types is not None:
            print(json.dumps(Api(config).request(f"instrument/{codezb(args.event_types)}/events/types"), indent=2))
            return 0
        if args.quote_info is not None:
            code = codezb(args.quote_info)
            api = Api(config)
            today = config.monitoring_date(utc_now()).isoformat()
            comparison = api.quote_comparisons([code], today).get(code)
            if comparison is None:
                raise DataError("API: no usable quote comparison for quote diagnostic")
            print(json.dumps({
                "observed_at": utc_now().isoformat(), "codezb": code,
                **comparison,
            }, indent=2, ensure_ascii=True))
            return 0
        directory = config.path("state_dir")
        with job_lock(directory / "cron.lock"):
            if args.outbox or args.resolve:
                if not (directory / "state.duckdb").is_file():
                    raise ValueError("No monitoring database exists yet")
                store = Store(directory / "state.duckdb")
                try:
                    if args.resolve:
                        identity, decision = args.resolve
                        row = store.db.execute("SELECT status FROM outbox WHERE id = ?", [identity]).fetchone()
                        if decision not in ("sent", "retry") or not row or row[0] not in (
                            "sending", "displaying", "uncertain",
                        ):
                            raise ValueError("Resolution requires an uncertain ID and sent or retry")
                        status = "displayed" if config.delivery == "console" else "sent"
                        store.set_status(identity, status if decision == "sent" else "pending", utc_now())
                        LOG.warning("Manual resolution: %s -> %s", identity, decision)
                        store.export(directory / "parquet")
                    else:
                        for row in store.db.execute(
                            "SELECT id, status, subject FROM outbox WHERE status NOT IN ('sent', 'displayed') "
                            "ORDER BY created_at",
                        ).fetchall():
                            print(json.dumps(row, ensure_ascii=True))
                finally:
                    store.close()
                return 0
            config.validate(require_mail_password=not args.dry_run, require_smtp=not args.dry_run)
            api = Api(config)
            with tempfile.TemporaryDirectory(prefix="zb-alerting-") as temporary:
                state_path = directory / "state.duckdb"
                if args.dry_run:
                    dry_path = Path(temporary) / "state.duckdb"
                    if state_path.exists():
                        # Recover/checkpoint any WAL before copying the database.
                        live = Store(state_path)
                        live.db.execute("CHECKPOINT")
                        live.close()
                        shutil.copy2(state_path, dry_path)
                    state_path = dry_path
                store = Store(state_path)
                try:
                    now = utc_now()
                    collected = False
                    try:
                        collected = Monitor(config, store, api).collect(now)
                    except (ApiError, DataError, ValueError, OSError):
                        LOG.exception("Holdings collection failed; existing outbox retained")
                    if args.dry_run:
                        for row in store.db.execute(
                            "SELECT kind, subject, body FROM outbox WHERE status = 'pending'",
                        ).fetchall():
                            print(json.dumps(row, ensure_ascii=True))
                        sent = True
                    else:
                        sent = display(
                            store, now, include_prices=config.price_alerts_enabled,
                        ) if config.delivery == "console" else deliver(
                            store, Mailer(config.raw["smtp"]), now, include_prices=config.price_alerts_enabled,
                        )
                        store.export(directory / "parquet")
                    return 0 if collected and sent else 1
                finally:
                    store.close()
    except AlreadyRunning:
        LOG.info("Previous cron run still active; skipping this invocation")
        return 0
    except (EOFError, KeyboardInterrupt):
        LOG.error("Operation annulee ; aucun nouveau jeton enregistre")
        return 1
    except Exception:
        LOG.exception("Cron failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
