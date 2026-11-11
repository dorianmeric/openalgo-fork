"""
Market data for IBKR: quotes, depth and history.

The shape of this file is forced by the venue, and it is worth being explicit
about it, because it is unlike every other broker in this tree.

**IBKR has no REST quote endpoint.** TWS streams prices over the socket, so a
one-shot quote is a market-data *snapshot*: connect, request, collect the ticks
until ``tickSnapshotEnd``, disconnect. That is the same trade-off
``broker/aliceblue`` makes -- a quote costs a round trip rather than a cheap GET
-- and it is why :meth:`BrokerData.get_multiquotes` matters here: a per-symbol
loop over an option chain is slow, so batch callers should use it.

**Depth is level 1 only.** A full five-level book needs ``reqMktDepth``, whose
frame layout is not published in the reference this plugin was built from, and
guessing a positional frame corrupts the stream. Rather than invent it, depth
here reports the snapshot's best bid and offer at level 1 and pads levels 2-5
with zeros, which is what the contract asks consumers to tolerate.
"""

from datetime import datetime, timezone

import pandas as pd

from broker.ibkr.api import auth_api, tws_client
from broker.ibkr.mapping.transform_data import build_ibkr_contract
from database.token_db import get_br_symbol, get_symbol_info, get_token
from utils.logging import get_logger

logger = get_logger(__name__)

#: OpenAlgo interval -> IBKR ``barSizeSetting``.
#:
#: IBKR accepts a fixed vocabulary of bar sizes; anything else is rejected outright.
BAR_SIZE_MAP = {
    "1m": "1 min",
    "2m": "2 mins",
    "3m": "3 mins",
    "5m": "5 mins",
    "10m": "10 mins",
    "15m": "15 mins",
    "20m": "20 mins",
    "30m": "30 mins",
    "60m": "1 hour",
    "1h": "1 hour",
    "2h": "2 hours",
    "3h": "3 hours",
    "4h": "4 hours",
    "1d": "1 day",
    "D": "1 day",
    "1w": "1 week",
    "W": "1 week",
    "1M": "1 month",
    "M": "1 month",
}

#: IBKR caps how much history one request may span for a given bar size.
#: Requesting more than the cap fails with error 162 rather than truncating, so
#: the requested range is clamped to the cap instead.
MAX_DURATION_DAYS = {
    "1 min": 1,
    "2 mins": 1,
    "3 mins": 1,
    "5 mins": 2,
    "10 mins": 2,
    "15 mins": 3,
    "20 mins": 3,
    "30 mins": 5,
    "1 hour": 30,
    "2 hours": 30,
    "3 hours": 30,
    "4 hours": 30,
    "1 day": 365,
    "1 week": 365 * 3,
    "1 month": 365 * 5,
}


def _ibkr_duration(days):
    """A day count as an IBKR duration string."""
    if days <= 1:
        return "1 D"
    if days <= 30:
        return f"{int(days)} D"
    if days <= 365:
        return f"{max(1, round(days / 30))} M"
    return f"{max(1, round(days / 365))} Y"


class BrokerData:
    """Quotes, depth and history for one IBKR session.

    ``auth_token`` is the connection descriptor stored at login. The constructor
    takes a single argument on purpose: the service layer inspects the argument
    count and would pass a second *feed token* if this declared one, and IBKR has
    no separate feed credential.
    """

    TIMEFRAME_MAP = BAR_SIZE_MAP

    def __init__(self, auth_token):
        self.auth_token = auth_token
        self.timeframe_map = self.TIMEFRAME_MAP

    # -- internals --

    def _contract(self, symbol, exchange):
        """The IBKR contract for an OpenAlgo symbol, with a resolved ``conId``."""
        contract = build_ibkr_contract(symbol, exchange)
        if not contract.get("conId"):
            token = get_token(symbol, exchange)
            if token:
                try:
                    contract["conId"] = int(token)
                except (TypeError, ValueError):
                    pass
        return contract

    # -- quotes --

    def get_quotes(self, symbol: str, exchange: str) -> dict:
        """One snapshot quote for a single symbol."""
        contract = self._contract(symbol, exchange)
        with auth_api.get_connection(self.auth_token) as connection:
            quote = connection.snapshot(contract)

        return {
            "ask": _f(quote.get("ask")),
            "bid": _f(quote.get("bid")),
            "high": _f(quote.get("high")),
            "low": _f(quote.get("low")),
            "ltp": _f(quote.get("ltp")) or _f(quote.get("close")),
            "open": _f(quote.get("open")),
            "prev_close": _f(quote.get("prev_close")),
            "volume": _f(quote.get("volume")),
            # IBKR does not return open interest in a top-of-book snapshot; the
            # generic tick that carries it is not part of the default set.
            "oi": 0,
        }

    def get_multiquotes(self, symbols: list) -> list:
        """Batch quotes over one connection.

        Each symbol still costs its own snapshot round trip, but sharing the
        connection avoids a handshake per symbol, which is the expensive part.
        """
        results = []
        with auth_api.get_connection(self.auth_token) as connection:
            for entry in symbols:
                symbol = entry.get("symbol") if isinstance(entry, dict) else entry
                exchange = entry.get("exchange", "US") if isinstance(entry, dict) else "US"
                try:
                    contract = self._contract(symbol, exchange)
                    quote = connection.snapshot(contract)
                    results.append(
                        {
                            "symbol": symbol,
                            "exchange": exchange,
                            "ask": _f(quote.get("ask")),
                            "bid": _f(quote.get("bid")),
                            "high": _f(quote.get("high")),
                            "low": _f(quote.get("low")),
                            "ltp": _f(quote.get("ltp")) or _f(quote.get("close")),
                            "open": _f(quote.get("open")),
                            "prev_close": _f(quote.get("prev_close")),
                            "volume": _f(quote.get("volume")),
                            "oi": 0,
                        }
                    )
                except tws_client.TwsError as exc:
                    logger.warning("Snapshot failed for %s: %s", symbol, exc)
                    results.append({"symbol": symbol, "exchange": exchange, "error": str(exc)})
        return results

    # -- depth --

    def get_depth(self, symbol: str, exchange: str) -> dict:
        """Best bid/offer, padded to the five levels the contract requires.

        Levels 2-5 are zero-filled: a real book needs ``reqMktDepth``, which this
        plugin does not send. See the module docstring.
        """
        contract = self._contract(symbol, exchange)
        with auth_api.get_connection(self.auth_token) as connection:
            quote = connection.snapshot(contract)

        bid = _f(quote.get("bid"))
        ask = _f(quote.get("ask"))
        bid_size = _f(quote.get("bid_size"))
        ask_size = _f(quote.get("ask_size"))

        bids = [{"price": bid, "quantity": bid_size}] if bid else []
        asks = [{"price": ask, "quantity": ask_size}] if ask else []
        # Consumers index into five levels, so pad rather than leave short
        # arrays: a two-element list where five are expected breaks the UI, and
        # truncation alone does not cover the fewer-than-five case.
        while len(bids) < 5:
            bids.append({"price": 0, "quantity": 0})
        while len(asks) < 5:
            asks.append({"price": 0, "quantity": 0})

        return {
            "asks": asks[:5],
            "bids": bids[:5],
            "high": _f(quote.get("high")),
            "low": _f(quote.get("low")),
            "ltp": _f(quote.get("ltp")) or _f(quote.get("close")),
            "ltq": _f(quote.get("last_size")),
            "oi": 0,
            "open": _f(quote.get("open")),
            "prev_close": _f(quote.get("prev_close")),
            "totalbuyqty": bid_size,
            "totalsellqty": ask_size,
            "volume": _f(quote.get("volume")),
        }

    # -- history --

    def get_history(
        self, symbol: str, exchange: str, timeframe: str, from_date: str, to_date: str
    ) -> pd.DataFrame:
        """Historical bars as a DataFrame of epoch-second timestamps.

        IBKR paces historical requests hard -- no identical request within 15
        seconds, at most 60 in ten minutes, and ``BID_ASK`` counts double. A long
        range on a fine interval therefore cannot be fetched in one call, and this
        method clamps to IBKR's per-bar-size cap instead of pretending otherwise.
        """
        bar_size = BAR_SIZE_MAP.get(timeframe)
        if not bar_size:
            raise ValueError(
                f"Timeframe {timeframe!r} is not supported by IBKR. "
                f"Supported: {', '.join(sorted(BAR_SIZE_MAP))}"
            )

        start = _parse_date(from_date)
        end = _parse_date(to_date)
        if start is None or end is None:
            raise ValueError("from_date and to_date must be YYYY-MM-DD")

        requested_days = max(1, (end - start).days + 1)
        cap = MAX_DURATION_DAYS.get(bar_size, 30)
        used_days = min(requested_days, cap)
        if used_days < requested_days:
            logger.warning(
                "Requested %s days of %s bars; IBKR caps this at %s and the range "
                "was clamped. Narrow the range or page through it.",
                requested_days,
                bar_size,
                cap,
            )

        # IBKR wants the end as either an empty string (now) or "YYYYMMDD HH:MM:SS"
        # in the exchange's timezone. UTC is used, which IBKR accepts.
        end_datetime = f"{end.strftime('%Y%m%d')} 23:59:59"

        contract = self._contract(symbol, exchange)
        with auth_api.get_connection(self.auth_token) as connection:
            data = connection.historical_bars(
                contract,
                end_datetime=end_datetime,
                duration=_ibkr_duration(used_days),
                bar_size=bar_size,
                what_to_show="TRADES",
                use_rth=True,
            )

        rows = []
        for bar in data.get("bars", []):
            timestamp = _bar_timestamp(bar.get("date"), intraday=(cap <= 5))
            if timestamp is None:
                continue
            rows.append(
                {
                    "timestamp": timestamp,
                    "open": bar["open"],
                    "high": bar["high"],
                    "low": bar["low"],
                    "close": bar["close"],
                    "volume": bar["volume"],
                    "oi": 0,
                }
            )

        frame = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
        return frame


def _f(value):
    """A quote field as float; blank becomes 0."""
    try:
        if value in (None, ""):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _parse_date(text):
    try:
        return datetime.strptime(str(text).strip(), "%Y-%m-%d")
    except (TypeError, ValueError):
        return None


def _bar_timestamp(raw, intraday):
    """An IBKR bar date as epoch seconds.

    ``formatDate=2`` gives epoch seconds for intraday bars, but daily, weekly and
    monthly bars always come back as ``YYYYMMDD`` strings regardless -- so both
    shapes have to be handled, and a daily bar is anchored to midnight UTC.
    """
    text = str(raw or "").strip()
    if not text:
        return None
    if len(text) == 8 and text.isdigit():
        try:
            return int(
                datetime.strptime(text, "%Y%m%d")
                .replace(tzinfo=timezone.utc)
                .timestamp()
            )
        except ValueError:
            return None
    try:
        return int(float(text))
    except ValueError:
        return None
