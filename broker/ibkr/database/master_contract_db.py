"""
IBKR master contract.

**IBKR publishes no instrument master.** Every other broker in this tree hands
over a complete symbol file -- a CSV or JSON of every tradable instrument -- and
the plugin parses it. IBKR has nothing equivalent: ``reqMatchingSymbols`` returns
about sixteen prefix matches, and ``reqContractDetails`` needs the contract
already described. A complete US universe therefore cannot be *downloaded*; it
has to be *resolved*.

So this module seeds from a symbol list and resolves it:

1. Every symbol in the universe is resolved to a stock contract with
   ``reqContractDetails``. That yields the ``conId`` (IBKR's stable contract id,
   stored as ``token``) and the listing venue.
2. Options are resolved per *expiry month*, not per contract: a partially
   specified option contract makes ``reqContractDetails`` expand to every strike
   and both rights in that month. IBKR's own documentation recommends narrowing
   by at least year+month for exactly this reason -- an un-narrowed request for a
   whole chain is paced and throttled.

Consequences a trader should know:

* The universe is configuration, not discovery. ``IBKR_SYMBOL_UNIVERSE`` sets it;
  the bundled default is a short list of liquid US names.
* Options cover the months in ``IBKR_OPTION_EXPIRY_MONTHS`` (default: the next
  three). An expiry outside that window will not resolve until the download is
  re-run.
* This is a paced, socket-bound operation that takes minutes, not seconds -- it
  is not the sub-second CSV parse the other brokers do.
"""

import os
import time
from datetime import datetime

import pandas as pd
from sqlalchemy import Column, Float, Index, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from broker.ibkr.api import auth_api, tws_client
from database.engine_factory import create_db_engine
from extensions import socketio
from utils.logging import get_logger

logger = get_logger(__name__)

# IBKR is a US venue for this plugin, so everything is USD and on one exchange
# pair: US for cash, US-OPT for options.
EXCHANGE_CASH = "US"
EXCHANGE_OPTIONS = "US-OPT"
CURRENCY = "USD"

#: A short list of liquid US names, used when IBKR_SYMBOL_UNIVERSE is not set.
#: Deliberately modest: every symbol costs contract-detail calls, and IBKR paces
#: those. A trader who wants a wider universe sets the variable.
DEFAULT_UNIVERSE = (
    "AAPL,MSFT,NVDA,AMZN,GOOGL,META,TSLA,AVGO,BRK.B,JPM,"
    "V,UNH,XOM,WMT,MA,PG,JNJ,HD,CVX,ABBV,"
    "KO,PEP,COST,MRK,ADBE,CRM,AMD,NFLX,INTC,DIS,"
    "SPY,QQQ,IWM"
)

#: How many expiry months of options to enumerate, counted from the next month.
DEFAULT_EXPIRY_MONTHS = 3

#: Seconds between contract-detail requests. IBKR paces these and answers a
#: flood with a pacing violation that fails the whole download.
REQUEST_SPACING = 0.4

engine = create_db_engine()
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()


class SymToken(Base):
    """The shared symbol table.

    The columns match ``database/symbol.py`` exactly, including
    ``contract_value``, so a fresh-install ``create_all()`` produces the same
    table as every other broker's declaration.
    """

    __tablename__ = "symtoken"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=False, index=True)
    brsymbol = Column(String, nullable=False)
    name = Column(String, nullable=False)
    exchange = Column(String, nullable=False, index=True)
    brexchange = Column(String)
    token = Column(String, nullable=False)
    expiry = Column(String)
    strike = Column(Float)
    lotsize = Column(Integer)
    instrumenttype = Column(String)
    tick_size = Column(Float)
    contract_value = Column(Float)

    __table_args__ = (
        Index("idx_symbol_exchange", "symbol", "exchange"),
        Index("idx_token_exchange", "token", "exchange"),
    )


def delete_symtoken_table():
    """Clear the table before a re-download."""
    try:
        db_session.query(SymToken).delete()
        db_session.commit()
        logger.info("Cleared the symtoken table")
    except Exception:
        db_session.rollback()
        logger.exception("Could not clear the symtoken table")
        raise


def copy_from_dataframe(df):
    """Bulk-insert the resolved rows."""
    if df is None or df.empty:
        logger.warning("No IBKR contracts to insert")
        return
    try:
        df.to_sql("symtoken", con=engine, if_exists="append", index=False, method="multi")
        db_session.commit()
        logger.info("Inserted %s IBKR contracts", len(df))
    except Exception:
        db_session.rollback()
        logger.exception("Could not insert IBKR contracts")
        raise


def _universe():
    raw = (os.getenv("IBKR_SYMBOL_UNIVERSE") or "").strip() or DEFAULT_UNIVERSE
    seen = []
    for item in raw.replace("\n", ",").split(","):
        symbol = item.strip().upper()
        if symbol and symbol not in seen:
            seen.append(symbol)
    return seen


def _expiry_months():
    """``(year, month)`` pairs to enumerate options for, starting next month."""
    try:
        count = int(os.getenv("IBKR_OPTION_EXPIRY_MONTHS") or DEFAULT_EXPIRY_MONTHS)
    except ValueError:
        count = DEFAULT_EXPIRY_MONTHS
    months = []
    now = datetime.now()
    year, month = now.year, now.month
    for _ in range(max(0, count)):
        month += 1
        if month > 12:
            month = 1
            year += 1
        months.append((year, month))
    return months


def _openalgo_expiry(ibkr_expiry):
    """``YYYYMMDD`` (IBKR) -> ``DD-MMM-YY`` uppercase (OpenAlgo's convention)."""
    text = str(ibkr_expiry or "").strip()
    if len(text) < 8 or not text[:8].isdigit():
        return ""
    try:
        return datetime.strptime(text[:8], "%Y%m%d").strftime("%d-%b-%y").upper()
    except ValueError:
        return ""


def _strike_text(strike):
    """A strike as it appears in a symbol: decimals kept, ``.0`` dropped."""
    value = float(strike or 0)
    if value == int(value):
        return str(int(value))
    return f"{value:g}"


def _option_symbol(underlying, expiry, strike, instrument_type):
    """OpenAlgo's US option symbol: ``AAPL17JAN25190CE``.

    The design in ``docs/superpowers/specs/2026-07-13-global-market-architecture-design.md``
    reuses the Indian option renderer for US options, so this is the same shape
    an NFO contract takes.
    """
    day_month_year = _openalgo_expiry(expiry).replace("-", "")
    return f"{underlying}{day_month_year}{_strike_text(strike)}{instrument_type}"


def _equity_rows(connection, symbol):
    """Resolve one cash symbol to its SymToken rows."""
    contract = {
        "symbol": symbol,
        "secType": "STK",
        "exchange": "SMART",
        "currency": CURRENCY,
    }
    details = connection.contract_details(contract)
    rows = []
    for detail in details:
        if (detail.get("secType") or "").upper() != "STK":
            continue
        con_id = detail.get("conId")
        if not con_id:
            continue
        rows.append(
            {
                "symbol": symbol,
                "brsymbol": detail.get("localSymbol") or symbol,
                "name": detail.get("longName") or symbol,
                "exchange": EXCHANGE_CASH,
                "brexchange": detail.get("primaryExchange") or "SMART",
                "token": str(con_id),
                "expiry": "",
                "strike": 0.0,
                "lotsize": 1,
                "instrumenttype": "EQ",
                "tick_size": float(detail.get("minTick") or 0.01),
                "contract_value": None,
            }
        )
    return rows


def _option_rows(connection, symbol, year, month):
    """Resolve one underlying's options for a single expiry month."""
    month_text = f"{year:04d}{month:02d}"
    contract = {
        "symbol": symbol,
        "secType": "OPT",
        "lastTradeDateOrContractMonth": month_text,
        "exchange": "SMART",
        "currency": CURRENCY,
    }
    details = connection.contract_details(contract)
    rows = []
    for detail in details:
        if (detail.get("secType") or "").upper() != "OPT":
            continue
        con_id = detail.get("conId")
        expiry = detail.get("lastTradeDateOrContractMonth") or ""
        right = (detail.get("right") or "").upper()
        strike = detail.get("strike")
        if not con_id or not expiry or right not in ("C", "P") or not strike:
            continue
        instrument_type = "CE" if right == "C" else "PE"
        rows.append(
            {
                "symbol": _option_symbol(symbol, expiry, strike, instrument_type),
                "brsymbol": detail.get("localSymbol") or f"{symbol} {expiry} {strike} {right}",
                "name": symbol,
                "exchange": EXCHANGE_OPTIONS,
                "brexchange": detail.get("exchange") or "SMART",
                "token": str(con_id),
                "expiry": _openalgo_expiry(expiry),
                "strike": float(strike),
                "lotsize": int(float(detail.get("multiplier") or 100)),
                "instrumenttype": instrument_type,
                "tick_size": float(detail.get("minTick") or 0.01),
                "contract_value": None,
            }
        )
    return rows


def fetch_contracts(auth_token):
    """Resolve the universe into SymToken rows. Raises on a dead session."""
    universe = _universe()
    months = _expiry_months() if (_env_flag("IBKR_INCLUDE_OPTIONS", True)) else []
    logger.info(
        "Resolving %s IBKR symbols (%s option months) — this is paced and takes minutes",
        len(universe),
        len(months),
    )

    rows = []
    failures = []
    with auth_api.get_connection(auth_token) as connection:
        for index, symbol in enumerate(universe, start=1):
            try:
                symbol_rows = _equity_rows(connection, symbol)
                if not symbol_rows:
                    failures.append(symbol)
                rows.extend(symbol_rows)
            except tws_client.TwsError as exc:
                logger.warning("Contract lookup failed for %s: %s", symbol, exc)
                failures.append(symbol)
            time.sleep(REQUEST_SPACING)

            for year, month in months:
                try:
                    rows.extend(_option_rows(connection, symbol, year, month))
                except tws_client.TwsError as exc:
                    logger.warning(
                        "Option lookup failed for %s %04d-%02d: %s", symbol, year, month, exc
                    )
                time.sleep(REQUEST_SPACING)

            if index % 10 == 0:
                logger.info("Resolved %s/%s symbols (%s rows)", index, len(universe), len(rows))

    if failures:
        logger.warning("No contract found for: %s", ", ".join(failures))
    return pd.DataFrame(rows)


def _env_flag(name, default):
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in ("true", "1", "yes", "t")


def master_contract_download():
    """Fetch, replace and publish the IBKR symbol table.

    Runs in a background thread, so it resolves the session itself and releases
    the scoped session in a ``finally``.
    """
    try:
        auth_token = _resolve_session()
        if not auth_token:
            return socketio.emit(
                "master_contract_download",
                {
                    "status": "error",
                    "message": (
                        "No IBKR session found. Log in to Interactive Brokers first: "
                        "the symbol table is resolved through the gateway session."
                    ),
                },
            )

        frame = fetch_contracts(auth_token)
        if frame.empty:
            return socketio.emit(
                "master_contract_download",
                {
                    "status": "error",
                    "message": (
                        "IBKR returned no contracts. Check that TWS or IB Gateway is "
                        "logged in and that IBKR_SYMBOL_UNIVERSE is not empty."
                    ),
                },
            )

        delete_symtoken_table()
        copy_from_dataframe(frame)
        return socketio.emit(
            "master_contract_download",
            {
                "status": "success",
                "message": f"Downloaded {len(frame)} IBKR contracts",
                "total_symbols": len(frame),
            },
        )
    except Exception as exc:  # noqa: BLE001 - reported through the socket event
        logger.exception("IBKR master contract download failed")
        return socketio.emit(
            "master_contract_download", {"status": "error", "message": str(exc)}
        )
    finally:
        db_session.remove()


def _resolve_session():
    """The logged-in IBKR token, for a thread with no request context.

    This runs on a background thread, so ``LOGIN_USERNAME`` is often unset.
    Falling back to the ``Auth`` table is what the other plugins do: OpenAlgo is
    single-user, so the one non-revoked ``ibkr`` row is this session.
    """
    from database.auth_db import Auth, get_auth_token

    username = os.getenv("LOGIN_USERNAME")
    if username:
        token = get_auth_token(username)
        if token:
            return token

    row = (
        db_session.query(Auth)
        .filter(Auth.broker == "ibkr", Auth.is_revoked.is_(False))
        .first()
    )
    return get_auth_token(row.name) if row else None
