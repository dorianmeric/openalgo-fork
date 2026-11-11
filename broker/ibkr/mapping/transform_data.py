"""
OpenAlgo order -> IBKR ``Contract`` + ``Order``.

IBKR orders differ from every Indian broker's in one structural way: an order
does not carry a token, it carries a **contract** (a ``conId`` plus enough
description to identify it). So the transform produces both halves, and the
``conId`` from the master contract rides along so the gateway never has to guess
what was meant.

Two OpenAlgo concepts have no IBKR equivalent and are handled explicitly rather
than silently dropped:

* **product** (``CNC``/``NRML``/``MIS``). IBKR has no delivery/intraday product
  axis: an order is an order, and whether it is "intraday" is a matter of what
  you do with it. ``MIS`` therefore cannot be made to auto-square-off by the
  broker; the product is accepted and ignored for order construction.
* **MARKET / SL-M and outside regular hours.** ``outsideRth`` is forwarded when
  the caller asks, because IBKR fills a market order in the pre/post session
  only if it is set.
"""

from database.token_db import get_br_symbol, get_symbol_info, get_token
from utils.logging import get_logger

logger = get_logger(__name__)

#: OpenAlgo price type -> IBKR ``orderType``.
#:
#: ``SL`` is a stop-*limit* in OpenAlgo (it carries both a trigger and a limit
#: price), which is IBKR's ``STP LMT``. ``SL-M`` is a stop with no limit, IBKR's
#: plain ``STP``, whose stop price travels in ``auxPrice``.
ORDER_TYPE_MAP = {
    "MARKET": "MKT",
    "LIMIT": "LMT",
    "SL": "STP LMT",
    "SL-M": "STP",
}

#: OpenAlgo validity -> IBKR time-in-force. IBKR defaults to DAY.
TIF_MAP = {
    "DAY": "DAY",
    "IOC": "IOC",
    "FOK": "FOK",
    "GTC": "GTC",
}

#: OpenAlgo exchange -> IBKR exchange. ``SMART`` is IBKR's router and is what a
#: US equity or option order should use; the concrete listing venue is carried
#: separately as ``primaryExchange`` when the master contract knows it.
EXCHANGE_MAP = {
    "US": "SMART",
    "US-OPT": "SMART",
}

DEFAULT_CURRENCY = "USD"


def map_order_type(pricetype):
    """OpenAlgo price type -> IBKR order type. Unmapped types default to LMT."""
    return ORDER_TYPE_MAP.get((pricetype or "").upper(), "LMT")


def map_tif(validity):
    """OpenAlgo validity -> IBKR time-in-force. Defaults to DAY."""
    return TIF_MAP.get((validity or "").upper(), "DAY")


def map_exchange(exchange):
    """OpenAlgo exchange -> IBKR exchange."""
    return EXCHANGE_MAP.get((exchange or "").upper(), "SMART")


def ibkr_expiry(openalgo_expiry):
    """``DD-MMM-YY`` (the OpenAlgo convention) -> ``YYYYMMDD`` (IBKR's).

    Empty input stays empty, which is how a cash instrument is expressed.
    """
    text = (openalgo_expiry or "").strip().upper()
    if not text:
        return ""
    from datetime import datetime

    for fmt in ("%d-%b-%y", "%d-%b-%Y", "%Y%m%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y%m%d")
        except ValueError:
            continue
    logger.warning("Could not parse expiry %r for an IBKR contract", openalgo_expiry)
    return ""


def build_ibkr_contract(symbol, exchange):
    """Build the IBKR contract for an OpenAlgo symbol from the master contract.

    The ``SymToken`` row is the source of truth: its ``token`` is the IBKR
    ``conId``, ``instrumenttype`` says EQ/CE/PE, and ``strike``/``expiry``/
    ``lotsize`` give the option's strike, expiry and multiplier.
    """
    info = get_symbol_info(symbol, exchange)
    contract = {
        "symbol": symbol,
        "exchange": map_exchange(exchange),
        "currency": DEFAULT_CURRENCY,
    }

    if info is not None:
        instrument_type = (info.instrumenttype or "EQ").upper()
        # For an option IBKR wants the *underlying* in `symbol`, with the
        # contract described by expiry/strike/right.
        underlying = None
        if instrument_type in ("CE", "PE"):
            underlying = (info.name or symbol).strip()
            contract.update(
                {
                    "symbol": underlying or symbol,
                    "secType": "OPT",
                    "lastTradeDateOrContractMonth": ibkr_expiry(info.expiry),
                    "strike": float(info.strike or 0),
                    "right": "C" if instrument_type == "CE" else "P",
                    "multiplier": str(int(info.lotsize)) if info.lotsize else "100",
                }
            )
            contract["primaryExchange"] = info.brexchange or ""
        else:
            contract.update({"secType": "STK", "primaryExchange": info.brexchange or ""})

        if info.token:
            try:
                contract["conId"] = int(info.token)
            except (TypeError, ValueError):
                logger.debug("Non-numeric conId %r for %s", info.token, symbol)
    else:
        # No master-contract row yet. Fall back to a description-only equity
        # contract: IBKR can still resolve it, but the caller has no conId, so
        # an option is not addressable this way.
        logger.warning(
            "No master-contract row for %s on %s; sending a description-only contract",
            symbol,
            exchange,
        )
        contract["secType"] = "STK"

    return contract


def transform_data(data, token=None):
    """OpenAlgo order request -> ``{"contract": {...}, "order": {...}}``.

    Args:
        data: The OpenAlgo order payload (``symbol``, ``exchange``,
            ``action``, ``quantity``, ``pricetype``, ``product``, ``price``,
            ``trigger_price``, optional ``validity``, ``outside_rth``).
        token: The instrument token, i.e. the IBKR ``conId``.
    """
    symbol = data["symbol"]
    exchange = data["exchange"]

    # The broker symbol is kept for the order reference, so a TWS user can
    # recognise the order in the desktop app.
    br_symbol = get_br_symbol(symbol, exchange) or symbol
    contract = build_ibkr_contract(symbol, exchange)
    if token:
        try:
            contract["conId"] = int(token)
        except (TypeError, ValueError):
            contract["conId"] = get_token(symbol, exchange) or contract.get("conId", 0)
    if not contract.get("conId"):
        resolved = get_token(symbol, exchange)
        if resolved:
            try:
                contract["conId"] = int(resolved)
            except (TypeError, ValueError):
                pass

    pricetype = (data.get("pricetype") or "LIMIT").upper()
    order_type = map_order_type(pricetype)

    order = {
        "action": (data.get("action") or "BUY").upper(),
        "totalQuantity": float(data.get("quantity") or 0),
        "orderType": order_type,
        "tif": map_tif(data.get("validity")),
        "account": data.get("account", ""),
        "outsideRth": bool(data.get("outside_rth", False)),
        "orderRef": str(data.get("order_ref") or br_symbol)[:40],
    }

    if order_type in ("LMT", "STP LMT"):
        order["lmtPrice"] = _price(data.get("price"))
    if order_type in ("STP", "STP LMT"):
        order["auxPrice"] = _price(data.get("trigger_price"))

    logger.debug("Transformed IBKR order for %s: contract=%s order=%s", symbol, contract, order)
    return {"contract": contract, "order": order}


def _price(value):
    """A price as float, or ``None`` when it is unset.

    ``None`` matters: the wire field must stay empty rather than become ``0``,
    which would be a real price and could be rejected or, worse, filled.
    """
    if value in (None, "", 0, "0", 0.0, "0.0"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
