"""
Orders, order/trade books, positions and holdings for IBKR.

Three IBKR-specific facts shape this file:

* **Order ids are client-owned.** The gateway seeds a counter through
  ``nextValidId`` at connect and the client picks each id. This plugin opens a
  connection per operation, and each connection gets its own seed, so the id to
  use is always ``connection.next_order_id``.
* **There is no modify-order message.** The TWS API can cancel and it can place,
  but it cannot amend, so :func:`modify_order` is a cancel followed by a
  replace. IBKR's own clients do the same; the replaced order gets a new id.
* **There is no partial-fill "remaining quantity" edit.** A modify that only
  changes quantity still has to go through cancel+replace.

Order status comes from the separate ``ORDER_STATUS`` frame, which is fully
specified, rather than from the ``OPEN_ORDER`` frame's order-state block, which
is not.
"""

import time

from broker.ibkr.api import auth_api, tws_client
from broker.ibkr.mapping.order_data import (
    calculate_order_statistics,
    map_order_data,
    map_position_data,
    normalize_status,
    transform_order_data,
    transform_positions_data,
)
from broker.ibkr.mapping.transform_data import build_ibkr_contract, transform_data
from database.token_db import get_br_symbol, get_token
from utils.logging import get_logger

logger = get_logger(__name__)


class _Response:
    """The minimal response shim callers check: ``.status_code`` and ``.status``."""

    def __init__(self, status_code):
        self.status_code = status_code
        self.status = status_code


def _error(message, status_code=400):
    logger.error("[IBKR] %s", message)
    return _Response(status_code), {"status": "error", "message": message}, None


def _order_status_from_frames(frames, order_id, server_version):
    """Pull the final ``ORDER_STATUS`` for ``order_id`` out of the frames read."""
    for frame in frames or []:
        try:
            if int(float(frame[0])) != tws_client.IN_ORDER_STATUS:
                continue
        except (TypeError, ValueError, IndexError):
            continue
        idx = 1
        if server_version < tws_client.MIN_SERVER_VER_MARKET_CAP_PRICE:
            idx += 1
        if idx + 1 >= len(frame):
            continue
        if str(frame[idx]) != str(order_id):
            continue
        return {
            "status": frame[idx + 1],
            "filled": frame[idx + 2] if idx + 2 < len(frame) else "0",
            "remaining": frame[idx + 3] if idx + 3 < len(frame) else "0",
            "avgFillPrice": frame[idx + 4] if idx + 4 < len(frame) else "",
        }
    return None


def place_order_api(data, auth):
    """Place one order.

    Returns:
        tuple: ``(response, response_data, orderid)``. ``response.status == 200``
        means the gateway accepted the order.
    """
    symbol = data.get("symbol")
    exchange = data.get("exchange")
    token = get_token(symbol, exchange)
    if not token:
        return _error(
            f"Symbol '{symbol}' is not in the master contract for exchange "
            f"'{exchange}'. Run Master Contract download for IBKR first."
        )

    try:
        payload = transform_data(data, token)
    except Exception as exc:  # noqa: BLE001 - surfaced to the caller as a message
        return _error(f"Could not build the IBKR order for {symbol}: {exc}")

    try:
        with auth_api.get_connection(auth) as connection:
            order_id = connection.next_order_id
            server_version = connection.server_version
            frames = connection.place_order(order_id, payload["contract"], payload["order"])
            status = _order_status_from_frames(frames, order_id, server_version)
    except tws_client.TwsConnectionError as exc:
        return _error(f"IBKR is not reachable: {exc}", 503)
    except tws_client.TwsError as exc:
        return _error(f"IBKR rejected the order: {exc}")

    # A status that is present and clearly not accepted is a failure, even though
    # the socket accepted the frame.
    if status and normalize_status(status["status"]) == "rejected":
        return (
            _Response(400),
            {"status": "error", "message": f"IBKR rejected the order ({status['status']})"},
            str(order_id),
        )

    logger.info("IBKR order %s placed for %s", order_id, symbol)
    return (
        _Response(200),
        {
            "status": "success",
            "orderid": str(order_id),
            "message": status["status"] if status else "Submitted",
        },
        str(order_id),
    )


def place_smartorder_api(data, auth):
    """Place a smart order: reuse a key, and cancel the rest.

    The behaviour matches the other plugins -- one order is kept and any other
    open order on the same symbol is cancelled -- but the ids here are IBKR's, so
    the "keep" decision is made on the current open-order list.
    """
    # The service resolves the net position and the quantity to send before it
    # reaches a broker module, so this mirrors place_order_api and then tidies up
    # any other working order on the same contract.
    response, response_data, orderid = place_order_api(data, auth)
    if response.status_code != 200:
        return response, response_data, orderid

    symbol = data.get("symbol")
    try:
        with auth_api.get_connection(auth) as connection:
            frames = connection.all_open_orders()
            server_version = connection.server_version
            orders = map_order_data(
                frames,
                server_version=server_version,
                contract_fields=tws_client.contract_fields,
            )
            for order in orders:
                if order.get("symbol") != symbol:
                    continue
                if str(order.get("orderId")) == str(orderid):
                    continue
                if normalize_status(order.get("status")) != "open":
                    continue
                try:
                    connection.cancel_order(int(order["orderId"]))
                except (tws_client.TwsError, TypeError, ValueError) as exc:
                    logger.warning("Could not cancel superseded order %s: %s", order.get("orderId"), exc)
    except tws_client.TwsError as exc:
        logger.warning("Smart-order cleanup could not list open orders: %s", exc)

    return response, response_data, orderid


def modify_order(data, auth):
    """Modify an order by cancelling it and placing a replacement.

    The TWS API has no amend, so a new order id is issued and returned. Callers
    must treat the order id as changed.
    """
    orderid = data.get("orderid")
    if orderid in (None, ""):
        return {"status": "error", "message": "orderid is required to modify an order"}, 400

    try:
        with auth_api.get_connection(auth) as connection:
            connection.cancel_order(int(orderid))
            # Give the gateway a moment to release the id before replacing the
            # order; an immediate replace can race the cancel.
            time.sleep(0.5)
    except (tws_client.TwsError, TypeError, ValueError) as exc:
        return {"status": "error", "message": f"Could not cancel order {orderid}: {exc}"}, 400

    replacement = dict(data)
    response, response_data, new_id = place_order_api(replacement, auth)
    if response.status_code != 200:
        return response_data, response.status_code

    return {
        "status": "success",
        "orderid": new_id,
        "message": "IBKR has no amend: the order was cancelled and replaced.",
    }, 200


def cancel_order(orderid, auth):
    """Cancel one order by id."""
    if orderid in (None, ""):
        return {"status": "error", "message": "orderid is required"}, 400
    try:
        with auth_api.get_connection(auth) as connection:
            connection.cancel_order(int(orderid))
    except (tws_client.TwsError, TypeError, ValueError) as exc:
        return {"status": "error", "message": f"Could not cancel order {orderid}: {exc}"}, 400
    return {"status": "success", "orderid": str(orderid)}, 200


def cancel_all_orders_api(data, auth):
    """Cancel every working order.

    Returns:
        tuple[list, list]: ``(cancelled, failed)``.
    """
    cancelled = []
    failed = []
    try:
        with auth_api.get_connection(auth) as connection:
            frames = connection.all_open_orders()
            orders = map_order_data(
                frames,
                server_version=connection.server_version,
                contract_fields=tws_client.contract_fields,
            )
            for order in orders:
                order_id = order.get("orderId")
                if order_id in (None, ""):
                    continue
                if normalize_status(order.get("status")) != "open":
                    continue
                try:
                    connection.cancel_order(int(order_id))
                    cancelled.append(order_id)
                except (tws_client.TwsError, TypeError, ValueError) as exc:
                    logger.warning("Cancel-all failed for %s: %s", order_id, exc)
                    failed.append(order_id)
    except tws_client.TwsError as exc:
        return [], [str(exc)]
    return cancelled, failed


def close_all_positions(api_key, auth):
    """Flatten every open position with a market order."""
    try:
        positions = get_positions(auth)
    except tws_client.TwsError as exc:
        return {"status": "error", "message": f"Could not read positions: {exc}"}, 400

    closed, failed = [], []
    for position in positions:
        quantity = _number(position.get("position"))
        if not quantity:
            continue
        symbol = position.get("symbol")
        if not symbol:
            continue
        exchange = "US-OPT" if position.get("secType") == "OPT" else "US"
        order = {
            "symbol": symbol,
            "exchange": exchange,
            "action": "SELL" if quantity > 0 else "BUY",
            "quantity": abs(quantity),
            "pricetype": "MARKET",
            "product": "NRML" if position.get("secType") == "OPT" else "CNC",
        }
        response, _payload, _orderid = place_order_api(order, auth)
        if response.status_code == 200:
            closed.append(symbol)
        else:
            failed.append(symbol)

    return {
        "status": "success" if not failed else "partial",
        "closed": closed,
        "failed": failed,
    }, 200


def get_order_book(auth):
    """Raw open-order frames. The service maps them with the order mapper."""
    with auth_api.get_connection(auth) as connection:
        return connection.all_open_orders()


def get_trade_book(auth):
    """Today's executions, shaped for the trade mapper."""
    with auth_api.get_connection(auth) as connection:
        rows = connection.executions(account=auth_api.get_account(auth))
    trades = []
    for row in rows:
        trades.append(
            {
                "orderId": row.get("orderId"),
                "symbol": row.get("symbol"),
                "secType": row.get("secType"),
                "quantity": _number(row.get("shares")),
                "price": _number(row.get("price")),
                "action": row.get("side"),
                "time": row.get("time"),
            }
        )
    return trades


def get_positions(auth, strict=False):
    """Open positions as IBKR-shaped dicts.

    ``strict`` is accepted for parity with the other plugins' smart-order path:
    when true, an unreadable position book raises instead of reading as flat.
    """
    try:
        with auth_api.get_connection(auth) as connection:
            return connection.positions()
    except tws_client.TwsError:
        if strict:
            raise
        logger.exception("Could not read IBKR positions")
        return []


def get_holdings(auth):
    """Long stock positions, as IBKR portfolio rows.

    IBKR has no separate holdings concept for US equities; a settled long stock
    position is the holding.
    """
    try:
        with auth_api.get_connection(auth) as connection:
            _values, portfolio = connection.account_values(account=auth_api.get_account(auth))
    except tws_client.TwsError:
        logger.exception("Could not read IBKR portfolio")
        return []

    holdings = []
    for frame in portfolio:
        decoded = _decode_portfolio_value(frame)
        if decoded and decoded.get("secType") == "STK" and _number(decoded.get("position")) > 0:
            cost = _number(decoded.get("averageCost"))
            market = _number(decoded.get("marketPrice"))
            quantity = _number(decoded.get("position"))
            pnl = _number(decoded.get("unrealizedPNL"))
            invested = cost * quantity
            holdings.append(
                {
                    "symbol": decoded.get("symbol"),
                    "localSymbol": decoded.get("localSymbol") or decoded.get("symbol"),
                    "secType": "STK",
                    "quantity": quantity,
                    "pnl": pnl,
                    "pnlPercent": (pnl / invested * 100) if invested else 0.0,
                    "marketValue": _number(decoded.get("marketValue")),
                    "marketPrice": market,
                }
            )
    return holdings


def get_open_position(tradingsymbol, exchange, product, auth):
    """Net position for one symbol, as a string. ``"0"`` means flat."""
    try:
        positions = get_positions(auth)
    except tws_client.TwsError:
        logger.exception("Could not read IBKR positions for %s", tradingsymbol)
        return "0"

    wanted = get_br_symbol(tradingsymbol, exchange) or tradingsymbol
    total = 0.0
    for position in positions:
        if (position.get("symbol") or "").upper() != wanted.upper():
            continue
        total += _number(position.get("position"))
    return str(int(total)) if total == int(total) else str(total)


def _decode_portfolio_value(frame):
    """``PORTFOLIO_VALUE`` (7) -> dict.

    Layout: ``[7, v, conId, symbol, secType, expiry, strike, right,
    (multiplier, primaryExchange if v>=7), currency, localSymbol,
    (tradingClass if v>=8), position, marketPrice, marketValue, averageCost,
    unrealizedPNL, realizedPNL, accountName]``.
    """
    if not frame:
        return None
    try:
        if int(float(frame[0])) != tws_client.IN_PORTFOLIO_VALUE:
            return None
    except (TypeError, ValueError, IndexError):
        return None

    message_version = _int(frame[1])
    idx = 2
    if idx + 11 > len(frame):
        return None
    decoded = {
        "conId": _int(frame[idx]),
        "symbol": frame[idx + 1],
        "secType": frame[idx + 2],
        "lastTradeDateOrContractMonth": frame[idx + 3],
        "strike": frame[idx + 4],
        "right": frame[idx + 5],
    }
    idx += 6
    if message_version >= 7:
        decoded["multiplier"] = frame[idx]
        decoded["primaryExchange"] = frame[idx + 1]
        idx += 2
    decoded["currency"] = frame[idx]
    decoded["localSymbol"] = frame[idx + 1]
    idx += 2
    if message_version >= 8:
        decoded["tradingClass"] = frame[idx]
        idx += 1
    for name in (
        "position",
        "marketPrice",
        "marketValue",
        "averageCost",
        "unrealizedPNL",
        "realizedPNL",
    ):
        if idx < len(frame):
            decoded[name] = frame[idx]
            idx += 1
    return decoded


def _int(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _number(value):
    try:
        if value in (None, ""):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


# Re-exported so the service layer finds them on this module as it does for the
# other brokers.
__all__ = [
    "calculate_order_statistics",
    "cancel_all_orders_api",
    "cancel_order",
    "close_all_positions",
    "get_holdings",
    "get_open_position",
    "get_order_book",
    "get_positions",
    "get_trade_book",
    "map_order_data",
    "map_position_data",
    "modify_order",
    "place_order_api",
    "place_smartorder_api",
    "transform_order_data",
    "transform_positions_data",
]
