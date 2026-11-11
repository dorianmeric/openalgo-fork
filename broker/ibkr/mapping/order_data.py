"""
IBKR frames -> OpenAlgo normalized rows.

Where the wire format is certain, this module decodes it. Where it is not, it
says so and reads only the part that is.

The uncertain frame is ``OPEN_ORDER`` (5). IBKR sends the order as one
positional block that runs to roughly 120 fields, followed by an order-state
block, followed by server-added fields, and the reference this plugin was built
from does not enumerate that tail. The *prefix* is fully determined though -- it
is the order id, then exactly the contract block, then the leading order fields --
so this module reads that prefix and stops. Everything the orderbook needs
(symbol, action, quantity, price, type) lives in it.

Order **status** is therefore taken from the separate ``ORDER_STATUS`` (3) frame,
whose layout *is* fully specified, and joined on order id. That is also why
``map_order_data`` takes raw frames (a list of field lists) rather than dicts:
deciding which frame is which is the decoder's job, not the caller's.
"""

from utils.logging import get_logger

logger = get_logger(__name__)

#: IBKR order status -> OpenAlgo's normalized vocabulary. OpenAlgo's statuses are
#: the ones the Indian plugins emit (complete/open/rejected/cancelled/...), so an
#: IBKR-specific status must not leak into the UI.
STATUS_MAP = {
    "PENDINGSUBMIT": "open",
    "PRESUBMITTED": "open",
    "SUBMITTED": "open",
    "APIPENDING": "open",
    "PENDINGCANCEL": "open",
    "FILLED": "complete",
    "CANCELLED": "cancelled",
    "APICANCELLED": "cancelled",
    "INACTIVE": "rejected",
}

#: IBKR time-in-force -> the OpenAlgo validity spelling.
TIF_TO_VALIDITY = {
    "DAY": "DAY",
    "GTC": "GTC",
    "IOC": "IOC",
    "FOK": "FOK",
    "GTD": "DAY",
    "OPG": "DAY",
}

#: IBKR order type -> OpenAlgo price type.
ORDER_TYPE_TO_PRICE_TYPE = {
    "MKT": "MARKET",
    "LMT": "LIMIT",
    "STP": "SL-M",
    "STP LMT": "SL",
    "TRAIL": "SL-M",
    "TRAIL LIMIT": "SL",
}


def normalize_status(status):
    """IBKR order status -> the OpenAlgo status vocabulary."""
    return STATUS_MAP.get((status or "").strip().upper(), "open")


def normalize_order_type(order_type):
    """IBKR order type -> OpenAlgo price type."""
    return ORDER_TYPE_TO_PRICE_TYPE.get((order_type or "").strip().upper(), "LIMIT")


# --- Order book ---------------------------------------------------------------


def _open_order_prefix(fields, server_version, contract_fields):
    """Read the determined prefix of an ``OPEN_ORDER`` (5) frame.

    Layout: ``[5, (version only if sv<145), orderId, <contract block>,
    <order block: action, totalQuantity, orderType, lmtPrice, auxPrice, tif, ...>]``.

    Returns ``None`` when the frame is too short to be one of ours -- better to
    skip a row than to publish a half-decoded order into a trading UI.
    """
    idx = 1
    if server_version < 145:
        idx += 1  # the message's own version field
    if idx >= len(fields):
        return None
    order_id = fields[idx]
    idx += 1

    contract_len = len(contract_fields({"symbol": "", "secType": "STK"}, server_version))
    if idx + contract_len + 6 > len(fields):
        return None

    contract = {
        "conId": fields[idx],
        "symbol": fields[idx + 1],
        "secType": fields[idx + 2],
        "lastTradeDateOrContractMonth": fields[idx + 3],
        "strike": fields[idx + 4],
        "right": fields[idx + 5],
        "multiplier": fields[idx + 6],
        "exchange": fields[idx + 7],
        "primaryExchange": fields[idx + 8],
        "currency": fields[idx + 9],
        "localSymbol": fields[idx + 10],
    }
    idx += contract_len

    order = {
        "orderId": order_id,
        "action": fields[idx],
        "totalQuantity": fields[idx + 1],
        "orderType": fields[idx + 2],
        "lmtPrice": fields[idx + 3],
        "auxPrice": fields[idx + 4],
        "tif": fields[idx + 5],
    }
    return {"contract": contract, "order": order}


def map_order_data(order_data, server_version=178, contract_fields=None):
    """Raw frames from ``reqAllOpenOrders`` -> normalized order dicts.

    Accepts either the raw frames (a list of field lists, including
    ``ORDER_STATUS`` frames, which are joined on order id) or already-written
    dicts, so the same function serves tests and live calls.

    Args:
        order_data: Iterable of wire frames (lists of ``str``) or dicts.
        server_version: The version negotiated with the gateway.
        contract_fields: The ``contract_fields`` builder, injected to avoid a
            circular import between this mapper and the protocol client.
    """
    if not order_data:
        return []
    if contract_fields is None:
        from broker.ibkr.api.tws_client import contract_fields as _cf

        contract_fields = _cf

    statuses = {}
    decoded = {}

    for frame in order_data:
        if isinstance(frame, dict):
            decoded.setdefault(frame.get("orderId"), frame)
            continue
        if not frame:
            continue
        try:
            kind = int(float(frame[0]))
        except (TypeError, ValueError, IndexError):
            continue

        if kind == 5:
            prefix = _open_order_prefix(frame, server_version, contract_fields)
            if prefix:
                contract = prefix["contract"]
                order = prefix["order"]
                decoded[order["orderId"]] = {
                    "orderId": order["orderId"],
                    "symbol": contract["symbol"],
                    "secType": contract["secType"],
                    "localSymbol": contract.get("localSymbol") or contract["symbol"],
                    "action": order["action"],
                    "quantity": order["totalQuantity"],
                    "orderType": order["orderType"],
                    "price": order["lmtPrice"],
                    "triggerPrice": order["auxPrice"],
                    "tif": order["tif"],
                }
        elif kind == 3:
            # [3, (v only if sv<131), orderId, status, filled, remaining,
            #  avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld,
            #  (mktCapPrice if sv>=131)]
            idx = 1
            if server_version < 131:
                idx += 1
            if idx + 3 >= len(frame):
                continue
            statuses[frame[idx]] = {
                "status": frame[idx + 1],
                "filled": frame[idx + 2],
                "remaining": frame[idx + 3],
                "avgFillPrice": frame[idx + 4] if idx + 4 < len(frame) else "",
            }

    rows = []
    for order_id, row in decoded.items():
        status = statuses.get(order_id)
        if status:
            row["status"] = status["status"]
            row["filled"] = status["filled"]
            row["remaining"] = status["remaining"]
            row["avgFillPrice"] = status["avgFillPrice"]
        else:
            row.setdefault("status", "Submitted")
        rows.append(row)
    return rows


def calculate_order_statistics(order_data):
    """Counts the orderbook UI shows above the table."""
    stats = {
        "total_buy_orders": 0,
        "total_sell_orders": 0,
        "total_completed_orders": 0,
        "total_open_orders": 0,
        "total_rejected_orders": 0,
    }
    for order in order_data or []:
        if not isinstance(order, dict):
            continue
        action = (order.get("action") or "").upper()
        if action == "BUY":
            stats["total_buy_orders"] += 1
        elif action == "SELL":
            stats["total_sell_orders"] += 1
        status = normalize_status(order.get("status"))
        if status == "complete":
            stats["total_completed_orders"] += 1
        elif status == "rejected":
            stats["total_rejected_orders"] += 1
        else:
            stats["total_open_orders"] += 1
    return stats


def transform_order_data(orders):
    """Normalized IBKR orders -> OpenAlgo orderbook rows."""
    if orders is None:
        return []
    if isinstance(orders, dict):
        orders = [orders]

    rows = []
    for order in orders:
        if not isinstance(order, dict):
            logger.warning("Expected a dict in orderbook data, got %s", type(order))
            continue
        rows.append(
            {
                "symbol": order.get("localSymbol") or order.get("symbol", ""),
                "exchange": "US-OPT" if (order.get("secType") == "OPT") else "US",
                "action": (order.get("action") or "").upper(),
                "quantity": _number(order.get("quantity")),
                "price": _number(order.get("price")),
                "trigger_price": _number(order.get("triggerPrice")),
                "pricetype": normalize_order_type(order.get("orderType")),
                "product": "NRML" if (order.get("secType") == "OPT") else "CNC",
                "orderid": str(order.get("orderId", "")),
                "order_status": normalize_status(order.get("status")),
                "timestamp": order.get("timestamp", ""),
            }
        )
    return rows


# --- Trade book ---------------------------------------------------------------


def map_trade_data(trade_data):
    """IBKR executions -> normalized fills.

    Accepts the dicts produced by ``tws_client.decode_execution`` plus the
    account/trade fields added by the caller.
    """
    return list(trade_data or [])


def transform_tradebook_data(tradebook_data):
    """Normalized fills -> OpenAlgo tradebook rows."""
    if tradebook_data is None:
        return []
    rows = []
    for trade in tradebook_data:
        if not isinstance(trade, dict):
            continue
        quantity = _number(trade.get("quantity"))
        price = _number(trade.get("price"))
        rows.append(
            {
                "symbol": trade.get("localSymbol") or trade.get("symbol", ""),
                "exchange": "US-OPT" if trade.get("secType") == "OPT" else "US",
                "product": "NRML" if trade.get("secType") == "OPT" else "CNC",
                "action": (trade.get("action") or "").upper(),
                "quantity": quantity,
                "average_price": price,
                "trade_value": quantity * price,
                "orderid": str(trade.get("orderId", "")),
                "timestamp": trade.get("time", ""),
            }
        )
    return rows


# --- Positions ----------------------------------------------------------------


def map_position_data(position_data):
    """IBKR position rows -> normalized positions.

    IBKR's position frame carries the average cost but no last price and no P&L;
    ``ltp`` and ``pnl`` are filled in from portfolio values when the caller has
    them, and stay zero otherwise rather than being invented.
    """
    rows = []
    for position in position_data or []:
        if not isinstance(position, dict):
            continue
        quantity = _number(position.get("position"))
        if quantity == 0:
            continue
        rows.append(
            {
                "symbol": position.get("symbol", ""),
                "secType": position.get("secType", "STK"),
                "localSymbol": position.get("localSymbol") or position.get("symbol", ""),
                "conId": position.get("conId", 0),
                "product": "NRML" if position.get("secType") == "OPT" else "CNC",
                "quantity": quantity,
                "average_price": _number(position.get("avgCost")),
                "ltp": _number(position.get("marketPrice")),
                "pnl": _number(position.get("unrealizedPNL")),
                "multiplier": _number(position.get("multiplier")) or 1.0,
            }
        )
    return rows


def transform_positions_data(positions_data):
    """Normalized positions -> OpenAlgo position rows."""
    if positions_data is None:
        return []
    rows = []
    for position in positions_data:
        if not isinstance(position, dict):
            continue
        quantity = _number(position.get("quantity"))
        average_price = _number(position.get("average_price"))
        ltp = _number(position.get("ltp"))
        # Fall back to the broker's own P&L; only compute it when it is absent
        # and a last price is known, so an unknown price cannot manufacture a
        # profit or a loss.
        pnl = position.get("pnl")
        if pnl in (None, "") and ltp:
            pnl = (ltp - average_price) * quantity
        rows.append(
            {
                "symbol": position.get("localSymbol") or position.get("symbol", ""),
                "exchange": "US-OPT" if position.get("secType") == "OPT" else "US",
                "product": position.get("product", "CNC"),
                "quantity": quantity,
                "average_price": float(average_price),
                "ltp": float(ltp),
                "pnl": float(_number(pnl)),
                "lot_size": float(_number(position.get("multiplier")) or 1.0),
            }
        )
    return rows


# --- Holdings -----------------------------------------------------------------


def map_portfolio_data(portfolio_data):
    """IBKR portfolio-value frames -> long cash positions (holdings).

    IBKR has no separate "holdings" concept for US equities: a long settled stock
    position *is* the holding. Only long stock is treated as one.
    """
    rows = []
    for item in portfolio_data or []:
        if isinstance(item, dict):
            rows.append(item)
    return rows


def calculate_portfolio_statistics(holdings_data):
    """Total holding value and P&L for the holdings header."""
    total_value = 0.0
    total_pnl = 0.0
    for holding in holdings_data or []:
        if not isinstance(holding, dict):
            continue
        total_value += _number(holding.get("marketValue"))
        total_pnl += _number(holding.get("pnl"))
    return {"total_value": total_value, "total_pnl": total_pnl}


def transform_holdings_data(holdings_data):
    """Normalized holdings -> OpenAlgo holdings rows."""
    if holdings_data is None:
        return []
    rows = []
    for holding in holdings_data:
        if not isinstance(holding, dict):
            continue
        quantity = _number(holding.get("quantity"))
        if quantity <= 0:
            continue
        rows.append(
            {
                "symbol": holding.get("localSymbol") or holding.get("symbol", ""),
                "exchange": "US",
                "quantity": quantity,
                "product": "CNC",
                "pnl": float(_number(holding.get("pnl"))),
                "pnlpercent": float(_number(holding.get("pnlPercent"))),
            }
        )
    return rows


def _number(value):
    """A wire field as float, treating blanks and junk as zero."""
    try:
        if value in (None, ""):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0
