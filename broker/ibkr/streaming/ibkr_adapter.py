"""
The IBKR streaming adapter.

Unlike the HTTP brokers, IBKR does not hand out a per-symbol subscription URL.
One socket carries every subscription, market data arrives as ticks with a
ticker id, and the adapter's job is to fan those ticks out onto the ZMQ bus in
OpenAlgo's common shape so the rest of the stack cannot tell which broker it is.

The connection is opened at :meth:`connect` and read by
:class:`~broker.ibkr.streaming.ibkr_websocket.IbkrStreamReader` on its own thread;
:meth:`subscribe` and :meth:`unsubscribe` only add and remove ticker ids on that
live connection.

A ticker id must be unique among live subscriptions (IBKR error 102 otherwise),
and IBKR allows 100 market-data lines by default, so subscriptions are counted
and refused past the limit with a message that names the setting rather than
failing obscurely at the gateway.
"""

from broker.ibkr.api import auth_api, tws_client
from broker.ibkr.mapping.transform_data import build_ibkr_contract
from broker.ibkr.streaming.ibkr_mapping import IbkrCapabilityRegistry, IbkrExchangeMapper
from broker.ibkr.streaming.ibkr_websocket import IbkrStreamReader
from database.token_db import get_token
from utils.logging import get_logger
from websocket_proxy.base_adapter import BaseBrokerWebSocketAdapter

logger = get_logger(__name__)

MODE_LTP = 1
MODE_QUOTE = 2
MODE_DEPTH = 3

MODE_NAMES = {MODE_LTP: "LTP", MODE_QUOTE: "QUOTE", MODE_DEPTH: "DEPTH"}


class IbkrWebSocketAdapter(BaseBrokerWebSocketAdapter):
    """Streams IBKR market data onto OpenAlgo's ZMQ bus."""

    def __init__(self, use_shared_zmq=False, shared_publisher=None):
        super().__init__(use_shared_zmq=use_shared_zmq, shared_publisher=shared_publisher)
        self.connection = None
        self.reader = None
        self.auth_token = None
        self.user_id = None
        self.broker_name = "ibkr"
        #: ticker id -> (symbol, exchange, mode)
        self.subscriptions = {}
        self._next_ticker_id = 1

    # -- lifecycle --

    def initialize(self, broker_name, user_id, auth_data=None):
        """Record the session. No socket is opened until :meth:`connect`."""
        self.broker_name = broker_name or "ibkr"
        self.user_id = user_id
        self.auth_token = auth_data
        if not self.auth_token:
            try:
                from database.auth_db import get_auth_token

                self.auth_token = get_auth_token(user_id)
            except Exception:
                logger.exception("Could not resolve an IBKR session for %s", user_id)
        self.logger.info("IBKR adapter initialised for %s", user_id)

    def connect(self):
        """Open the socket and start the reader thread."""
        if self.connection is not None:
            return self._create_success_response("Already connected")

        if not self.auth_token:
            return self._create_error_response(
                "NO_SESSION", "No IBKR session; log in to Interactive Brokers first"
            )

        try:
            self.connection = auth_api.get_connection(self.auth_token)
        except tws_client.TwsError as exc:
            return self._create_error_response("CONNECTION_FAILED", str(exc))

        self.reader = IbkrStreamReader(
            self.connection, on_tick=self._on_tick, on_error=self._on_stream_error
        )
        self.reader.start()
        self.connected = True
        return self._create_success_response("Connected to IBKR")

    def disconnect(self):
        """Stop the reader and close the socket."""
        if self.reader is not None:
            self.reader.stop()
            self.reader = None
        if self.connection is not None:
            try:
                self.connection.close()
            except tws_client.TwsError:
                logger.debug("Ignoring an error while closing the IBKR socket")
            self.connection = None
        self.subscriptions.clear()
        self.connected = False
        return self._create_success_response("Disconnected from IBKR")

    # -- subscriptions --

    def subscribe(self, symbol, exchange, mode=MODE_QUOTE, depth_level=5):
        """Add one symbol to the live stream."""
        if self.connection is None or self.reader is None:
            response = self.connect()
            if getattr(response, "get", None) and response.get("status") == "error":
                return response
            if self.connection is None:
                return self._create_error_response(
                    "NOT_CONNECTED", "IBKR is not connected"
                )

        exchange = (exchange or "US").upper()
        if not IbkrCapabilityRegistry.supports(exchange, mode):
            return self._create_error_response(
                "UNSUPPORTED",
                f"IBKR streams LTP and quote modes on US/US-OPT; "
                f"{MODE_NAMES.get(mode, mode)} on {exchange} is not supported",
            )

        if len(self.subscriptions) >= IbkrCapabilityRegistry.MAX_SYMBOLS_PER_CONNECTION:
            return self._create_error_response(
                "MAX_SYMBOLS",
                f"IBKR allows {IbkrCapabilityRegistry.MAX_SYMBOLS_PER_CONNECTION} "
                "market-data lines by default. Unsubscribe something, or raise the "
                "limit with IBKR's quote booster packs.",
            )

        for ticker_id, (sub_symbol, sub_exchange, _mode) in self.subscriptions.items():
            if sub_symbol == symbol and sub_exchange == exchange:
                return self._create_success_response(
                    "Already subscribed", symbol=symbol, exchange=exchange
                )

        contract = build_ibkr_contract(symbol, exchange)
        if not contract.get("conId"):
            token = get_token(symbol, exchange)
            if token:
                try:
                    contract["conId"] = int(token)
                except (TypeError, ValueError):
                    pass
        if not contract.get("conId") and contract.get("secType") == "OPT":
            # An option without a conId cannot be addressed: IBKR would have to
            # search, which is not something a live subscription can do.
            return self._create_error_response(
                "UNKNOWN_CONTRACT",
                f"{symbol} has no IBKR contract id. Run Master Contract download first.",
            )

        ticker_id = self._next_ticker_id
        self._next_ticker_id += 1

        try:
            self.connection.send(
                *tws_client._market_data_request_fields(
                    ticker_id, contract, self.connection.server_version, snapshot=False
                )
            )
        except tws_client.TwsError as exc:
            return self._create_error_response("SUBSCRIBE_FAILED", str(exc))

        self.subscriptions[ticker_id] = (symbol, exchange, mode)
        self.reader.register(ticker_id, symbol, exchange, contract)
        self.logger.info("Subscribed to %s on %s (ticker %s)", symbol, exchange, ticker_id)
        return self._create_success_response(
            "Subscribed", symbol=symbol, exchange=exchange, mode=mode
        )

    def unsubscribe(self, symbol, exchange, mode=MODE_QUOTE):
        """Remove one symbol from the live stream."""
        exchange = (exchange or "US").upper()
        target = None
        for ticker_id, (sub_symbol, sub_exchange, _mode) in self.subscriptions.items():
            if sub_symbol == symbol and sub_exchange == exchange:
                target = ticker_id
                break
        if target is None:
            return self._create_success_response(
                "Not subscribed", symbol=symbol, exchange=exchange
            )

        try:
            if self.connection is not None:
                self.connection.send(tws_client.OUT_CANCEL_MKT_DATA, 2, target)
        except tws_client.TwsError as exc:
            logger.warning("Could not cancel IBKR ticker %s: %s", target, exc)

        self.subscriptions.pop(target, None)
        if self.reader is not None:
            self.reader.unregister(target)
        return self._create_success_response(
            "Unsubscribed", symbol=symbol, exchange=exchange
        )

    # -- fan-out --

    def _on_tick(self, symbol, exchange, quote):
        """Publish one tick onto the ZMQ bus in OpenAlgo's common shape."""
        mode = MODE_QUOTE
        for sub_symbol, sub_exchange, sub_mode in self.subscriptions.values():
            if sub_symbol == symbol and sub_exchange == exchange:
                mode = sub_mode
                break

        ltp = quote.get("ltp") or quote.get("close") or 0
        topic = f"{exchange}_{symbol}_{MODE_NAMES.get(mode, 'QUOTE')}"

        if mode == MODE_LTP:
            payload = {"mode": MODE_LTP, "ltp": ltp}
        else:
            payload = {
                "mode": MODE_QUOTE,
                "ltp": ltp,
                "volume": quote.get("volume", 0),
                "open": quote.get("open", 0),
                "high": quote.get("high", 0),
                "low": quote.get("low", 0),
                "prev_close": quote.get("prev_close", 0),
                "bid": quote.get("bid", 0),
                "ask": quote.get("ask", 0),
                "bids": [{"price": quote.get("bid", 0), "quantity": quote.get("bid_size", 0)}],
                "asks": [{"price": quote.get("ask", 0), "quantity": quote.get("ask_size", 0)}],
                "totalbuyqty": quote.get("bid_size", 0),
                "totalsellqty": quote.get("ask_size", 0),
            }

        self.publish_market_data(topic, payload)

    def _on_stream_error(self, message):
        self.logger.warning("IBKR stream error: %s", message)
        self.connected = False
