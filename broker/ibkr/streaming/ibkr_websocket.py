"""
The streaming reader thread.

TWS pushes market data down the same socket every other request uses, so the
stream is not a separate connection: it is the connection that stays open and is
read continuously instead of being closed after one response.

The loop is deliberately synchronous and blocking, which is the same shape the
``websocket-client``-based adapters in this tree use. Under eventlet the socket
is green and a blocking read yields to the hub; under gthread it blocks a real
thread. Both are covered by the repo's worker contract.

Two IBKR rules the loop has to respect:

* A ticker id must be unique among live subscriptions, or the gateway answers
  error 102 and the earlier subscription is disturbed.
* When the gateway reports error 1101 (the IB-to-server link was restored *with*
  data loss), every market-data subscription has to be re-sent. The loop does
  that rather than silently going quiet, which is the failure mode a trader
  notices last.
"""

import threading

from broker.ibkr.api import tws_client
from utils.logging import get_logger

logger = get_logger(__name__)


class IbkrStreamReader:
    """Reads a live TWS connection and hands decoded ticks to a callback."""

    def __init__(self, connection, on_tick, on_error=None):
        self.connection = connection
        self.on_tick = on_tick
        self.on_error = on_error
        self._thread = None
        self._running = False
        #: ticker id -> (symbol, exchange), so a tick can be attributed back.
        self._tickers = {}
        #: ticker id -> contract, so a data-losing reconnect can resubscribe.
        self._contracts = {}

    def register(self, ticker_id, symbol, exchange, contract=None):
        self._tickers[ticker_id] = (symbol, exchange)
        if contract is not None:
            self._contracts[ticker_id] = contract

    def unregister(self, ticker_id):
        self._tickers.pop(ticker_id, None)
        self._contracts.pop(ticker_id, None)

    def last_ticker_id(self):
        return max(self._tickers) if self._tickers else None

    def start(self):
        """Start the reader thread. Idempotent."""
        if self._thread and self._thread.is_alive():
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run, name="ibkr-stream-reader", daemon=True
        )
        self._thread.start()

    def stop(self):
        self._running = False
        thread, self._thread = self._thread, None
        if thread and thread.is_alive():
            thread.join(timeout=2.0)

    def _run(self):
        quote_cache = {}
        while self._running:
            try:
                frame = self.connection.read_frame()
            except tws_client.TwsRequestTimeout:
                # A quiet market is not an error; the socket timeout just means
                # no tick arrived in the window.
                continue
            except tws_client.TwsError as exc:
                if self._running:
                    logger.warning("IBKR stream ended: %s", exc)
                    if self.on_error is not None:
                        self.on_error(str(exc))
                return

            try:
                kind = int(float(frame[0]))
            except (TypeError, ValueError, IndexError):
                continue

            if kind == tws_client.IN_ERR_MSG:
                code = _int(frame[3]) if len(frame) > 3 else 0
                if code == 1101:
                    # Data was lost across the reconnect: re-send subscriptions.
                    logger.warning("IBKR reported a data-losing reconnect; resubscribing")
                    self._resubscribe()
                elif code in tws_client.INFO_CODES or code == 2108:
                    continue
                else:
                    logger.debug("IBKR stream notice %s: %s", code, frame[4:5])
                continue

            if kind not in (
                tws_client.IN_TICK_PRICE,
                tws_client.IN_TICK_SIZE,
                tws_client.IN_TICK_GENERIC,
                tws_client.IN_TICK_STRING,
            ):
                continue

            ticker_id = _int(frame[2]) if len(frame) > 2 else None
            target = self._tickers.get(ticker_id)
            if target is None:
                continue
            symbol, exchange = target
            cache = quote_cache.setdefault(ticker_id, {})
            tws_client.TwsConnection._apply_tick(cache, frame, ticker_id)
            self.on_tick(symbol, exchange, dict(cache))

    def _resubscribe(self):
        """Re-send every live subscription after a data-losing reconnect."""
        for ticker_id, (symbol, _exchange) in list(self._tickers.items()):
            try:
                contract = self._contract_for(ticker_id)
                if contract is None:
                    continue
                self.connection.send(
                    *tws_client._market_data_request_fields(
                        ticker_id, contract, self.connection.server_version, snapshot=False
                    )
                )
            except tws_client.TwsError as exc:
                logger.warning("Could not resubscribe %s: %s", symbol, exc)

    def _contract_for(self, ticker_id):
        return self._contracts.get(ticker_id)


def _int(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None
