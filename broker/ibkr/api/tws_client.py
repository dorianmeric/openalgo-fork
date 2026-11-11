"""
A minimal, dependency-free client for the IBKR **TWS API** socket protocol.

Why this file exists instead of ``ibapi``/``ib_insync``
------------------------------------------------------
Every other plugin in this tree reaches its broker over HTTPS. IBKR is the
exception: TWS and IB Gateway are desktop applications that expose a plain TCP
socket, so there is no HTTP to speak.

The two usual shortcuts are both wrong here:

* ``ib_insync`` is built on ``asyncio``. OpenAlgo's default worker is eventlet
  (``start.sh``: ``--worker-class eventlet``), and ``CLAUDE.md`` is explicit that
  eventlet "is incompatible with ``asyncio.run()``, ``async``/``await``, and
  ``asyncio.get_event_loop()``". It cannot be used in request code.
* ``ibapi`` ships an ``EReader`` thread doing blocking ``select``/socket reads.
  ``CLAUDE.md`` names "works on the dev server, breaks under eventlet" as the
  single most common defect in this codebase, and that is exactly the shape.

So the protocol is implemented directly, which is also what ADR-0002 requires
("Direct integration only -- raw REST/WebSocket, no vendor SDKs/aggregators").
Nothing here is imported from IBKR.

Protocol summary
----------------
Length-prefixed, NUL-delimited ASCII::

    [4-byte big-endian length][field\\0field\\0...field\\0]

The one exception is the version banner, whose ``API\\0`` prefix is not counted
in the length and whose payload is not NUL-terminated.

After connecting, the client advertises its supported version range, the server
answers with the version it will speak, and that negotiated number gates the
field layout of nearly every message. Get it wrong and every later frame decodes
at the wrong offset -- silently.

Field layout is positional and has no tags, so a short frame does not fail
cleanly: it corrupts the stream. ``place_order`` in particular must emit every
field, deprecated ones included (IBKR error 320).

Provenance
----------
Message type integers, field orders and version gates follow the official
``ibapi`` Python client (``client.py``/``decoder.py``/``comm.py``/``message.py``
/``server_versions.py``) as the byte-level specification, cross-checked against
IBKR's public documentation. Where the two disagree, the SDK wins. Version
thresholds come from the Python decoder only, so treat individual thresholds as
verified-with-caveat.

Operational facts that bite
---------------------------
* IBKR re-authenticates the desktop session itself (TWS about weekly, IB Gateway
  about daily). A dropped session is a normal state to report, not a bug.
* "Enable ActiveX and Socket Clients" must be ticked in TWS. "Read-Only API" is
  on by default and silently blocks order placement.
* One clientId per connection; a second use of the same id fails with error 326.
* The connection is not usable until ``nextValidId`` arrives.
* More than ~50 messages/second gets the client disconnected (error 100).
* Only 100 concurrent market-data lines by default (error 101).
"""

import os
import socket
import struct
import time

from utils.logging import get_logger

logger = get_logger(__name__)

# --- Protocol constants -------------------------------------------------------

# The range advertised in the connect banner. MIN_CLIENT_VER has been 100 since
# the v100 ("useV100Plus") protocol. MAX_CLIENT_VER tracks the SDK release, so it
# is overridable: a client that advertises a version its TWS build does not know
# still negotiates down, but keeping it current avoids surprising gates.
MIN_CLIENT_VER = 100
DEFAULT_MAX_CLIENT_VER = 178

# Server-version gates used below. Names follow the official SDK.
MIN_SERVER_VER_REQ_MATCHING_SYMBOLS = 108
MIN_SERVER_VER_ACCT_SUMMARY = 67
MIN_SERVER_VER_POSITIONS = 67
MIN_SERVER_VER_OPTIONAL_CAPABILITIES = 72
MIN_SERVER_VER_MARKET_CAP_PRICE = 131
MIN_SERVER_VER_ORDER_CONTAINER = 145
MIN_SERVER_VER_SIZE_RULES = 164
MIN_SERVER_VER_SEC_DEF_OPT_PARAMS = 88

# Outgoing message type integers.
OUT_REQ_MKT_DATA = 1
OUT_CANCEL_MKT_DATA = 2
OUT_PLACE_ORDER = 3
OUT_CANCEL_ORDER = 4
OUT_REQ_OPEN_ORDERS = 5
OUT_REQ_ACCT_DATA = 6
OUT_REQ_EXECUTIONS = 7
OUT_REQ_IDS = 8
OUT_REQ_CONTRACT_DATA = 9
OUT_REQ_AUTO_OPEN_ORDERS = 15
OUT_REQ_ALL_OPEN_ORDERS = 16
OUT_REQ_HISTORICAL_DATA = 20
OUT_CANCEL_HISTORICAL_DATA = 25
OUT_REQ_CURRENT_TIME = 49
OUT_REQ_MARKET_DATA_TYPE = 59
OUT_REQ_POSITIONS = 61
OUT_REQ_ACCOUNT_SUMMARY = 62
OUT_CANCEL_ACCOUNT_SUMMARY = 63
OUT_CANCEL_POSITIONS = 64
OUT_START_API = 71
OUT_REQ_MATCHING_SYMBOLS = 81

# Incoming message type integers.
IN_TICK_PRICE = 1
IN_TICK_SIZE = 2
IN_ORDER_STATUS = 3
IN_ERR_MSG = 4
IN_OPEN_ORDER = 5
IN_ACCT_VALUE = 6
IN_PORTFOLIO_VALUE = 7
IN_ACCT_UPDATE_TIME = 8
IN_NEXT_VALID_ID = 9
IN_CONTRACT_DATA = 10
IN_EXECUTION_DATA = 11
IN_MANAGED_ACCTS = 15
IN_HISTORICAL_DATA = 17
IN_TICK_GENERIC = 45
IN_TICK_STRING = 46
IN_CURRENT_TIME = 49
IN_CONTRACT_DATA_END = 52
IN_OPEN_ORDER_END = 53
IN_ACCT_DOWNLOAD_END = 54
IN_EXECUTION_DATA_END = 55
IN_TICK_SNAPSHOT_END = 57
IN_MARKET_DATA_TYPE = 58
IN_POSITION_DATA = 61
IN_POSITION_END = 62
IN_ACCOUNT_SUMMARY = 63
IN_ACCOUNT_SUMMARY_END = 64
IN_SYMBOL_SAMPLES = 79
IN_HISTORICAL_DATA_UPDATE = 90

# Error codes that are not failures. Modeled from IBKR's "Message Codes" table;
# the grouping (benign / transport / order) is this module's synthesis, since
# the table itself does not classify them.
NO_DATA_CODES = frozenset({200, 162, 165, 366})
TRANSPORT_CODES = frozenset(
    {502, 503, 504, 326, 1100, 1101, 1102, 2103, 2104, 2105, 2106, 2107, 2108, 2110, 2158}
)
INFO_CODES = frozenset({2104, 2106, 2158})
RATE_LIMIT_CODES = frozenset({100, 101, 102, 103})
ORDER_REJECT_CODES = frozenset({201, 202, 203, 399, 400})

# Tick types used to build a quote out of the tick stream. Values are IBKR's
# documented tick ids.
TICK_BID_SIZE = 0
TICK_BID = 1
TICK_ASK = 2
TICK_ASK_SIZE = 3
TICK_LAST = 4
TICK_LAST_SIZE = 5
TICK_HIGH = 6
TICK_LOW = 7
TICK_VOLUME = 8
TICK_CLOSE = 9
TICK_OPEN = 14
TICK_OPEN_PRICE = 14

# reqId space for market-data / contract-detail requests. Separate from order
# ids, which the gateway seeds through nextValidId.
_FIRST_REQ_ID = 9000


class TwsError(Exception):
    """Base class for everything raised by this module."""

    def __init__(self, message, code=None, req_id=None):
        super().__init__(message)
        self.code = code
        self.req_id = req_id


class TwsConnectionError(TwsError):
    """Could not reach the gateway, or the handshake did not complete.

    Almost always means TWS/IB Gateway is not running, is not logged in, has
    "Enable ActiveX and Socket Clients" unticked, or is on a different port.
    """


class TwsProtocolError(TwsError):
    """The gateway sent something that does not fit the protocol."""


class TwsRequestTimeout(TwsError):
    """A request's terminal response did not arrive inside its timeout."""


# --- Framing ------------------------------------------------------------------


def encode_field(value):
    """One wire field: the value as text, NUL-terminated.

    ``None`` becomes an empty field, which is how the protocol says "leave this
    value unset". Booleans become ``0``/``1``. Floats use ``repr`` so a price
    round-trips without losing precision.

    .. warning::
       An empty field is **not** an absent field. Because the layout is
       positional and untagged, ``None`` and ``""`` both occupy a slot. A field
       that must be dropped for the negotiated version (a version gate) has to be
       left out of the frame, never passed here as ``None`` -- otherwise every
       field after it shifts by one and the server misparses the request. Build
       gated frames with a list you conditionally append to (see
       :func:`contract_fields`), not a fixed-length tuple.
    """
    if value is None:
        text = ""
    elif isinstance(value, bool):
        text = "1" if value else "0"
    elif isinstance(value, float):
        text = repr(value)
    else:
        text = str(value)
    return text + "\0"


def encode_message(*fields):
    """Wrap fields into a frame: 4-byte big-endian length, then the payload."""
    payload = "".join(encode_field(f) for f in fields).encode("ascii")
    return struct.pack("!I", len(payload)) + payload


def encode_banner(max_client_ver=None):
    """The connect banner.

    ``API\\0`` + [4-byte length][``v<min>..<max>``]. The prefix is deliberately
    outside the length, and the version string has no trailing NUL.
    """
    top = max_client_ver or int(os.getenv("IBKR_MAX_CLIENT_VER") or DEFAULT_MAX_CLIENT_VER)
    version = f"v{MIN_CLIENT_VER}..{top}".encode("ascii")
    return b"API\0" + struct.pack("!I", len(version)) + version


def read_fields(payload):
    """Split a frame payload into fields, dropping the trailing empty one."""
    return tuple(payload.split(b"\0")[:-1])


def decode_text(fields):
    """Fields as ``str``, which is what every decoder here wants."""
    return [f.decode("ascii", errors="replace") for f in fields]


def to_int(value, default=0):
    """A wire field as int, tolerating blanks (unset numerics arrive as "")."""
    try:
        text = value.strip() if isinstance(value, str) else value
        if text in ("", None):
            return default
        return int(float(text))
    except (TypeError, ValueError):
        return default


def to_float(value, default=0.0):
    """A wire field as float, tolerating blanks."""
    try:
        text = value.strip() if isinstance(value, str) else value
        if text in ("", None):
            return default
        return float(text)
    except (TypeError, ValueError):
        return default


# --- Connection ---------------------------------------------------------------


class TwsConnection:
    """One socket to TWS/IB Gateway, with the request/response loop on top.

    Deliberately short-lived: OpenAlgo's broker contract is request/response, and
    one connection per operation keeps the green-thread story simple and makes FD
    leaks impossible to accumulate. The cost is a handshake per call, which is
    the same trade-off ``broker/aliceblue`` makes for its WebSocket-sourced
    quotes.

    Not thread-safe: one connection belongs to the thread that opened it.
    """

    def __init__(self, host, port, client_id, connect_timeout=10.0, request_timeout=20.0):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout

        self.server_version = 0
        self.connection_time = ""
        self.next_order_id = None
        self.managed_accounts = []
        self._sock = None

    # -- lifecycle --

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def connect(self):
        """Open the socket, complete the handshake, and StartApi.

        Returns once ``nextValidId`` has arrived, because the protocol says
        requests sent before it may be silently dropped.
        """
        try:
            self._sock = socket.create_connection(
                (self.host, self.port), timeout=self.connect_timeout
            )
        except OSError as exc:
            raise TwsConnectionError(
                f"Could not reach TWS/IB Gateway at {self.host}:{self.port} ({exc}). "
                "Check that TWS or IB Gateway is running and logged in, that "
                "'Enable ActiveX and Socket Clients' is ticked in API settings, "
                "and that IBKR_PORT matches the configured socket port.",
                code=502,
            ) from exc

        # The socket keeps a read timeout for every request; a slow gateway must
        # raise rather than hang a request thread forever.
        self._sock.settimeout(self.request_timeout)

        try:
            self._sock.sendall(encode_banner())
            self.server_version, self.connection_time = self._read_handshake()
        except TwsError:
            self.close()
            raise
        except OSError as exc:
            self.close()
            raise TwsConnectionError(f"Handshake with TWS failed: {exc}") from exc

        if self.server_version < MIN_CLIENT_VER:
            self.close()
            raise TwsConnectionError(
                f"TWS negotiated API version {self.server_version}, older than the "
                f"minimum this plugin speaks ({MIN_CLIENT_VER}). Update TWS or IB Gateway."
            )

        # optCapab is only expected from server version 72 on, so it is omitted
        # from the frame entirely rather than sent empty (see encode_field).
        start_fields = [OUT_START_API, 2, self.client_id]
        if self.server_version >= MIN_SERVER_VER_OPTIONAL_CAPABILITIES:
            start_fields.append("")
        self.send(*start_fields)

        self._await_ready()
        return self

    def close(self):
        sock, self._sock = self._sock, None
        if sock is None:
            return
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            sock.close()
        except OSError:
            pass

    # -- framing --

    def send(self, *fields):
        """Send one frame. Every value is stringified; ``None`` means unset."""
        if self._sock is None:
            raise TwsConnectionError("Connection is closed")
        # Unset fields (None) are still emitted, as empty fields: the layout is
        # positional, so dropping one shifts every field after it.
        try:
            self._sock.sendall(encode_message(*fields))
        except OSError as exc:
            raise TwsConnectionError(f"Could not send to TWS: {exc}") from exc

    def read_frame(self):
        """Read one frame and return its fields as ``str``."""
        if self._sock is None:
            raise TwsConnectionError("Connection is closed")
        try:
            header = self._recv_exactly(4)
            (size,) = struct.unpack("!I", header)
            payload = self._recv_exactly(size) if size else b""
        except socket.timeout as exc:
            raise TwsRequestTimeout(
                f"No response from TWS within {self.request_timeout}s"
            ) from exc
        except OSError as exc:
            raise TwsConnectionError(f"Could not read from TWS: {exc}") from exc
        return decode_text(read_fields(payload))

    def _recv_exactly(self, count):
        chunks = []
        remaining = count
        while remaining > 0:
            chunk = self._sock.recv(remaining)
            if not chunk:
                raise TwsConnectionError("TWS closed the connection")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _read_handshake(self):
        """The server's answer: exactly two fields, ``[serverVersion, time]``."""
        fields = self.read_frame()
        if len(fields) < 2:
            raise TwsProtocolError(
                f"Bad handshake reply from TWS: expected 2 fields, got {len(fields)}"
            )
        return to_int(fields[0]), fields[1]

    def _await_ready(self):
        """Consume the opening chatter until ``nextValidId``.

        Also absorbs the informational farm-status errors (2104/2106/2158) TWS
        sends with ``reqId = -1``, which are not failures.
        """
        deadline = time.monotonic() + self.request_timeout
        while time.monotonic() < deadline:
            fields = self.read_frame()
            kind = to_int(fields[0])
            if kind == IN_NEXT_VALID_ID:
                # [9, version, orderId]
                self.next_order_id = to_int(fields[2]) if len(fields) > 2 else 0
                return
            if kind == IN_MANAGED_ACCTS and len(fields) > 2:
                self.managed_accounts = [a for a in fields[2].split(",") if a]
            elif kind == IN_ERR_MSG:
                self._raise_if_fatal(fields)
            # Anything else arriving this early is not ours; ignore it.
        raise TwsRequestTimeout(
            "TWS did not send nextValidId; the connection is not ready. "
            "'Read-Only API' being enabled, or an unlogged-in session, both do this."
        )

    @staticmethod
    def _raise_if_fatal(fields):
        """Turn a fatal error frame into an exception; ignore informational ones.

        Layout: ``[4, version, reqId, errorCode, errorString, ...]``.
        """
        code = to_int(fields[3]) if len(fields) > 3 else 0
        message = fields[4] if len(fields) > 4 else ""
        if code in INFO_CODES:
            logger.debug("TWS notice %s: %s", code, message)
            return
        detail = f"TWS error {code}: {message}" if message else f"TWS error {code}"
        if code in TRANSPORT_CODES:
            raise TwsConnectionError(detail, code=code)
        raise TwsError(detail, code=code)

    # -- request helpers --

    def collect(self, terminal_types, *, is_terminal=None, timeout=None, fatal_codes=()):
        """Read frames until one satisfies ``is_terminal`` (or a terminal type).

        Args:
            terminal_types: message ids that end the request.
            is_terminal: optional predicate taking the decoded text fields;
                overrides the plain type check where a type can arrive more than
                once per request (``tickSnapshotEnd`` is the usual case).
            timeout: override the connection default.
            fatal_codes: error codes to raise on instead of collecting.

        Returns:
            list[list[str]]: every frame read, including the terminal one.
        """
        deadline = time.monotonic() + (timeout if timeout is not None else self.request_timeout)
        seen = []
        while time.monotonic() < deadline:
            fields = self.read_frame()
            seen.append(fields)
            kind = to_int(fields[0])
            if kind == IN_ERR_MSG:
                code = to_int(fields[3]) if len(fields) > 3 else 0
                if code in fatal_codes:
                    self._raise_if_fatal(fields)
                elif code in TRANSPORT_CODES:
                    self._raise_if_fatal(fields)
                else:
                    logger.debug("TWS error during collect: %s", fields)
            if is_terminal is not None:
                if is_terminal(fields):
                    return seen
            elif kind in terminal_types:
                return seen
        raise TwsRequestTimeout(
            "TWS did not send a terminal response in time "
            f"(waited {timeout if timeout is not None else self.request_timeout}s)"
        )

    # -- high-level requests --

    def _next_req_id(self):
        self._req_seq = getattr(self, "_req_seq", _FIRST_REQ_ID) + 1
        return self._req_seq

    def current_time(self):
        """The gateway's clock, in epoch seconds. The cheapest liveness probe."""
        self.send(OUT_REQ_CURRENT_TIME, 1)
        for fields in self.collect({IN_CURRENT_TIME}):
            if to_int(fields[0]) == IN_CURRENT_TIME:
                return to_int(fields[2]) if len(fields) > 2 else 0
        return 0

    def contract_details(self, contract, timeout=None):
        """Resolve a contract to its tradable definition(s).

        May return several matches -- an under-specified option contract returns
        the whole chain -- so callers should pick by ``conId``.
        """
        req_id = self._next_req_id()
        fields = [OUT_REQ_CONTRACT_DATA, 8]
        if self.server_version >= 40:
            fields.append(req_id)
        # The sec-id pair has to be held back: reqContractDetails interposes
        # includeExpired between tradingClass and secIdType/secId, unlike the
        # market-data and order frames, where the pair follows tradingClass.
        fields.extend(contract_fields(contract, self.server_version, include_sec_ids=False))
        fields.append(0)  # includeExpired
        if self.server_version >= 45:
            fields.append(contract.get("secIdType", ""))
            fields.append(contract.get("secId", ""))
        if self.server_version >= 176:
            fields.append(contract.get("issuerId", ""))
        self.send(*fields)

        results = []
        seen = self.collect({IN_CONTRACT_DATA_END}, timeout=timeout)
        for frame in seen:
            if to_int(frame[0]) == IN_CONTRACT_DATA:
                details = decode_contract_details(frame, self.server_version)
                if details.get("reqId") == req_id:
                    results.append(details)
        return results

    def matching_symbols(self, pattern, timeout=None):
        """Fuzzy symbol lookup. Requires server version 108 or newer."""
        if self.server_version < MIN_SERVER_VER_REQ_MATCHING_SYMBOLS:
            raise TwsError(
                "reqMatchingSymbols needs TWS API version 108 or newer; this gateway "
                f"negotiated {self.server_version}"
            )
        req_id = self._next_req_id()
        self.send(OUT_REQ_MATCHING_SYMBOLS, req_id, pattern)
        samples = []
        for frame in self.collect({IN_SYMBOL_SAMPLES}, timeout=timeout):
            if to_int(frame[0]) != IN_SYMBOL_SAMPLES:
                continue
            idx = 2
            count = to_int(frame[idx]) if idx < len(frame) else 0
            idx += 1
            for _ in range(count):
                if idx + 5 >= len(frame):
                    break
                sample = {
                    "conId": to_int(frame[idx]),
                    "symbol": frame[idx + 1],
                    "secType": frame[idx + 2],
                    "primaryExchange": frame[idx + 3],
                    "currency": frame[idx + 4],
                }
                idx += 5
                derivatives = to_int(frame[idx]) if idx < len(frame) else 0
                idx += 1 + derivatives
                samples.append(sample)
        return samples

    def snapshot(self, contract, timeout=None):
        """One-shot market-data snapshot, assembled from the tick stream.

        A snapshot request must carry an empty generic tick list, so the gateway
        answers with whatever ticks it has and then ``tickSnapshotEnd``.
        """
        req_id = self._next_req_id()
        self.send(*_market_data_request_fields(req_id, contract, self.server_version))
        quote = {}
        try:
            frames = self.collect(
                {IN_TICK_SNAPSHOT_END},
                is_terminal=lambda f: to_int(f[0]) == IN_TICK_SNAPSHOT_END
                and to_int(f[2]) == req_id,
                timeout=timeout,
            )
        finally:
            # The subscription is one-shot, but cancelling keeps the ticker id
            # free and the market-data line released if the snapshot never ended.
            try:
                self.send(OUT_CANCEL_MKT_DATA, 2, req_id)
            except TwsError:
                pass
        for frame in frames:
            self._apply_tick(quote, frame, req_id)
        return quote

    @staticmethod
    def _apply_tick(quote, frame, req_id):
        """Fold one tick frame into ``quote``."""
        kind = to_int(frame[0])
        if kind not in (IN_TICK_PRICE, IN_TICK_SIZE, IN_TICK_GENERIC, IN_TICK_STRING):
            return
        if len(frame) < 4 or to_int(frame[2]) != req_id:
            return
        tick_type = to_int(frame[3])
        if kind == IN_TICK_PRICE and len(frame) > 4:
            price = to_float(frame[4])
            mapping = {
                TICK_BID: "bid",
                TICK_ASK: "ask",
                TICK_LAST: "ltp",
                TICK_HIGH: "high",
                TICK_LOW: "low",
                TICK_VOLUME: "volume",
                TICK_CLOSE: "prev_close",
                TICK_OPEN: "open",
            }
            if tick_type in mapping and price:
                quote[mapping[tick_type]] = price
        elif kind == IN_TICK_SIZE and len(frame) > 4:
            size = to_float(frame[4])
            if tick_type == TICK_BID_SIZE:
                quote["bid_size"] = size
            elif tick_type == TICK_ASK_SIZE:
                quote["ask_size"] = size
            elif tick_type == TICK_VOLUME:
                quote.setdefault("volume", size)

    def historical_bars(self, contract, *, end_datetime="", duration="1 M",
                        bar_size="1 day", what_to_show="TRADES", use_rth=True,
                        timeout=None):
        """Historical bars for one contract.

        IBKR paces this hard (no identical request within 15s, at most 60 in ten
        minutes, ``BID_ASK`` counts double), so callers must not loop tightly.
        """
        req_id = self._next_req_id()
        self.send(
            *_historical_request_fields(
                req_id, contract, end_datetime, duration, bar_size, what_to_show,
                use_rth, self.server_version,
            )
        )
        for frame in self.collect({IN_HISTORICAL_DATA}, timeout=timeout):
            if to_int(frame[0]) == IN_HISTORICAL_DATA:
                decoded_req, data = decode_historical_bars(frame, self.server_version)
                if decoded_req == req_id:
                    return data
        return {"start": "", "end": "", "bars": []}

    def positions(self, timeout=None):
        """Open positions across the account."""
        self.send(OUT_REQ_POSITIONS, 1)
        rows = []
        for frame in self.collect({IN_POSITION_END}, timeout=timeout):
            if to_int(frame[0]) == IN_POSITION_DATA:
                rows.append(decode_positions(frame))
        return rows

    def account_summary(self, tags="NetLiquidation,TotalCashValue,BuyingPower,AvailableFunds",
                        group="All", timeout=None):
        """Account summary rows for the given tags."""
        req_id = self._next_req_id()
        self.send(OUT_REQ_ACCOUNT_SUMMARY, 1, req_id, group, tags)
        rows = []
        for frame in self.collect({IN_ACCOUNT_SUMMARY_END}, timeout=timeout):
            if to_int(frame[0]) == IN_ACCOUNT_SUMMARY and len(frame) > 5:
                rows.append(
                    {
                        "reqId": to_int(frame[2]),
                        "account": frame[3],
                        "tag": frame[4],
                        "value": frame[5],
                        "currency": frame[6] if len(frame) > 6 else "",
                    }
                )
        try:
            self.send(OUT_CANCEL_ACCOUNT_SUMMARY, 1, req_id)
        except TwsError:
            pass
        return rows

    def account_values(self, account="", timeout=None):
        """Account update key/values, plus the portfolio rows.

        Returns ``(values, portfolio)``. IBKR allows only one account-update
        subscription at a time, so this subscribes, waits for the download to
        finish, then unsubscribes.
        """
        self.send(OUT_REQ_ACCT_DATA, 2, 1, account)
        values = {}
        portfolio = []
        try:
            for frame in self.collect({IN_ACCT_DOWNLOAD_END}, timeout=timeout):
                kind = to_int(frame[0])
                if kind == IN_ACCT_VALUE and len(frame) > 4:
                    values[frame[2]] = {"value": frame[3], "currency": frame[4]}
                elif kind == IN_PORTFOLIO_VALUE:
                    portfolio.append(frame)
        finally:
            try:
                self.send(OUT_REQ_ACCT_DATA, 2, 0, account)
            except TwsError:
                pass
        return values, portfolio

    def executions(self, account="", timeout=None):
        """Today's executions (IBKR only exposes the last 24 hours here)."""
        fields = [OUT_REQ_EXECUTIONS, 3]
        if self.server_version >= 42:
            fields.append(self._next_req_id())
        fields.extend([0, account, "", "", "", "", ""])
        self.send(*fields)
        rows = []
        for frame in self.collect({IN_EXECUTION_DATA_END}, timeout=timeout):
            if to_int(frame[0]) == IN_EXECUTION_DATA:
                rows.append(decode_execution(frame, self.server_version))
        return rows

    def all_open_orders(self, timeout=None):
        """Every open order on the account, as raw OPEN_ORDER/ORDER_STATUS frames.

        Deliberately returns the raw frames: OPEN_ORDER carries the same ~120
        positional fields as ``place_order`` followed by an order-state block, and
        mapping them is the mapper's job, where it can be tested against recorded
        frames. See ``broker/ibkr/mapping/order_data.py``.
        """
        self.send(OUT_REQ_ALL_OPEN_ORDERS, 1)
        return self.collect({IN_OPEN_ORDER_END}, timeout=timeout)

    def place_order(self, order_id, contract, order, timeout=None):
        """Submit one order. Returns the frames the gateway answered with."""
        self.send(*build_place_order_frame(order_id, contract, order, self.server_version))
        # openOrder + orderStatus normally follow; wait for a status for our id,
        # but do not fail the placement just because status is slow to arrive.
        try:
            return self.collect(
                {IN_ORDER_STATUS},
                is_terminal=lambda f: to_int(f[0]) == IN_ORDER_STATUS
                and to_int(f[2]) == order_id,
                timeout=timeout,
            )
        except TwsRequestTimeout:
            logger.debug("No orderStatus for order %s within the timeout", order_id)
            return []

    def cancel_order(self, order_id):
        """Cancel one order."""
        fields = [OUT_CANCEL_ORDER, 1, order_id]
        if self.server_version >= 169:
            fields.append("")  # manualCancelOrderTime
        self.send(*fields)


# --- Contract / order field builders -----------------------------------------


def contract_fields(contract, server_version, include_sec_ids=True):
    """The positional contract block used by contract-details, market-data and orders.

    Wire order (not the struct order)::

        conId, symbol, secType, lastTradeDateOrContractMonth, strike, right,
        multiplier, exchange, primaryExchange, currency, localSymbol,
        [tradingClass] [secIdType, secId]

    The bracketed fields are version-gated and are **omitted from the list** when
    the negotiated version is too old. They must not be passed as ``None``: see
    the warning on :func:`encode_field` -- an empty field still occupies a
    position, so a gated-out field sent as ``None`` would shift every field after
    it and silently misparse the frame.

    Args:
        include_sec_ids: ``reqContractDetails`` interposes ``includeExpired``
            *before* the sec-id pair, so it builds its own tail and passes
            ``False`` here. Market-data and order frames take the pair directly.
    """
    security_type = (contract.get("secType") or "").upper()
    fields = [
        contract.get("conId", 0),
        contract.get("symbol", ""),
        security_type,
        contract.get("lastTradeDateOrContractMonth", ""),
        contract.get("strike"),
        contract.get("right", ""),
        contract.get("multiplier", ""),
        contract.get("exchange", "SMART"),
        contract.get("primaryExchange", ""),
        contract.get("currency", "USD"),
        contract.get("localSymbol", ""),
    ]
    # tradingClass is only expected from server version 68 on.
    if server_version >= 68:
        fields.append(contract.get("tradingClass", ""))
    # secIdType/secId are only expected from server version 45 on.
    if include_sec_ids and server_version >= 45:
        fields.append(contract.get("secIdType", ""))
        fields.append(contract.get("secId", ""))
    return fields


def equity_contract(symbol, primary_exchange="", currency="USD"):
    """A US equity contract. ``SMART`` routes; ``primaryExchange`` disambiguates."""
    return {
        "symbol": symbol.upper(),
        "secType": "STK",
        "exchange": "SMART",
        "primaryExchange": primary_exchange,
        "currency": currency,
    }


def option_contract(symbol, expiry, strike, right, multiplier="100", currency="USD"):
    """A US option contract.

    ``expiry`` is ``YYYYMMDD``, ``right`` is ``C`` or ``P``. This is the shape
    both ``reqContractDetails`` (to resolve a ``conId``) and orders accept.
    """
    return {
        "symbol": symbol.upper(),
        "secType": "OPT",
        "lastTradeDateOrContractMonth": str(expiry),
        "strike": strike,
        "right": right.upper()[:1],
        "multiplier": str(multiplier),
        "exchange": "SMART",
        "currency": currency,
    }


def build_order(**overrides):
    """A retail US equity/option order with every protocol field defaulted.

    Only the fields a caller actually sets are meaningful; the rest exist because
    the frame is positional and a short one corrupts the stream (IBKR error 320).
    """
    order = {
        "action": "BUY",
        "totalQuantity": 0,
        "orderType": "LMT",
        "lmtPrice": None,
        "auxPrice": None,
        "tif": "DAY",
        "account": "",
        "outsideRth": False,
        "orderRef": "",
        "transmit": True,
        "parentId": 0,
    }
    order.update(overrides)
    return order


# Placing an order is the one request where a mistake does not fail cleanly.
# The frame is ~120 positional fields with no tags, so a field too few shifts
# everything after it and the gateway misparses the *next* order's bytes too
# (IBKR error 320, "Unable to parse field"). Every field below is therefore
# emitted explicitly, deprecated ones included.

# Order types whose field blocks this plugin cannot emit faithfully. The
# pegged-to-benchmark block (server version 102+) and the PEG BEST/MID block
# (server version 170+) carry condition and adjusted-order sub-frames whose
# exact layout is not published in IBKR's prose documentation and is not
# enumerated in the reference this plugin was built from. Guessing them would
# corrupt the stream, so they are refused with a clear message instead. Plain
# US equity and option orders (MKT/LMT/STP/STP LMT/TRAIL) are unaffected.
UNSUPPORTED_ORDER_TYPES = frozenset(
    {"PEG BENCH", "PEGBENCH", "PEG BEST", "PEGBEST", "PEG MID", "PEGMID", "REL", "VWAP"}
)


def build_place_order_frame(order_id, contract, order, server_version):
    """The full ``placeOrder`` frame for the negotiated server version.

    Mirrors the official client's send order. The version field itself is only
    sent below server version 145.

    Raises:
        TwsError: for an order type whose field block is not implemented here,
            rather than emitting a frame that would corrupt the stream.
    """
    order_type = (order.get("orderType") or "LMT").upper()
    if order_type in UNSUPPORTED_ORDER_TYPES:
        raise TwsError(
            f"Order type {order_type!r} needs IBKR's pegged/conditions field block, "
            "which this plugin does not emit. Use MKT, LMT, STP, STP LMT or TRAIL."
        )

    fields = [OUT_PLACE_ORDER]
    if server_version < MIN_SERVER_VER_ORDER_CONTAINER:
        fields.append(45 if server_version >= 44 else 27)
    fields.append(order_id)
    # Contract block. secIdType/secId follow tradingClass here, unlike
    # reqContractDetails which interposes includeExpired first.
    fields.extend(contract_fields(contract, server_version))

    tier = order.get("softDollarTier") or {}
    security_type = (contract.get("secType") or "").upper()

    fields.extend([
        order.get("action", "BUY"),
        order.get("totalQuantity", 0),
        order_type,
        order.get("lmtPrice"),
        order.get("auxPrice"),
        order.get("tif", "DAY"),
        order.get("ocaGroup", ""),
        order.get("account", ""),
        order.get("openClose", ""),
        order.get("origin", 0),
        order.get("orderRef", ""),
        order.get("transmit", True),
        order.get("parentId", 0),
        order.get("blockOrder", False),
        order.get("sweepToFill", False),
        order.get("displaySize", 0),
        order.get("triggerMethod", 0),
        order.get("outsideRth", False),
        order.get("hidden", False),
    ])

    # A BAG (combo) contract would carry its legs here, between the fields above
    # and sharesAllocation. This plugin never builds one; if a BAG ever reaches
    # this function the frame would be wrong, so refuse it explicitly.
    if security_type == "BAG":
        raise TwsError("BAG (combo) orders are not supported by this plugin")

    fields.extend([
        "",  # deprecated sharesAllocation
        order.get("discretionaryAmt", 0),
        order.get("goodAfterTime", ""),
        order.get("goodTillDate", ""),
        order.get("faGroup", ""),
        order.get("faMethod", ""),
        order.get("faPercentage", ""),
    ])
    if server_version < 177:
        fields.append("")  # deprecated faProfile
    if server_version >= 103:
        fields.append(order.get("modelCode", ""))
    fields.extend([
        order.get("shortSaleSlot", 0),
        order.get("designatedLocation", ""),
    ])
    if server_version >= 51:
        fields.append(order.get("exemptCode", -1))
    fields.extend([
        order.get("ocaType", 0),
        order.get("rule80A", ""),
        order.get("settlingFirm", ""),
        order.get("allOrNone", False),
        order.get("minQty"),
        order.get("percentOffset"),
        0,  # eTradeOnly, deprecated
        0,  # firmQuoteOnly, deprecated
        None,  # nbboPriceCap, deprecated
        order.get("auctionStrategy", 0),
        order.get("startingPrice"),
        order.get("stockRefPrice"),
        order.get("delta"),
        order.get("stockRangeLower"),
        order.get("stockRangeUpper"),
        order.get("overridePercentageConstraints", False),
        order.get("volatility"),
        order.get("volatilityType"),
        order.get("deltaNeutralOrderType", ""),
        order.get("deltaNeutralAuxPrice"),
    ])

    if server_version >= 58 and order.get("deltaNeutralOrderType"):
        fields.extend([
            order.get("deltaNeutralConId", 0),
            order.get("deltaNeutralSettlingFirm", ""),
            order.get("deltaNeutralClearingAccount", ""),
            order.get("deltaNeutralClearingIntent", ""),
        ])
        if server_version >= 66:
            fields.extend([
                order.get("deltaNeutralOpenClose", ""),
                order.get("deltaNeutralShortSale", False),
                order.get("deltaNeutralShortSaleSlot", 0),
                order.get("deltaNeutralDesignatedLocation", ""),
            ])

    fields.extend([
        order.get("continuousUpdate", False),
        order.get("referencePriceType"),
        order.get("trailStopPrice"),
    ])
    if server_version >= 62:
        fields.append(order.get("trailingPercent"))
    if server_version >= 40:
        fields.extend([
            order.get("scaleInitLevelSize"),
            order.get("scaleSubsLevelSize"),
        ])
        fields.append(order.get("scalePriceIncrement"))
        if server_version >= 60 and order.get("scalePriceIncrement"):
            fields.extend([
                order.get("scalePriceAdjustValue"),
                order.get("scalePriceAdjustInterval"),
                order.get("scaleProfitOffset"),
                order.get("scaleAutoReset", False),
                order.get("scaleInitPosition"),
                order.get("scaleInitFillQty"),
                order.get("scaleRandomPercent", False),
            ])
    if server_version >= 69:
        fields.extend([
            order.get("scaleTable", ""),
            order.get("activeStartTime", ""),
            order.get("activeStopTime", ""),
        ])
    if server_version >= 54:
        hedge_type = order.get("hedgeType", "")
        fields.append(hedge_type)
        if hedge_type:
            fields.append(order.get("hedgeParam", ""))
    if server_version >= 56:
        fields.append(order.get("optOutSmartRouting", False))
    if server_version >= 39:
        fields.extend([
            order.get("clearingAccount", ""),
            order.get("clearingIntent", ""),
        ])
    if server_version >= 44:
        fields.append(order.get("notHeld", False))
    if server_version >= 40:
        fields.append(0)  # no delta-neutral contract
    if server_version >= 41:
        # No algo strategy, so the parameter list is empty.
        fields.append(order.get("algoStrategy", ""))
        if order.get("algoStrategy"):
            params = order.get("algoParams") or []
            fields.append(len(params))
            for param in params:
                fields.extend([param.get("tag", ""), param.get("value", "")])
    if server_version >= 71:
        fields.append(order.get("algoId", ""))
    fields.append(order.get("whatIf", False))
    if server_version >= 70:
        fields.append(order.get("miscOptions", ""))
    if server_version >= 73:
        fields.append(order.get("solicited", False))
    if server_version >= 76:
        fields.extend([
            order.get("randomizeSize", False),
            order.get("randomizePrice", False),
        ])

    # ------------------------------------------------------------------
    # Server version 102+ tail (pegged-to-benchmark, conditions, adjusted
    # order), and the 170+ PEG BEST/MID tail, are NOT emitted.
    #
    # They require condition and adjusted-order sub-frames whose exact layout
    # this plugin's reference does not enumerate, and guessing would shift every
    # field after them. An order type that needs them is refused at the top of
    # this function, so reaching here means the tail is the well-known
    # empty-conditions / no-adjusted-order case. Those fields are still emitted
    # because the parser expects the positions to exist.
    # ------------------------------------------------------------------
    fields.extend([
        0,  # conditions: none
        "",  # adjustedOrderType
        None,  # triggerPrice
        None,  # lmtPriceOffset
        None,  # adjustedStopPrice
        None,  # adjustedStopLimitPrice
        None,  # adjustedTrailingAmount
        0,  # adjustableTrailingUnit
    ])
    if server_version >= 105:
        fields.append(order.get("extOperator", ""))
    if server_version >= 106:
        fields.extend([tier.get("name", ""), tier.get("val", "")])
    if server_version >= 111:
        fields.append(order.get("cashQty"))
    if server_version >= 138:
        fields.extend([
            order.get("mifid2DecisionMaker", ""),
            order.get("mifid2DecisionAlgo", ""),
        ])
    if server_version >= 139:
        fields.extend([
            order.get("mifid2ExecutionTrader", ""),
            order.get("mifid2ExecutionAlgo", ""),
        ])
    if server_version >= 141:
        fields.append(order.get("dontUseAutoPriceForHedge", True))
    if server_version >= MIN_SERVER_VER_ORDER_CONTAINER:
        fields.append(order.get("isOmsContainer", False))
    if server_version >= 148:
        fields.append(order.get("discretionaryUpToLimitPrice", False))
    if server_version >= 151:
        fields.append(order.get("usePriceMgmtAlgo"))
    if server_version >= 158:
        fields.append(order.get("duration", -1))
    if server_version >= 160:
        fields.append(order.get("postToAts", 0))
    if server_version >= 162:
        fields.append(order.get("autoCancelParent", True))
    if server_version >= 166:
        fields.append(order.get("advancedErrorOverride", ""))
    if server_version >= 169:
        fields.append(order.get("manualOrderTime", ""))
    return fields


# --- Decoders -----------------------------------------------------------------


def decode_contract_details(fields, server_version):
    """CONTRACT_DATA (10) -> a plain dict.

    Positional layout, from the SDK decoder. The leading ``version`` field is
    only present below server version 164, and several fields in the middle are
    gated; getting this wrong misreads every later field rather than failing.
    """
    idx = 1
    if server_version < MIN_SERVER_VER_SIZE_RULES:
        message_version = to_int(fields[idx])
        idx += 1
    else:
        message_version = 8

    def take(default=""):
        nonlocal idx
        value = fields[idx] if idx < len(fields) else default
        idx += 1
        return value

    req_id = to_int(take()) if message_version >= 3 else 0
    details = {
        "reqId": req_id,
        "symbol": take(),
        "secType": take(),
        "lastTradeDateOrContractMonth": take(),
        "strike": to_float(take()),
        "right": take(),
        "exchange": take(),
        "currency": take(),
        "localSymbol": take(),
        "marketName": take(),
        "tradingClass": take(),
        "conId": to_int(take()),
        "minTick": to_float(take()),
    }
    if 110 <= server_version < MIN_SERVER_VER_SIZE_RULES:
        details["mdSizeMultiplier"] = to_int(take())
    details["multiplier"] = take()
    details["orderTypes"] = take()
    details["validExchanges"] = take()
    details["priceMagnifier"] = to_int(take())
    if message_version >= 4:
        details["underConId"] = to_int(take())
    if message_version >= 5:
        details["longName"] = take()
        details["primaryExchange"] = take()
    if message_version >= 6:
        details["contractMonth"] = take()
        details["industry"] = take()
        details["category"] = take()
        details["subcategory"] = take()
        details["timeZoneId"] = take()
        details["tradingHours"] = take()
        details["liquidHours"] = take()
    if message_version >= 8:
        details["evRule"] = take()
        details["evMultiplier"] = to_float(take())
    if message_version >= 7:
        count = to_int(take())
        sec_ids = {}
        for _ in range(count):
            tag = take()
            sec_ids[tag] = take()
        details["secIdList"] = sec_ids
    if server_version >= 121:
        details["aggGroup"] = to_int(take())
    if server_version >= 122:
        details["underSymbol"] = take()
        details["underSecType"] = take()
    if server_version >= 126:
        details["marketRuleIds"] = take()
    if server_version >= 134:
        details["realExpirationDate"] = take()
    if server_version >= 152:
        details["stockType"] = take()
    if 163 <= server_version < MIN_SERVER_VER_SIZE_RULES:
        details["sizeMinTick"] = to_float(take())
    if server_version >= MIN_SERVER_VER_SIZE_RULES:
        details["minSize"] = to_float(take())
        details["sizeIncrement"] = to_float(take())
        details["suggestedSizeIncrement"] = to_float(take())
    return details


def decode_historical_bars(fields, server_version):
    """HISTORICAL_DATA (17) -> ``(req_id, bars)``.

    Layout: ``[17, (version if sv<124), reqId, startDate, endDate, barCount,
    {date, open, high, low, close, volume, wap, (unused if sv<124), barCount}...]``.

    There is no separate end-of-history message on the wire: the SDK synthesises
    one once the bars in this single frame are consumed, which is why this
    function returns a complete list rather than a stream.
    """
    idx = 1
    if server_version < 124:
        idx += 1  # the message's own version field
    req_id = to_int(fields[idx])
    idx += 1
    start_date = fields[idx] if idx < len(fields) else ""
    idx += 1
    end_date = fields[idx] if idx < len(fields) else ""
    idx += 1
    bar_count = to_int(fields[idx]) if idx < len(fields) else 0
    idx += 1

    bars = []
    for _ in range(bar_count):
        if idx + 6 >= len(fields) + 1:
            break
        bar = {
            "date": fields[idx],
            "open": to_float(fields[idx + 1]),
            "high": to_float(fields[idx + 2]),
            "low": to_float(fields[idx + 3]),
            "close": to_float(fields[idx + 4]),
            "volume": to_float(fields[idx + 5]),
            "wap": to_float(fields[idx + 6]),
        }
        idx += 7
        if server_version < 124:
            idx += 1  # an unused string the old format carried
        idx += 1  # the per-bar barCount
        bars.append(bar)
    return req_id, {"start": start_date, "end": end_date, "bars": bars}


def decode_execution(fields, server_version):
    """EXECUTION_DATA (11) -> a dict.

    Only the fields this plugin maps are pulled out; the frame's tail is
    version-dependent and ignored past the point we stop reading.
    """
    idx = 1
    if server_version < 136:
        exec_version = to_int(fields[idx])
        idx += 1
    else:
        exec_version = 10
    req_id = to_int(fields[idx]) if exec_version >= 7 else 0
    if exec_version >= 7:
        idx += 1
    return {
        "reqId": req_id,
        "orderId": to_int(fields[idx]) if idx < len(fields) else 0,
        "conId": to_int(fields[idx + 1]) if idx + 1 < len(fields) else 0,
        "symbol": fields[idx + 2] if idx + 2 < len(fields) else "",
        "secType": fields[idx + 3] if idx + 3 < len(fields) else "",
        "lastTradeDateOrContractMonth": fields[idx + 4] if idx + 4 < len(fields) else "",
        "strike": to_float(fields[idx + 5]) if idx + 5 < len(fields) else 0.0,
        "right": fields[idx + 6] if idx + 6 < len(fields) else "",
        "_raw": fields,
    }


# --- Requests -----------------------------------------------------------------


def _historical_request_fields(req_id, contract, end_datetime, duration, bar_size,
                               what_to_show, use_rth, server_version):
    """Build the ``reqHistoricalData`` field list, honouring the version gates.

    Gates here are ``append``-based for the same reason as ``contract_fields``: a
    gated-out field must not occupy a slot.
    """
    fields = [OUT_REQ_HISTORICAL_DATA]
    if server_version < 124:
        fields.append(6)
    fields.append(req_id)
    if server_version >= 68:
        fields.append(contract.get("conId", 0))
    fields.extend([
        contract.get("symbol", ""),
        contract.get("secType", ""),
        contract.get("lastTradeDateOrContractMonth", ""),
        contract.get("strike"),
        contract.get("right", ""),
        contract.get("multiplier", ""),
        contract.get("exchange", "SMART"),
        contract.get("primaryExchange", ""),
        contract.get("currency", "USD"),
        contract.get("localSymbol", ""),
    ])
    if server_version >= 68:
        fields.append(contract.get("tradingClass", ""))
    fields.extend([
        0,  # includeExpired
        end_datetime,
        bar_size,
        duration,
        1 if use_rth else 0,
        what_to_show,
        2,  # formatDate=2 -> epoch seconds for intraday bars
    ])
    if server_version >= 124:
        fields.append(0)  # keepUpToDate
    if server_version >= 70:
        fields.append("")  # chartOptions
    return fields


def _market_data_request_fields(req_id, contract, server_version, snapshot=True):
    """Build the ``reqMktData`` field list (VERSION 11).

    A snapshot must carry an empty generic tick list, or IBKR answers error 414.
    """
    fields = [OUT_REQ_MKT_DATA, 11, req_id]
    if server_version >= 47:
        fields.append(contract.get("conId", 0))
    fields.extend([
        contract.get("symbol", ""),
        contract.get("secType", ""),
        contract.get("lastTradeDateOrContractMonth", ""),
        contract.get("strike"),
        contract.get("right", ""),
        contract.get("multiplier", ""),
        contract.get("exchange", "SMART"),
        contract.get("primaryExchange", ""),
        contract.get("currency", "USD"),
        contract.get("localSymbol", ""),
    ])
    if server_version >= 68:
        fields.append(contract.get("tradingClass", ""))
    # combo legs only apply to BAG contracts, which this plugin does not use.
    if server_version >= 40:
        fields.append(0)  # deltaNeutralContract present flag
    if server_version >= 31:
        fields.append("")  # genericTickList -- must be empty for a snapshot
    if server_version >= 35:
        fields.append(1 if snapshot else 0)
    if server_version >= 114:
        fields.append(0)  # regulatorySnapshot
    if server_version >= 70:
        fields.append("")  # mktDataOptions
    return fields


def decode_positions(fields):
    """POSITION_DATA (61) -> dict. The version field is always present here."""
    message_version = to_int(fields[1]) if len(fields) > 1 else 1
    idx = 2
    position = {
        "account": fields[idx] if idx < len(fields) else "",
        "conId": to_int(fields[idx + 1]) if idx + 1 < len(fields) else 0,
        "symbol": fields[idx + 2] if idx + 2 < len(fields) else "",
        "secType": fields[idx + 3] if idx + 3 < len(fields) else "",
        "lastTradeDateOrContractMonth": fields[idx + 4] if idx + 4 < len(fields) else "",
        "strike": to_float(fields[idx + 5]) if idx + 5 < len(fields) else 0.0,
        "right": fields[idx + 6] if idx + 6 < len(fields) else "",
        "multiplier": fields[idx + 7] if idx + 7 < len(fields) else "",
        "exchange": fields[idx + 8] if idx + 8 < len(fields) else "",
        "currency": fields[idx + 9] if idx + 9 < len(fields) else "",
        "localSymbol": fields[idx + 10] if idx + 10 < len(fields) else "",
    }
    idx += 11
    if message_version >= 2:
        position["tradingClass"] = fields[idx] if idx < len(fields) else ""
        idx += 1
    position["position"] = to_float(fields[idx]) if idx < len(fields) else 0.0
    idx += 1
    position["avgCost"] = to_float(fields[idx]) if (message_version >= 3 and idx < len(fields)) else 0.0
    return position
