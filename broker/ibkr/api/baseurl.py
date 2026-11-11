"""
IBKR connection settings.

IBKR is unlike every other broker plugin in this tree: it is **not** reached over
HTTP. TWS and IB Gateway are desktop applications that expose a plain TCP socket
on the machine running them, and the trading session lives inside that
application -- there is no cloud endpoint, no API key, and nothing to store as an
OAuth token.

Consequences that shape the whole plugin:

* ``authenticate_broker`` cannot exchange a credential for a token. It connects
  to the socket and asks the gateway whether the session is live and the account
  is authorised. What it stores is a *connection descriptor*, not a secret.
* Nothing works unless TWS or IB Gateway is running, is logged in, and has
  "Enable ActiveX and Socket Clients" ticked in API settings. IBKR re-authenticates
  the desktop session on its own schedule (TWS roughly weekly, IB Gateway roughly
  daily), so an outage is a normal state to report, not a bug.
* The socket is loopback-only by default. Do not point ``IBKR_HOST`` at a remote
  machine: the TWS socket protocol is unauthenticated and unencrypted, so
  exposing it off-host hands anyone on the network full trading access.

These settings stay in ``.env`` rather than Broker Configuration -- the standard
Broker Configuration form only saves ``BROKER_API_KEY`` / ``BROKER_API_SECRET``,
which do not apply here. See ``utils/broker_setup_guide.py``.
"""

import os

# --- Defaults -----------------------------------------------------------------
#
# Loopback. See the warning above before changing this.
DEFAULT_HOST = "127.0.0.1"

# TWS and IB Gateway listen on different ports for live vs paper. IBKR's
# documented defaults:
#   TWS        live 7496   paper 7497
#   IB Gateway live 4001   paper 4002
# ``IBKR_PORT`` overrides whichever default the paper toggle selects.
TWS_PORT_LIVE = 7496
TWS_PORT_PAPER = 7497
GATEWAY_PORT_LIVE = 4001
GATEWAY_PORT_PAPER = 4002

# A clientId identifies this API connection to the gateway. IBKR only allows one
# live connection per clientId; a second one with the same id is rejected with
# error 326 ("client id is already in use"). OpenAlgo opens connections per
# request, so the default is deliberately outside the range a human is likely to
# have left open from a manual TWS session or another bot.
DEFAULT_CLIENT_ID = 77

# How long to wait for the connect handshake, and for a request's terminal
# response, in seconds. The socket protocol has no request ids for several
# calls, so every request carries its own bounded wait.
DEFAULT_CONNECT_TIMEOUT = 10.0
DEFAULT_REQUEST_TIMEOUT = 20.0


def _get_int(name, default):
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _get_float(name, default):
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def is_paper():
    """Whether the configured account is a paper-trading account.

    ``IBKR_PAPER`` is read as truthy the same way the rest of the tree reads
    booleans (``true``/``1``/``yes``/``t``).
    """
    return (os.getenv("IBKR_PAPER") or "").strip().lower() in ("true", "1", "yes", "t")


def use_ib_gateway():
    """Whether the socket host is IB Gateway rather than full TWS.

    Only affects which *default* port is chosen; ``IBKR_PORT`` always wins.
    """
    return (os.getenv("IBKR_GATEWAY") or "").strip().lower() in ("true", "1", "yes", "t")


def get_port():
    """The socket port, honouring ``IBKR_PORT`` and then the paper/gateway defaults."""
    if (os.getenv("IBKR_PORT") or "").strip():
        return _get_int("IBKR_PORT", TWS_PORT_LIVE)

    paper = is_paper()
    if use_ib_gateway():
        return GATEWAY_PORT_PAPER if paper else GATEWAY_PORT_LIVE
    return TWS_PORT_PAPER if paper else TWS_PORT_LIVE


def get_client_id():
    """The ``clientId`` this plugin connects with."""
    return _get_int("IBKR_CLIENT_ID", DEFAULT_CLIENT_ID)


def get_account():
    """The account id to trade, or ``""`` to let IBKR pick the default.

    Only needed for a multi-account login; the gateway returns the default
    account when this is blank.
    """
    return (os.getenv("IBKR_ACCOUNT") or "").strip()


def get_host():
    return (os.getenv("IBKR_HOST") or "").strip() or DEFAULT_HOST


def get_connect_timeout():
    return _get_float("IBKR_CONNECT_TIMEOUT", DEFAULT_CONNECT_TIMEOUT)


def get_request_timeout():
    return _get_float("IBKR_REQUEST_TIMEOUT", DEFAULT_REQUEST_TIMEOUT)


def connection_descriptor():
    """The stored "token" for an IBKR login.

    IBKR has no credential to keep, so what gets stored (encrypted, like every
    other broker's token) is the connection the login proved works. It is a
    descriptive string, not a secret -- it contains no password and no key --
    and it exists so that later calls know which gateway and account the session
    was established against.
    """
    return f"{get_host()}:{get_port()}:{get_client_id()}:{get_account()}"


def parse_descriptor(token):
    """Split a stored descriptor back into connection settings.

    Returns ``(host, port, client_id, account)``. Falls back to the current
    environment for anything missing, so a token minted before a setting changed
    still resolves.
    """
    parts = (token or "").split(":")
    host = parts[0] if len(parts) > 0 and parts[0] else get_host()
    try:
        port = int(parts[1]) if len(parts) > 1 and parts[1] else get_port()
    except ValueError:
        port = get_port()
    try:
        client_id = int(parts[2]) if len(parts) > 2 and parts[2] else get_client_id()
    except ValueError:
        client_id = get_client_id()
    account = parts[3] if len(parts) > 3 else get_account()
    return host, port, client_id, account
