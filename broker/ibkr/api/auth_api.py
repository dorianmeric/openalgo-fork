"""
IBKR "login".

There is no credential exchange here, and that is not an omission: IBKR keeps the
trading session inside TWS or IB Gateway, which the trader logs into by hand. The
API socket is only a door into that already-authenticated session. So
``authenticate_broker`` proves the door opens and the session is live, then
stores a *connection descriptor* -- host, port, client id, account -- instead of
a token, because there is no token to store.

What that means in practice, and what the setup guide tells a trader:

* TWS or IB Gateway must be running and logged in before this is called.
* "Enable ActiveX and Socket Clients" must be ticked in TWS, or the connect
  fails with error 502.
* "Read-Only API" is on by default in TWS and makes every order call fail; it
  must be turned off to trade.
* IBKR re-authenticates the desktop session itself (TWS roughly weekly, IB
  Gateway roughly daily). A failed login here usually means that session lapsed,
  not that anything in OpenAlgo is misconfigured.

Per the broker integration contract the keys are read at call time, never at
import, so a login sees its own configuration.
"""

from broker.ibkr.api import baseurl, tws_client
from utils.logging import get_logger

logger = get_logger(__name__)


def authenticate_broker(request_token=None):
    """Prove the IBKR session is reachable, and describe it for later calls.

    Args:
        request_token: Ignored. IBKR has no redirect and no code to exchange; the
            argument exists only so the name matches the contract every caller
            uses (``utils/plugin_loader`` looks up exactly ``authenticate_broker``).

    Returns:
        tuple[str | None, str | None]: ``(descriptor, None)`` on success,
        ``(None, message)`` on failure. The message is written for a trader, not
        a log file -- it names the setting to check.
    """
    host = baseurl.get_host()
    port = baseurl.get_port()
    client_id = baseurl.get_client_id()
    account = baseurl.get_account()

    if host not in ("127.0.0.1", "localhost", "::1"):
        logger.warning(
            "IBKR_HOST is %s, not loopback. The TWS socket protocol is "
            "unauthenticated and unencrypted; exposing it off-host grants full "
            "trading access to anyone who can reach the port.",
            host,
        )

    try:
        with tws_client.TwsConnection(
            host,
            port,
            client_id,
            connect_timeout=baseurl.get_connect_timeout(),
            request_timeout=baseurl.get_request_timeout(),
        ) as connection:
            # current_time is the cheapest proof the session is live: it needs no
            # market-data permission and no entitlement.
            connection.current_time()
            accounts = list(connection.managed_accounts)
            version = connection.server_version
    except tws_client.TwsConnectionError as exc:
        return None, (
            f"{exc}\n\nCheck that TWS or IB Gateway is running and logged in, that "
            f"'{host}:{port}' is the socket port configured in its API settings, "
            "and that 'Enable ActiveX and Socket Clients' is ticked."
        )
    except tws_client.TwsError as exc:
        return None, f"IBKR rejected the connection: {exc}"

    if account and accounts and account not in accounts:
        return None, (
            f"IBKR_ACCOUNT is set to {account}, but the logged-in session exposes "
            f"{', '.join(accounts)}. Correct IBKR_ACCOUNT or clear it to use the "
            "session's default account."
        )

    logger.info(
        "IBKR session is live: %s:%s clientId=%s apiVersion=%s accounts=%s",
        host,
        port,
        client_id,
        version,
        ",".join(accounts) or "(none reported)",
    )
    return baseurl.connection_descriptor(), None


def get_account(auth_token):
    """The account to trade, from a stored descriptor."""
    _host, _port, _client_id, account = baseurl.parse_descriptor(auth_token)
    return account


def get_connection(auth_token):
    """Open a connection described by a stored token.

    Used by the other modules in this plugin. Returns a connected
    ``TwsConnection`` that the caller owns and must close (or use as a context
    manager).
    """
    host, port, client_id, _account = baseurl.parse_descriptor(auth_token)
    return tws_client.TwsConnection(
        host,
        port,
        client_id,
        connect_timeout=baseurl.get_connect_timeout(),
        request_timeout=baseurl.get_request_timeout(),
    ).connect()


def verify_session(auth_token):
    """Whether a stored connection still answers. Never raises."""
    try:
        with get_connection(auth_token) as connection:
            connection.current_time()
        return True
    except tws_client.TwsError:
        return False
