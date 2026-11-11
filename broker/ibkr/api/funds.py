"""
Account funds for IBKR, in OpenAlgo's margin shape.

IBKR reports balances as account-summary tags rather than one balance object, so
the tags are requested explicitly and summed across accounts (a session can
expose more than one).

Two mappings are approximations worth naming, because they are not the same
concept:

* ``collateral`` has no IBKR equivalent -- IBKR does not have the Indian
  pledge/collateral mechanism -- so it reports ``GrossPositionValue``, the value
  of what is held.
* ``utiliseddebits`` reports ``InitMarginReq``, the initial margin the account is
  currently required to hold.
"""

from broker.ibkr.api import auth_api, tws_client
from utils.logging import get_logger

logger = get_logger(__name__)

DEFAULT_MARGIN_RESPONSE = {
    "availablecash": "0.00",
    "collateral": "0.00",
    "m2mrealized": "0.00",
    "m2munrealized": "0.00",
    "utiliseddebits": "0.00",
}

#: The account-summary tags this plugin needs. Requesting a tag IBKR does not
#: recognise fails the whole call, so the list is kept to well-known tags.
SUMMARY_TAGS = (
    "TotalCashValue,AvailableFunds,BuyingPower,GrossPositionValue,"
    "InitMarginReq,MaintMarginReq,UnrealizedPnL,RealizedPnL,NetLiquidation"
)


def _f(value):
    """A tag value as float; blanks and junk become zero."""
    try:
        if value in (None, ""):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def get_margin_data(auth_token):
    """Funds and P&L for the logged-in IBKR account."""
    try:
        with auth_api.get_connection(auth_token) as connection:
            rows = connection.account_summary(
                tags=SUMMARY_TAGS, group="All", account=auth_api.get_account(auth_token)
            )
    except tws_client.TwsError:
        logger.exception("Could not read IBKR account summary")
        return dict(DEFAULT_MARGIN_RESPONSE)

    totals = {}
    for row in rows:
        tag = row.get("tag")
        if not tag:
            continue
        totals[tag] = totals.get(tag, 0.0) + _f(row.get("value"))

    if not totals:
        logger.warning("IBKR returned no account summary rows")
        return dict(DEFAULT_MARGIN_RESPONSE)

    return {
        "availablecash": f"{totals.get('AvailableFunds', totals.get('TotalCashValue', 0.0)):.2f}",
        "collateral": f"{totals.get('GrossPositionValue', 0.0):.2f}",
        "m2mrealized": f"{totals.get('RealizedPnL', 0.0):.2f}",
        "m2munrealized": f"{totals.get('UnrealizedPnL', 0.0):.2f}",
        "utiliseddebits": f"{totals.get('InitMarginReq', 0.0):.2f}",
    }
