"""
Margin requirement calculation for IBKR.

**Not implemented, deliberately.**

The correct IBKR mechanism is a *what-if* order: ``placeOrder`` with ``whatIf=1``
makes the gateway price the order without routing it, and return the margin
impact in the order-state block of the ``OPEN_ORDER`` frame. That block is not
enumerated in the reference this plugin was built from, so the frame cannot be
decoded reliably -- and guessing a positional frame does not fail cleanly, it
misreads every field after the mistake.

The alternative would be to report an account-level margin figure as though it
were this order's requirement, which is a different number. This plugin is used
to size real positions, so it refuses rather than mislead: the caller gets an
explicit unsupported response, and the capability gate surfaces it as such.

To implement this properly:

1. Encode ``placeOrder`` with ``order.whatIf = True`` (see
   ``tws_client.build_place_order_frame`` -- the field already exists and obeys
   its version gate).
2. Decode the ``orderState`` block of the returned ``OPEN_ORDER`` frame
   (``initMarginChange`` and ``maintMarginChange``), pinning its layout against
   a recorded frame from a real session first.
"""

from utils.logging import get_logger

logger = get_logger(__name__)

_UNSUPPORTED = (
    "IBKR margin calculation is not available: it requires the what-if order path, "
    "whose order-state response block this plugin does not decode. OpenAlgo will "
    "size the order without a broker margin figure."
)


class _Response:
    """A response shim so the caller sees a normal status code."""

    def __init__(self, status_code):
        self.status_code = status_code
        self.status = status_code


def calculate_margin_api(positions, auth, api_key=None):
    """Return an explicit unsupported result.

    Returns:
        tuple: ``(response, data)`` with ``response.status == 501``.
    """
    logger.info("IBKR margin calculation requested but not supported: %s", _UNSUPPORTED)
    return _Response(501), {"status": "error", "message": _UNSUPPORTED}
