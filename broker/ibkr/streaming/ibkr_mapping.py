"""
Exchange and capability registries for the IBKR streaming adapter.

IBKR speaks one vocabulary: a contract on an exchange, with ``SMART`` routing.
There is no per-exchange market-data code to translate the way the Indian brokers
need, so this file is small on purpose -- its job is to declare what this plugin
can stream, which is what the capability checks read.
"""

from utils.logging import get_logger

logger = get_logger(__name__)


class IbkrExchangeMapper:
    """Maps OpenAlgo exchanges onto IBKR's."""

    #: OpenAlgo exchange -> IBKR exchange.
    EXCHANGE_MAP = {
        "US": "SMART",
        "US-OPT": "SMART",
    }

    @classmethod
    def to_ibkr(cls, exchange):
        """An OpenAlgo exchange as IBKR spells it."""
        return cls.EXCHANGE_MAP.get((exchange or "").upper(), "SMART")

    @classmethod
    def to_openalgo(cls, ibkr_exchange):
        """An IBKR exchange as an OpenAlgo exchange."""
        return "US-OPT" if (ibkr_exchange or "").upper() in ("OPT",) else "US"


class IbkrCapabilityRegistry:
    """What this adapter can stream.

    Streaming modes follow OpenAlgo's numbering: 1 = LTP, 2 = quote, 3 = depth.
    Depth is not supported -- see ``api/data.py`` on ``reqMktDepth``.
    """

    SUPPORTED_EXCHANGES = {"US", "US-OPT"}
    SUPPORTED_MODES = {1, 2}
    MAX_SYMBOLS_PER_CONNECTION = 100  # IBKR's default market-data line limit

    @classmethod
    def supports(cls, exchange, mode):
        return (exchange or "").upper() in cls.SUPPORTED_EXCHANGES and mode in cls.SUPPORTED_MODES

    @classmethod
    def get_supported_exchanges(cls):
        return set(cls.SUPPORTED_EXCHANGES)
