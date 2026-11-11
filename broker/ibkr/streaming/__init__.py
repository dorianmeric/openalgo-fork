# IBKR streaming module
#
# Deliberately no imports here.
#
# `websocket_proxy/__init__.py` imports `...streaming.ibkr_adapter` directly, and
# that module imports `websocket_proxy.base_adapter`, which imports the
# `websocket_proxy` package. If this file also imported the adapter, whichever of
# the two packages was imported first would find the other only half-initialised
# and raise ImportError.
#
# The brokers that tree imports for streaming (ibulls, iifl) keep this file empty
# or a bare comment for the same reason; angel has no `__init__.py` at all. Import
# the adapter by its module path: `broker.ibkr.streaming.ibkr_adapter`.
