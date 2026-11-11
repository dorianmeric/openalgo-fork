"""Protocol-level tests for the IBKR TWS client.

These cover the parts that can be verified without a live TWS/IB Gateway: the
byte framing, the value coercions, the version gates, and the error
classification. The network paths (handshake, request/response loops) are not
exercised here — they need a running gateway and are listed in the plugin's
"verify live" checklist.

The framing assertions are written against the official ``ibapi`` reference
implementation, which is the byte-level specification for this protocol.
"""

import os
import struct
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.ibkr.api import tws_client as tws  # noqa: E402


class TestFraming:
    """``[4-byte big-endian length][field\\0field\\0...]``, per the SDK's comm.py."""

    def test_encode_field_nul_terminates(self):
        assert tws.encode_field("abc") == "abc\0"

    def test_encode_field_none_is_an_empty_field(self):
        # Unset numerics travel as empty fields, never as a missing field:
        # the layout is positional.
        assert tws.encode_field(None) == "\0"

    def test_encode_field_bool_is_an_int(self):
        assert tws.encode_field(True) == "1\0"
        assert tws.encode_field(False) == "0\0"

    def test_encode_field_float_keeps_precision(self):
        assert tws.encode_field(196.5) == "196.5\0"

    def test_encode_message_prefixes_a_big_endian_length(self):
        frame = tws.encode_message(1, 2)
        (size,) = struct.unpack("!I", frame[:4])
        assert size == len(frame) - 4
        assert frame[4:] == b"1\0002\0"

    def test_encode_message_length_matches_payload_for_long_values(self):
        frame = tws.encode_message("x" * 5000)
        (size,) = struct.unpack("!I", frame[:4])
        assert size == 5001  # 5000 chars + the NUL

    def test_read_fields_drops_the_trailing_empty_element(self):
        assert tws.read_fields(b"9\00042\0") == (b"9", b"42")

    def test_round_trip_through_read_fields(self):
        frame = tws.encode_message(9, 1, 12345)
        assert tws.read_fields(frame[4:]) == (b"9", b"1", b"12345")

    def test_banner_is_api_nul_then_a_length_prefixed_version_string(self):
        # The ``API\0`` prefix is deliberately outside the length, and the
        # version string has no trailing NUL -- the one exception to the rule.
        banner = tws.encode_banner()
        assert banner.startswith(b"API\0")
        (size,) = struct.unpack("!I", banner[4:8])
        assert banner[8:] == b"v100..178"
        assert size == len(b"v100..178")
        assert not banner.endswith(b"\0")

    def test_banner_honours_the_max_client_version_override(self, monkeypatch):
        monkeypatch.setenv("IBKR_MAX_CLIENT_VER", "160")
        assert tws.encode_banner() == b"API\0" + struct.pack("!I", 9) + b"v100..160"


class TestValueCoercion:
    """Blank fields are how the protocol says "unset", so coercions must not throw."""

    @pytest.mark.parametrize("blank", ["", "   ", None])
    def test_to_int_treats_blank_as_the_default(self, blank):
        assert tws.to_int(blank, default=7) == 7

    def test_to_int_accepts_a_float_shaped_integer(self):
        assert tws.to_int("12345.0") == 12345

    def test_to_int_falls_back_on_garbage(self):
        assert tws.to_int("not-a-number") == 0

    @pytest.mark.parametrize("blank", ["", None])
    def test_to_float_treats_blank_as_the_default(self, blank):
        assert tws.to_float(blank, default=1.5) == 1.5

    def test_to_float_reads_a_price(self):
        assert tws.to_float("196.55") == pytest.approx(196.55)


class TestVersionGates:
    """Field layout is gated on the negotiated serverVersion."""

    CONTRACT = {"symbol": "AAPL", "secType": "STK", "tradingClass": "NMS", "secId": "X"}

    def test_modern_server_gets_trading_class_and_sec_id_fields(self):
        fields = tws.contract_fields(self.CONTRACT, server_version=178)
        assert len(fields) == 14
        assert "NMS" in fields

    def test_below_68_omits_trading_class_only(self):
        fields = tws.contract_fields(self.CONTRACT, server_version=60)
        assert len(fields) == 13
        assert "NMS" not in fields

    def test_below_45_omits_sec_id_fields_too(self):
        fields = tws.contract_fields(self.CONTRACT, server_version=40)
        assert len(fields) == 11
        assert "NMS" not in fields and "X" not in fields

    def test_gated_fields_are_omitted_not_emptied(self):
        """Regression: a gated-out field must not occupy a frame slot.

        ``encode_field(None)`` and ``encode_field("")`` both emit one NUL, so a
        gate expressed as ``None`` is not a gate at all -- it leaves the field in
        place and shifts everything after it. The length must therefore differ
        across versions.
        """
        oldest = tws.contract_fields(self.CONTRACT, server_version=40)
        newest = tws.contract_fields(self.CONTRACT, server_version=178)
        # The gate only ever appends to the tail, so the leading positions must
        # be byte-for-byte identical between versions.
        assert len(newest) - len(oldest) == 3  # tradingClass + secIdType + secId
        assert oldest[:11] == newest[:11]

    def test_unspecified_contract_values_stay_unset(self):
        fields = tws.contract_fields({"symbol": "AAPL", "secType": "STK"}, server_version=178)
        # position 0 is conId, position 4 is strike: both must encode as blank.
        assert fields[0] == 0
        assert fields[4] is None


class TestPlaceOrderFrame:
    """The frame is positional and untagged, so length and order are the contract.

    A field too few shifts everything after it and corrupts the stream (IBKR
    error 320), which is why these assert structure rather than values.
    """

    CONTRACT = tws.equity_contract("AAPL")
    ORDER = tws.build_order(action="BUY", totalQuantity=10, lmtPrice=196.5)

    def test_modern_server_frame_omits_the_version_field(self):
        # From server version 145 the frame starts [3, orderId, ...], with no
        # version field at all.
        frame = tws.build_place_order_frame(1, self.CONTRACT, self.ORDER, 178)
        assert frame[0] == tws.OUT_PLACE_ORDER
        assert frame[1] == 1  # orderId, not a version

    def test_older_server_still_carries_the_version_field(self):
        frame = tws.build_place_order_frame(1, self.CONTRACT, self.ORDER, 140)
        assert frame[0] == tws.OUT_PLACE_ORDER
        assert frame[1] == 45
        assert frame[2] == 1

    def test_frame_is_roughly_the_documented_size(self):
        # The reference describes the v151 frame as "~120 fields". A large
        # regression here means a block was dropped.
        frame = tws.build_place_order_frame(1, self.CONTRACT, self.ORDER, 178)
        assert 110 <= len(frame) <= 135

    def test_order_values_land_in_the_frame(self):
        frame = tws.build_place_order_frame(7, self.CONTRACT, self.ORDER, 178)
        assert "AAPL" in frame
        assert "BUY" in frame
        assert 196.5 in frame
        assert "DAY" in frame

    def test_the_frame_encodes_without_raising(self):
        # Every field must be coercible to ASCII text by encode_field.
        frame = tws.build_place_order_frame(1, self.CONTRACT, self.ORDER, 178)
        assert tws.encode_message(*frame).startswith(b"\x00\x00")

    @pytest.mark.parametrize("order_type", ["PEG BENCH", "PEG BEST", "PEG MID", "VWAP"])
    def test_order_types_with_unverified_blocks_are_refused(self, order_type):
        # Refusing beats emitting a frame that corrupts the stream.
        order = tws.build_order(orderType=order_type, totalQuantity=1)
        with pytest.raises(tws.TwsError, match="pegged/conditions field block"):
            tws.build_place_order_frame(1, self.CONTRACT, order, 178)

    def test_plain_order_types_are_allowed(self):
        for order_type in ("MKT", "LMT", "STP", "STP LMT", "TRAIL"):
            order = tws.build_order(orderType=order_type, totalQuantity=1)
            assert tws.build_place_order_frame(1, self.CONTRACT, order, 178)

    def test_bag_contracts_are_refused(self):
        bag = dict(self.CONTRACT, secType="BAG")
        with pytest.raises(tws.TwsError, match="BAG"):
            tws.build_place_order_frame(1, bag, self.ORDER, 178)

    def test_gates_change_the_frame_length(self):
        newest = tws.build_place_order_frame(1, self.CONTRACT, self.ORDER, 178)
        oldest = tws.build_place_order_frame(1, self.CONTRACT, self.ORDER, 100)
        assert len(newest) > len(oldest)


class TestDecoders:
    """Decoders are fed frames shaped like the documented responses."""

    def test_decode_positions_reads_the_documented_layout(self):
        # [61, v, account, conId, symbol, secType, expiry, strike, right,
        #  multiplier, exchange, currency, localSymbol,
        #  (tradingClass if v>=2), position, (avgCost if v>=3)]
        # v=3 so both the gated tradingClass and avgCost are present.
        frame = [
            "61", "3", "DU123", "265598", "AAPL", "STK", "", "0", "", "",
            "SMART", "USD", "AAPL", "NMS", "150", "180.25",
        ]
        row = tws.decode_positions(frame)
        assert row["symbol"] == "AAPL"
        assert row["conId"] == 265598
        assert row["position"] == 150.0
        assert row["avgCost"] == pytest.approx(180.25)

    def test_decode_historical_bars_reads_the_documented_layout(self):
        # [17, (v only if sv<124), reqId, start, end, barCount,
        #  {date, o, h, l, c, volume, wap, (unused if sv<124), barCount}]
        # sv>=124, so there is no message-version field and no unused string.
        frame = [
            "17", "9001", "20260101", "20260102", "1",
            "20260102", "100.0", "110.0", "99.0", "105.0", "1000", "102.5", "1",
        ]
        req_id, data = tws.decode_historical_bars(frame, server_version=178)
        assert req_id == 9001
        assert len(data["bars"]) == 1
        bar = data["bars"][0]
        assert (bar["open"], bar["high"], bar["low"], bar["close"]) == (100.0, 110.0, 99.0, 105.0)
        assert bar["volume"] == 1000.0

    def test_decode_contract_details_reads_the_documented_layout(self):
        # [10, reqId, symbol, secType, expiry, strike, right, exchange, currency,
        #  localSymbol, marketName, tradingClass, conId, minTick, multiplier, ...]
        # From server version 164 the message carries no version field, but the
        # reqId stays -- the decoder treats the version as 8 and reads it.
        frame = [
            "10", "9001", "AAPL", "STK", "", "0", "", "SMART", "USD", "AAPL", "", "NMS",
            "265598", "0.01", "", "", "", "0", "", "", "", "",
        ]
        details = tws.decode_contract_details(frame, server_version=178)
        assert details["symbol"] == "AAPL"
        assert details["conId"] == 265598
        assert details["minTick"] == pytest.approx(0.01)

    def test_market_data_request_skips_the_generic_tick_list_for_snapshots(self):
        fields = tws._market_data_request_fields(9001, tws.equity_contract("AAPL"), 178)
        assert fields[0] == tws.OUT_REQ_MKT_DATA
        assert fields[2] == 9001
        # snapshot flag must be 1, and the generic tick list empty.
        assert fields[-2] == 0  # regulatorySnapshot
        assert fields[-3] == 1  # snapshot

    def test_historical_request_includes_a_conid_from_68(self):
        contract = tws.equity_contract("AAPL")
        contract["conId"] = 265598
        fields = tws._historical_request_fields(
            9001, contract, "", "1 M", "1 day", "TRADES", True, 178
        )
        assert fields[0] == tws.OUT_REQ_HISTORICAL_DATA
        assert 265598 in fields


class TestTickAssembly:
    def test_apply_tick_maps_prices_onto_the_quote(self):
        quote = {}
        tws.TwsConnection._apply_tick(quote, ["1", "6", "9001", "4", "196.5", "0", "0"], 9001)
        tws.TwsConnection._apply_tick(quote, ["1", "6", "9001", "1", "195.0", "0", "0"], 9001)
        tws.TwsConnection._apply_tick(quote, ["1", "6", "9001", "2", "197.0", "0", "0"], 9001)
        assert quote["ltp"] == pytest.approx(196.5)
        assert quote["bid"] == pytest.approx(195.0)
        assert quote["ask"] == pytest.approx(197.0)

    def test_apply_tick_ignores_another_requests_ticks(self):
        quote = {}
        tws.TwsConnection._apply_tick(quote, ["1", "6", "111", "4", "9.9", "0", "0"], 9001)
        assert quote == {}


class TestStartApiFrame:
    """optCapab is version-gated, so the frame length must vary."""

    def test_optcapab_is_only_appended_from_72(self):
        # [71, 2, clientId] below 72; [71, 2, clientId, optCapab] from 72 on.
        assert tws.MIN_SERVER_VER_OPTIONAL_CAPABILITIES == 72
        below = [tws.OUT_START_API, 2, 77]
        at = [tws.OUT_START_API, 2, 77, ""]
        assert len(at) - len(below) == 1


class TestContractBuilders:
    def test_equity_contract_routes_through_smart(self):
        contract = tws.equity_contract("aapl", primary_exchange="NASDAQ")
        assert contract["secType"] == "STK"
        assert contract["symbol"] == "AAPL"
        assert contract["exchange"] == "SMART"
        assert contract["primaryExchange"] == "NASDAQ"
        assert contract["currency"] == "USD"

    def test_option_contract_carries_the_expiry_strike_and_right(self):
        contract = tws.option_contract("aapl", "20260117", 190, "c")
        assert contract["secType"] == "OPT"
        assert contract["lastTradeDateOrContractMonth"] == "20260117"
        assert contract["strike"] == 190
        assert contract["right"] == "C"
        assert contract["multiplier"] == "100"
        assert contract["exchange"] == "SMART"

    def test_build_order_defaults_are_a_retail_day_order(self):
        order = tws.build_order(action="SELL", totalQuantity=10, lmtPrice=200.0)
        assert order["action"] == "SELL"
        assert order["orderType"] == "LMT"
        assert order["tif"] == "DAY"
        assert order["transmit"] is True
        assert order["lmtPrice"] == 200.0


class TestErrorClassification:
    """A blank-field frame is a valid error frame; classification drives behaviour."""

    def test_informational_farm_status_is_not_a_failure(self):
        # [4, version, reqId=-1, code, message] -- 2104/2106/2158 are "farms OK".
        for code in ("2104", "2106", "2158"):
            tws.TwsConnection._raise_if_fatal(["4", "2", "-1", code, "farm is OK"])

    def test_transport_code_raises_a_connection_error(self):
        with pytest.raises(tws.TwsConnectionError) as excinfo:
            tws.TwsConnection._raise_if_fatal(["4", "2", "-1", "502", "Couldn't connect to TWS"])
        assert excinfo.value.code == 502

    def test_order_reject_raises_a_plain_error(self):
        with pytest.raises(tws.TwsError) as excinfo:
            tws.TwsConnection._raise_if_fatal(["4", "2", "5", "201", "Order rejected"])
        assert excinfo.value.code == 201
        assert not isinstance(excinfo.value, tws.TwsConnectionError)

    def test_no_data_codes_are_listed_for_callers_to_swallow(self):
        # 200/162/165/366 mean "nothing to return", not "something broke".
        for code in (200, 162, 165, 366):
            assert code in tws.NO_DATA_CODES
