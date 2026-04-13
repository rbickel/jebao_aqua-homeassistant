"""Tests for the discovery module."""

import asyncio
import pytest
from unittest.mock import MagicMock, patch, AsyncMock

from custom_components.jebao_aqua.discovery import (
    DiscoveryProtocol,
    discover_devices,
    BROADCAST_PORT,
    BROADCAST_PAYLOAD,
)


class TestDiscoveryProtocol:
    def test_datagram_received_valid(self):
        protocol = DiscoveryProtocol()
        # Build a fake 32+ byte packet with a device ID at offset 10
        device_id = "ABC123DEF456GHIJ789012"
        data = b"\x00" * 10 + device_id.encode() + b"\x00" * 10
        addr = ("192.168.1.50", 12414)

        protocol.datagram_received(data, addr)

        assert "ABC123DEF456GHIJ789012" in protocol.results
        assert protocol.results["ABC123DEF456GHIJ789012"] == "192.168.1.50"

    def test_datagram_received_too_short(self):
        protocol = DiscoveryProtocol()
        data = b"\x00" * 10  # Too short (< 32 bytes)
        addr = ("192.168.1.50", 12414)

        protocol.datagram_received(data, addr)

        assert len(protocol.results) == 0

    def test_connection_made(self):
        protocol = DiscoveryProtocol()
        transport = MagicMock()

        protocol.connection_made(transport)

        assert protocol.transport is transport

    def test_multiple_devices(self):
        protocol = DiscoveryProtocol()

        dev1_id = "DEVICE_1_ID_ABCDEFGHIJ"
        data1 = b"\x00" * 10 + dev1_id.encode() + b"\x00" * 10
        protocol.datagram_received(data1, ("192.168.1.50", 12414))

        dev2_id = "DEVICE_2_ID_KLMNOPQRST"
        data2 = b"\x00" * 10 + dev2_id.encode() + b"\x00" * 10
        protocol.datagram_received(data2, ("192.168.1.51", 12414))

        assert len(protocol.results) == 2
        assert protocol.results[dev1_id] == "192.168.1.50"
        assert protocol.results[dev2_id] == "192.168.1.51"


class TestDiscoverDevices:
    async def test_returns_empty_on_error(self):
        with patch("custom_components.jebao_aqua.discovery.asyncio.get_event_loop", side_effect=Exception("fail")):
            result = await discover_devices()
            assert result == {}
