"""Tests for the coordinator (GizwitsDataUpdateCoordinator)."""

import time
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

from custom_components.jebao_aqua.api import GizwitsApi, TokenExpiredError
from custom_components.jebao_aqua.const import (
    DOMAIN,
    GIZWITS_API_URLS,
    TOKEN_REFRESH_MARGIN,
    MAX_LAN_FAILURES,
)

from conftest import (
    SAMPLE_DEVICES_RESPONSE,
    SAMPLE_DEVICE_DATA,
    SAMPLE_LOGIN_RESPONSE_SUCCESS,
    SAMPLE_REFRESH_RESPONSE_SUCCESS,
    CONFIG_ENTRY_DATA_CLOUD,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_hass():
    """Create a minimal mock HomeAssistant."""
    hass = MagicMock()
    hass.data = {DOMAIN: {}}
    hass.config_entries = MagicMock()
    return hass


def _make_mock_entry(data=None):
    """Create a mock ConfigEntry."""
    entry = MagicMock()
    entry.entry_id = "test_entry_id"
    entry.data = dict(data or CONFIG_ENTRY_DATA_CLOUD)
    return entry


def _make_mock_api():
    """Create a mock GizwitsApi."""
    api = AsyncMock(spec=GizwitsApi)
    api._token = "test_token"
    api.get_devices = AsyncMock(return_value=SAMPLE_DEVICES_RESPONSE)
    api.get_device_data = AsyncMock(return_value=SAMPLE_DEVICE_DATA)
    api.get_local_device_data = AsyncMock(return_value=None)
    api.async_refresh_token = AsyncMock(return_value={
        "token": "new_token_from_refresh",
        "expires_at": int(time.time()) + 180 * 86400,
        "error": None,
    })
    api.async_login = AsyncMock(return_value={
        "token": "new_token_from_login",
        "refresh_token": "new_refresh_from_login",
        "expires_at": int(time.time()) + 180 * 86400,
        "error": None,
    })
    return api


def _make_coordinator(hass=None, api=None, entry=None, lan_only=False):
    """Create a GizwitsDataUpdateCoordinator with mocks."""
    from custom_components.jebao_aqua import GizwitsDataUpdateCoordinator
    hass = hass or _make_mock_hass()
    api = api or _make_mock_api()
    entry = entry or _make_mock_entry()
    coord = GizwitsDataUpdateCoordinator(hass, api, entry=entry, lan_only=lan_only)
    return coord


# ---------------------------------------------------------------------------
# fetch_initial_device_list
# ---------------------------------------------------------------------------

class TestFetchInitialDeviceList:
    async def test_populates_device_inventory(self):
        coord = _make_coordinator()
        entry = _make_mock_entry()

        await coord.fetch_initial_device_list(entry)

        assert len(coord.device_inventory) == 2
        assert coord.device_inventory[0]["did"] == "test_device_001"

    async def test_merges_lan_ips_from_config(self):
        coord = _make_coordinator()
        entry = _make_mock_entry(data=CONFIG_ENTRY_DATA_CLOUD)

        await coord.fetch_initial_device_list(entry)

        # test_device_001 should get its LAN IP from config entry
        dev_001 = next(d for d in coord.device_inventory if d["did"] == "test_device_001")
        assert dev_001["lan_ip"] == "192.168.1.100"

    async def test_token_expired_triggers_recovery(self):
        api = _make_mock_api()
        api.get_devices = AsyncMock(side_effect=[
            TokenExpiredError("Token invalid"),
            SAMPLE_DEVICES_RESPONSE,
        ])
        coord = _make_coordinator(api=api)
        entry = _make_mock_entry()

        await coord.fetch_initial_device_list(entry)

        assert len(coord.device_inventory) == 2

    async def test_empty_response(self):
        api = _make_mock_api()
        api.get_devices = AsyncMock(return_value={"other": "data"})
        coord = _make_coordinator(api=api)
        entry = _make_mock_entry()

        await coord.fetch_initial_device_list(entry)

        assert coord.device_inventory == []


# ---------------------------------------------------------------------------
# _maybe_refresh_token
# ---------------------------------------------------------------------------

class TestMaybeRefreshToken:
    async def test_skips_when_lan_only(self):
        coord = _make_coordinator(lan_only=True)
        result = await coord._maybe_refresh_token()
        assert result is False

    async def test_skips_when_no_entry(self):
        coord = _make_coordinator()
        coord._entry = None
        result = await coord._maybe_refresh_token()
        assert result is False

    async def test_skips_when_token_not_near_expiry(self):
        entry = _make_mock_entry(data={
            **CONFIG_ENTRY_DATA_CLOUD,
            "token_expires_at": time.time() + 90 * 86400,  # 90 days out
        })
        coord = _make_coordinator(entry=entry)
        result = await coord._maybe_refresh_token()
        assert result is False

    async def test_refreshes_when_near_expiry(self):
        entry = _make_mock_entry(data={
            **CONFIG_ENTRY_DATA_CLOUD,
            "token_expires_at": time.time() + 3 * 86400,  # 3 days out (< 7 day margin)
        })
        coord = _make_coordinator(entry=entry)
        result = await coord._maybe_refresh_token()
        assert result is True
        coord.api.async_refresh_token.assert_called_once()

    async def test_force_refresh_with_refresh_token(self):
        coord = _make_coordinator()
        result = await coord._maybe_refresh_token(force=True)
        assert result is True
        coord.api.async_refresh_token.assert_called_once_with("test_refresh_456")

    async def test_force_fallback_to_login(self):
        api = _make_mock_api()
        api.async_refresh_token = AsyncMock(return_value={
            "token": None, "expires_at": None, "error": "refresh_failed"
        })
        entry = _make_mock_entry(data={
            **CONFIG_ENTRY_DATA_CLOUD,
            "refresh_token": None,  # No refresh token available
        })
        coord = _make_coordinator(api=api, entry=entry)

        result = await coord._maybe_refresh_token(force=True)

        assert result is True
        api.async_login.assert_called_once_with("test@example.com", "testpass")

    async def test_force_fails_without_credentials(self):
        api = _make_mock_api()
        entry = _make_mock_entry(data={
            "token": "tok",
            "region": "eu",
            "refresh_token": None,
            # No email/password
        })
        coord = _make_coordinator(api=api, entry=entry)

        result = await coord._maybe_refresh_token(force=True)

        assert result is False

    async def test_updates_config_entry_on_refresh(self):
        hass = _make_mock_hass()
        entry = _make_mock_entry(data={
            **CONFIG_ENTRY_DATA_CLOUD,
            "token_expires_at": time.time() + 2 * 86400,
        })
        coord = _make_coordinator(hass=hass, entry=entry)

        await coord._maybe_refresh_token()

        hass.config_entries.async_update_entry.assert_called_once()
        call_kwargs = hass.config_entries.async_update_entry.call_args
        new_data = call_kwargs[1]["data"] if "data" in call_kwargs[1] else call_kwargs[0][1]
        assert new_data["token"] == "new_token_from_refresh"
        assert new_data["refresh_token"] is None  # Consumed


# ---------------------------------------------------------------------------
# get_device_data (coordinator level)
# ---------------------------------------------------------------------------

class TestCoordinatorGetDeviceData:
    async def test_cloud_only_device(self):
        coord = _make_coordinator()
        coord.device_inventory = [
            {"did": "dev1", "lan_ip": None, "product_key": "pk1"},
        ]

        data = await coord.get_device_data("dev1")

        assert data == SAMPLE_DEVICE_DATA
        coord.api.get_device_data.assert_called_once_with("dev1")

    async def test_lan_device_success(self):
        api = _make_mock_api()
        api.get_local_device_data = AsyncMock(return_value=SAMPLE_DEVICE_DATA)
        coord = _make_coordinator(api=api)
        coord.device_inventory = [
            {"did": "dev1", "lan_ip": "192.168.1.10", "product_key": "pk1"},
        ]

        data = await coord.get_device_data("dev1")

        assert data == SAMPLE_DEVICE_DATA
        api.get_local_device_data.assert_called_once()
        # Cloud should NOT have been called
        api.get_device_data.assert_not_called()

    async def test_lan_failure_falls_back_to_cloud(self):
        api = _make_mock_api()
        api.get_local_device_data = AsyncMock(return_value=None)
        coord = _make_coordinator(api=api)
        coord.device_inventory = [
            {"did": "dev1", "lan_ip": "192.168.1.10", "product_key": "pk1"},
        ]

        data = await coord.get_device_data("dev1")

        assert data == SAMPLE_DEVICE_DATA
        api.get_local_device_data.assert_called_once()
        api.get_device_data.assert_called_once_with("dev1")

    async def test_lan_failure_counter_increments(self):
        api = _make_mock_api()
        api.get_local_device_data = AsyncMock(return_value=None)
        coord = _make_coordinator(api=api)
        coord.device_inventory = [
            {"did": "dev1", "lan_ip": "192.168.1.10", "product_key": "pk1"},
        ]

        await coord.get_device_data("dev1")
        assert coord._lan_failure_counts["dev1"] == 1

        await coord.get_device_data("dev1")
        assert coord._lan_failure_counts["dev1"] == 2

    async def test_lan_backoff_after_max_failures(self):
        api = _make_mock_api()
        api.get_local_device_data = AsyncMock(return_value=None)
        coord = _make_coordinator(api=api)
        coord.device_inventory = [
            {"did": "dev1", "lan_ip": "192.168.1.10", "product_key": "pk1"},
        ]
        coord._lan_failure_counts["dev1"] = MAX_LAN_FAILURES

        await coord.get_device_data("dev1")

        # Should skip LAN and go straight to cloud
        api.get_local_device_data.assert_not_called()
        api.get_device_data.assert_called_once()

    async def test_lan_only_no_cloud_fallback(self):
        api = _make_mock_api()
        api.get_local_device_data = AsyncMock(return_value=None)
        coord = _make_coordinator(api=api, lan_only=True)
        coord.device_inventory = [
            {"did": "dev1", "lan_ip": "192.168.1.10", "product_key": "pk1"},
        ]

        data = await coord.get_device_data("dev1")

        assert data is None
        api.get_device_data.assert_not_called()  # Cloud not used


# ---------------------------------------------------------------------------
# _async_update_data / _do_update_devices
# ---------------------------------------------------------------------------

class TestAsyncUpdateData:
    async def test_update_returns_device_data(self):
        coord = _make_coordinator()
        coord.device_inventory = [
            {"did": "dev1", "lan_ip": None, "product_key": "pk1"},
        ]

        result = await coord._do_update_devices()

        assert "dev1" in result
        assert result["dev1"]["attr"]["switch"] is True

    async def test_preserves_cached_data_on_failure(self):
        api = _make_mock_api()
        api.get_device_data = AsyncMock(return_value=None)
        coord = _make_coordinator(api=api)
        coord.device_inventory = [
            {"did": "dev1", "lan_ip": None, "product_key": "pk1"},
        ]
        coord.device_data = {"dev1": SAMPLE_DEVICE_DATA}

        result = await coord._do_update_devices()

        # Should preserve cached data
        assert "dev1" in result
        assert result["dev1"] == SAMPLE_DEVICE_DATA

    async def test_token_expired_triggers_retry(self):
        api = _make_mock_api()
        call_count = 0

        async def side_effect(device_id):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise TokenExpiredError("expired")
            return SAMPLE_DEVICE_DATA

        api.get_device_data = side_effect
        api.get_local_device_data = AsyncMock(return_value=None)

        coord = _make_coordinator(api=api)
        coord.device_inventory = [
            {"did": "dev1", "lan_ip": None, "product_key": "pk1"},
        ]

        result = await coord._async_update_data()

        assert "dev1" in result
