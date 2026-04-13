"""Tests for the GizwitsApi class."""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from aiohttp import ClientResponseError

from custom_components.jebao_aqua.api import GizwitsApi, TokenExpiredError
from custom_components.jebao_aqua.const import GIZWITS_APP_ID, GIZWITS_API_URLS

from conftest import (
    SAMPLE_LOGIN_RESPONSE_SUCCESS,
    SAMPLE_LOGIN_RESPONSE_BAD_PASSWORD,
    SAMPLE_LOGIN_RESPONSE_USER_NOT_FOUND,
    SAMPLE_REFRESH_RESPONSE_SUCCESS,
    SAMPLE_TOKEN_INVALID_RESPONSE,
    SAMPLE_DEVICES_RESPONSE,
    SAMPLE_DEVICE_DATA,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_response(status, body_dict):
    """Create a mock aiohttp response."""
    resp = AsyncMock()
    resp.status = status
    resp.text = AsyncMock(return_value=json.dumps(body_dict))
    resp.headers = {}
    return resp


def _make_api(token="test_token"):
    """Create a GizwitsApi with mock session."""
    urls = GIZWITS_API_URLS["eu"]
    api = GizwitsApi(
        login_url=urls["LOGIN_URL"],
        devices_url=urls["DEVICES_URL"],
        device_data_url=urls["DEVICE_DATA_URL"],
        control_url=urls["CONTROL_URL"],
        token=token,
        refresh_token_url=urls["REFRESH_TOKEN_URL"],
    )
    api._session = MagicMock()
    api._session.closed = False
    return api


# ---------------------------------------------------------------------------
# Login tests
# ---------------------------------------------------------------------------

class TestAsyncLogin:
    """Tests for async_login."""

    async def test_login_success(self):
        api = _make_api()
        resp = _make_response(200, SAMPLE_LOGIN_RESPONSE_SUCCESS)
        api._session.post = MagicMock(return_value=AsyncContextManager(resp))

        result = await api.async_login("user@test.com", "password123")

        assert result["token"] == "test_user_token_abc123"
        assert result["refresh_token"] == "test_refresh_token_xyz789"
        assert result["expires_at"] == 1785552000
        assert result["error"] is None

    async def test_login_bad_password(self):
        api = _make_api()
        resp = _make_response(200, SAMPLE_LOGIN_RESPONSE_BAD_PASSWORD)
        api._session.post = MagicMock(return_value=AsyncContextManager(resp))

        result = await api.async_login("user@test.com", "wrong")

        assert result["token"] is None
        assert result["error"] == "invalid_password"

    async def test_login_user_not_found(self):
        api = _make_api()
        resp = _make_response(200, SAMPLE_LOGIN_RESPONSE_USER_NOT_FOUND)
        api._session.post = MagicMock(return_value=AsyncContextManager(resp))

        result = await api.async_login("nobody@test.com", "pass")

        assert result["token"] is None
        assert result["error"] == "user_not_exist"

    async def test_login_invalid_json(self):
        api = _make_api()
        resp = AsyncMock()
        resp.status = 200
        resp.text = AsyncMock(return_value="not json at all")
        resp.headers = {}
        api._session.post = MagicMock(return_value=AsyncContextManager(resp))

        result = await api.async_login("user@test.com", "pass")

        assert result["token"] is None
        assert result["error"] == "invalid_json"

    async def test_login_connection_error(self):
        api = _make_api()
        api._session.post = MagicMock(side_effect=Exception("Connection refused"))

        result = await api.async_login("user@test.com", "pass")

        assert result["token"] is None
        assert result["error"] == "connection_error"

    async def test_login_no_user_token_in_response(self):
        api = _make_api()
        response_data = {
            "code": "200",
            "data": {"uid": "123"},  # Missing userToken
            "error": False,
        }
        resp = _make_response(200, response_data)
        api._session.post = MagicMock(return_value=AsyncContextManager(resp))

        result = await api.async_login("user@test.com", "pass")

        assert result["token"] is None
        assert result["error"] == "invalid_response"

    async def test_login_unknown_error_code(self):
        api = _make_api()
        response_data = {
            "code": "9999999",
            "message": "something weird",
            "data": None,
            "error": True,
        }
        resp = _make_response(200, response_data)
        api._session.post = MagicMock(return_value=AsyncContextManager(resp))

        result = await api.async_login("user@test.com", "pass")

        assert result["token"] is None
        assert result["error"] == "unknown_error"


# ---------------------------------------------------------------------------
# Token refresh tests
# ---------------------------------------------------------------------------

class TestAsyncRefreshToken:
    """Tests for async_refresh_token."""

    async def test_refresh_success(self):
        api = _make_api(token="old_token")
        resp = _make_response(200, SAMPLE_REFRESH_RESPONSE_SUCCESS)
        api._session.post = MagicMock(return_value=AsyncContextManager(resp))

        result = await api.async_refresh_token("my_refresh_token")

        assert result["token"] == "new_refreshed_token_456"
        assert result["expires_at"] == 1791000000
        assert result["error"] is None
        # Verify the API instance token was updated
        assert api._token == "new_refreshed_token_456"

    async def test_refresh_failed_status(self):
        api = _make_api()
        resp = _make_response(400, SAMPLE_TOKEN_INVALID_RESPONSE)
        api._session.post = MagicMock(return_value=AsyncContextManager(resp))

        result = await api.async_refresh_token("bad_refresh_token")

        assert result["token"] is None
        assert result["error"] == "refresh_failed"

    async def test_refresh_no_url(self):
        api = _make_api()
        api.refresh_token_url = None

        result = await api.async_refresh_token("some_token")

        assert result["token"] is None
        assert result["error"] == "no_refresh_url"

    async def test_refresh_connection_error(self):
        api = _make_api()
        api._session.post = MagicMock(side_effect=Exception("Timeout"))

        result = await api.async_refresh_token("token")

        assert result["token"] is None
        assert result["error"] == "connection_error"


# ---------------------------------------------------------------------------
# Get devices tests
# ---------------------------------------------------------------------------

class TestGetDevices:
    """Tests for get_devices."""

    async def test_get_devices_success(self):
        api = _make_api()
        resp = _make_response(200, SAMPLE_DEVICES_RESPONSE)
        api._session.get = MagicMock(return_value=AsyncContextManager(resp))

        result = await api.get_devices()

        assert result is not None
        assert len(result["devices"]) == 2

    async def test_get_devices_token_expired(self):
        api = _make_api()
        resp = _make_response(400, SAMPLE_TOKEN_INVALID_RESPONSE)
        api._session.get = MagicMock(return_value=AsyncContextManager(resp))

        with pytest.raises(TokenExpiredError):
            await api.get_devices()

    async def test_get_devices_other_error(self):
        api = _make_api()
        resp = _make_response(500, {"error": "internal"})
        api._session.get = MagicMock(return_value=AsyncContextManager(resp))

        result = await api.get_devices()
        assert result is None

    async def test_get_devices_400_non_token_error(self):
        api = _make_api()
        resp = _make_response(400, {"error_message": "bad request", "error_code": 9015})
        api._session.get = MagicMock(return_value=AsyncContextManager(resp))

        result = await api.get_devices()
        assert result is None  # Should NOT raise TokenExpiredError


# ---------------------------------------------------------------------------
# Get device data tests
# ---------------------------------------------------------------------------

class TestGetDeviceData:
    """Tests for get_device_data."""

    async def test_get_device_data_success(self):
        api = _make_api()
        resp = _make_response(200, SAMPLE_DEVICE_DATA)
        api._session.get = MagicMock(return_value=AsyncContextManager(resp))

        result = await api.get_device_data("test_device_001")

        assert result is not None
        assert result["attr"]["switch"] is True

    async def test_get_device_data_token_expired(self):
        api = _make_api()
        resp = _make_response(400, SAMPLE_TOKEN_INVALID_RESPONSE)
        api._session.get = MagicMock(return_value=AsyncContextManager(resp))

        with pytest.raises(TokenExpiredError):
            await api.get_device_data("test_device_001")

    async def test_get_device_data_connection_error(self):
        api = _make_api()
        api._session.get = MagicMock(side_effect=Exception("Network error"))

        result = await api.get_device_data("test_device_001")
        assert result is None


# ---------------------------------------------------------------------------
# Control device tests
# ---------------------------------------------------------------------------

class TestControlDevice:
    """Tests for control_device."""

    async def test_control_device_success(self):
        api = _make_api()
        resp = _make_response(200, {"result": "ok"})
        api._session.post = MagicMock(return_value=AsyncContextManager(resp))

        result = await api.control_device("test_device_001", {"switch": True})

        assert result is not None

    async def test_control_device_token_expired(self):
        api = _make_api()
        resp = _make_response(400, SAMPLE_TOKEN_INVALID_RESPONSE)
        api._session.post = MagicMock(return_value=AsyncContextManager(resp))

        with pytest.raises(TokenExpiredError):
            await api.control_device("test_device_001", {"switch": True})

    async def test_control_device_server_error(self):
        api = _make_api()
        resp = _make_response(500, {"error": "internal"})
        api._session.post = MagicMock(return_value=AsyncContextManager(resp))

        result = await api.control_device("test_device_001", {"switch": True})
        assert result is None


# ---------------------------------------------------------------------------
# Session management tests
# ---------------------------------------------------------------------------

class TestSessionManagement:
    """Tests for session init and ensure."""

    async def test_init_session_creates_session(self):
        api = _make_api()
        # Remove the mock session to test creation
        del api._session
        with patch("custom_components.jebao_aqua.api.aiohttp.ClientSession") as mock_cs:
            mock_session = AsyncMock()
            mock_cs.return_value = mock_session
            await api.async_init_session()
            assert mock_cs.called

    async def test_init_session_skips_if_active(self):
        api = _make_api()
        api._session = MagicMock()
        api._session.closed = False
        with patch("custom_components.jebao_aqua.api.aiohttp.ClientSession") as mock_cs:
            await api.async_init_session()
            assert not mock_cs.called

    async def test_ensure_session_recreates_if_closed(self):
        api = _make_api()
        api._session.closed = True
        with patch("custom_components.jebao_aqua.api.aiohttp.ClientSession") as mock_cs:
            mock_session = AsyncMock()
            mock_cs.return_value = mock_session
            await api._ensure_session()
            assert mock_cs.called

    async def test_set_token(self):
        api = _make_api(token="old")
        api.set_token("new_token")
        assert api._token == "new_token"


# ---------------------------------------------------------------------------
# Async context manager helper for mocking `async with session.post(...)`
# ---------------------------------------------------------------------------

class AsyncContextManager:
    """Helper to mock async context managers (async with ...)."""

    def __init__(self, return_value):
        self._return_value = return_value

    async def __aenter__(self):
        return self._return_value

    async def __aexit__(self, *args):
        pass
