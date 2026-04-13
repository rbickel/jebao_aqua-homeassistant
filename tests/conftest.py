"""Shared fixtures for jebao_aqua tests."""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

SAMPLE_DEVICE = {
    "did": "test_device_001",
    "product_key": "5b3c136fd4b74f3fb2a366a254c76c9a",
    "dev_alias": "Test Pump",
    "lan_ip": "192.168.1.100",
}

SAMPLE_DEVICE_NO_LAN = {
    "did": "test_device_002",
    "product_key": "5b3c136fd4b74f3fb2a366a254c76c9a",
    "dev_alias": "Cloud Pump",
    "lan_ip": None,
}

SAMPLE_DEVICE_DATA = {
    "did": "test_device_001",
    "attr": {
        "switch": True,
        "channe1": False,
        "Speed": 50,
        "Mode": 2,
    },
}

SAMPLE_LOGIN_RESPONSE_SUCCESS = {
    "code": "200",
    "message": "success",
    "data": {
        "userToken": "test_user_token_abc123",
        "refreshToken": "test_refresh_token_xyz789",
        "uid": "test_uid_001",
        "userId": 12345,
        "createdAt": 1770000000,
        "expiredAt": 1785552000,
    },
    "error": False,
}

SAMPLE_LOGIN_RESPONSE_BAD_PASSWORD = {
    "code": "1000033",
    "message": "invalid password",
    "data": None,
    "error": True,
}

SAMPLE_LOGIN_RESPONSE_USER_NOT_FOUND = {
    "code": "1000000",
    "message": "user not exist",
    "data": None,
    "error": True,
}

SAMPLE_REFRESH_RESPONSE_SUCCESS = {
    "token": "new_refreshed_token_456",
    "uid": "test_uid_001",
    "expire_at": 1791000000,
}

SAMPLE_TOKEN_INVALID_RESPONSE = {
    "error_message": "token invalid!",
    "error_code": 9004,
    "detail_message": None,
}

SAMPLE_DEVICES_RESPONSE = {
    "devices": [
        {
            "did": "test_device_001",
            "product_key": "5b3c136fd4b74f3fb2a366a254c76c9a",
            "dev_alias": "Test Pump",
        },
        {
            "did": "test_device_002",
            "product_key": "5b3c136fd4b74f3fb2a366a254c76c9a",
            "dev_alias": "Cloud Pump",
        },
    ]
}

SAMPLE_MODEL = {
    "product_key": "5b3c136fd4b74f3fb2a366a254c76c9a",
    "name": "Doser 2.4 WiFi 4-Channel",
    "attrs": [
        {
            "display_name": "Power Switch",
            "name": "switch",
            "data_type": "bool",
            "position": {"byte_offset": 0, "unit": "bit", "len": 1, "bit_offset": 0},
            "type": "status_writable",
            "id": 0,
        },
        {
            "display_name": "Speed",
            "name": "Speed",
            "data_type": "uint8",
            "position": {"byte_offset": 1, "unit": "byte", "len": 1, "bit_offset": 0},
            "uint_spec": {"addition": 0, "max": 100, "ratio": 1, "min": 1},
            "type": "status_writable",
            "id": 1,
            "unit": "%",
        },
        {
            "display_name": "Mode",
            "name": "Mode",
            "data_type": "enum",
            "position": {"byte_offset": 2, "unit": "byte", "len": 1, "bit_offset": 0},
            "enum": [0, 1, 2, 3],
            "desc": ["Off", "Constant", "Pulse", "Wave"],
            "type": "status_writable",
            "id": 2,
        },
        {
            "display_name": "Error",
            "name": "Error",
            "data_type": "bool",
            "position": {"byte_offset": 3, "unit": "bit", "len": 1, "bit_offset": 0},
            "type": "fault",
            "id": 3,
        },
    ],
}

SAMPLE_ATTRIBUTE_MODELS = {
    "5b3c136fd4b74f3fb2a366a254c76c9a": SAMPLE_MODEL,
}

CONFIG_ENTRY_DATA_CLOUD = {
    "token": "test_token_123",
    "refresh_token": "test_refresh_456",
    "token_expires_at": 1785552000,
    "email": "test@example.com",
    "password": "testpass",
    "region": "eu",
    "country": "DE",
    "devices": [
        {
            "did": "test_device_001",
            "product_key": "5b3c136fd4b74f3fb2a366a254c76c9a",
            "dev_alias": "Test Pump",
            "lan_ip": "192.168.1.100",
        },
    ],
}

CONFIG_ENTRY_DATA_LAN_ONLY = {
    "lan_only": True,
    "devices": [
        {
            "did": "lan_192_168_1_50",
            "product_key": "5b3c136fd4b74f3fb2a366a254c76c9a",
            "dev_alias": "LAN Pump",
            "lan_ip": "192.168.1.50",
        },
    ],
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_aiohttp_session():
    """Create a mock aiohttp session."""
    session = AsyncMock()
    session.closed = False
    return session


@pytest.fixture
def api_urls():
    """Return EU region API URLs."""
    from custom_components.jebao_aqua.const import GIZWITS_API_URLS
    return GIZWITS_API_URLS["eu"]
