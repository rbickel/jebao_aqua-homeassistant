"""Tests for helper functions."""

import pytest
from custom_components.jebao_aqua.helpers import (
    get_device_info,
    create_entity_name,
    create_entity_id,
    create_unique_id,
    is_device_data_valid,
    get_attribute_value,
)
from custom_components.jebao_aqua.const import DOMAIN

from conftest import SAMPLE_DEVICE, SAMPLE_DEVICE_NO_LAN, SAMPLE_DEVICE_DATA


# ---------------------------------------------------------------------------
# get_device_info
# ---------------------------------------------------------------------------

class TestGetDeviceInfo:
    def test_device_with_lan_ip(self):
        info = get_device_info(SAMPLE_DEVICE)
        assert info["name"] == "Test Pump"
        assert info["manufacturer"] == "Jebao"
        assert (DOMAIN, "test_device_001") in info["identifiers"]
        assert ("ip", "192.168.1.100") in info["connections"]

    def test_device_without_lan_ip(self):
        info = get_device_info(SAMPLE_DEVICE_NO_LAN)
        assert info["name"] == "Cloud Pump"
        assert "connections" not in info

    def test_device_no_alias_uses_did(self):
        device = {"did": "dev_123", "lan_ip": None}
        info = get_device_info(device)
        assert info["name"] == "Device dev_123"


# ---------------------------------------------------------------------------
# create_entity_name / create_entity_id / create_unique_id
# ---------------------------------------------------------------------------

class TestEntityNaming:
    def test_create_entity_name(self):
        name = create_entity_name("Test Pump", "Power Switch")
        assert name == "Power Switch"

    def test_create_entity_id(self):
        eid = create_entity_id("switch", "Test Pump", "switch")
        assert eid == "switch.test_pump_switch"

    def test_create_entity_id_spaces(self):
        eid = create_entity_id("number", "My Device", "Max Speed")
        assert eid == "number.my_device_max_speed"

    def test_create_unique_id(self):
        uid = create_unique_id("device_001", "Power Switch")
        assert uid == "device_001_power_switch"

    def test_create_unique_id_no_spaces(self):
        uid = create_unique_id("dev", "Speed")
        assert uid == "dev_speed"


# ---------------------------------------------------------------------------
# is_device_data_valid
# ---------------------------------------------------------------------------

class TestIsDeviceDataValid:
    def test_valid_data(self):
        assert is_device_data_valid(SAMPLE_DEVICE_DATA) is True

    def test_none(self):
        assert is_device_data_valid(None) is False

    def test_empty_dict(self):
        assert is_device_data_valid({}) is False

    def test_no_attr_key(self):
        assert is_device_data_valid({"did": "123"}) is False

    def test_empty_attr(self):
        assert is_device_data_valid({"attr": {}}) is False

    def test_not_dict(self):
        assert is_device_data_valid("string") is False

    def test_attr_with_values(self):
        assert is_device_data_valid({"attr": {"switch": True}}) is True


# ---------------------------------------------------------------------------
# get_attribute_value
# ---------------------------------------------------------------------------

class TestGetAttributeValue:
    def test_existing_attribute(self):
        assert get_attribute_value(SAMPLE_DEVICE_DATA, "switch") is True

    def test_numeric_attribute(self):
        assert get_attribute_value(SAMPLE_DEVICE_DATA, "Speed") == 50

    def test_missing_attribute(self):
        assert get_attribute_value(SAMPLE_DEVICE_DATA, "nonexistent") is None

    def test_invalid_device_data(self):
        assert get_attribute_value(None, "switch") is None

    def test_empty_device_data(self):
        assert get_attribute_value({}, "switch") is None
