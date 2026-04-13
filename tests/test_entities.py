"""Tests for entity platforms (switch, binary_sensor, select, number)."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from custom_components.jebao_aqua.switch import JebaoPumpSwitch
from custom_components.jebao_aqua.binary_sensor import JebaoPumpSensor
from custom_components.jebao_aqua.select import JebaoPumpSelect
from custom_components.jebao_aqua.number import JebaoPumpNumber
from custom_components.jebao_aqua.const import DOMAIN

from conftest import (
    SAMPLE_DEVICE,
    SAMPLE_DEVICE_DATA,
    SAMPLE_MODEL,
    SAMPLE_ATTRIBUTE_MODELS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_coordinator(device_data=None):
    coordinator = MagicMock()
    coordinator.device_data = device_data or {"test_device_001": SAMPLE_DEVICE_DATA}
    coordinator.api = AsyncMock()
    coordinator.async_request_refresh = AsyncMock()
    return coordinator


def _get_attr(name):
    """Get an attribute definition from the sample model by name."""
    return next(a for a in SAMPLE_MODEL["attrs"] if a["name"] == name)


# ---------------------------------------------------------------------------
# Switch tests
# ---------------------------------------------------------------------------

class TestJebaoPumpSwitch:
    def test_init(self):
        coord = _make_coordinator()
        attr = _get_attr("switch")
        entity = JebaoPumpSwitch(coord, SAMPLE_DEVICE, attr)

        assert entity._attr_unique_id == "test_device_001_switch"
        assert entity.has_entity_name is True

    def test_is_on_true(self):
        coord = _make_coordinator()
        attr = _get_attr("switch")
        entity = JebaoPumpSwitch(coord, SAMPLE_DEVICE, attr)

        assert entity.is_on is True

    def test_is_on_false(self):
        data = {"test_device_001": {"attr": {"switch": False}}}
        coord = _make_coordinator(device_data=data)
        attr = _get_attr("switch")
        entity = JebaoPumpSwitch(coord, SAMPLE_DEVICE, attr)

        assert entity.is_on is False

    def test_available_with_valid_data(self):
        coord = _make_coordinator()
        attr = _get_attr("switch")
        entity = JebaoPumpSwitch(coord, SAMPLE_DEVICE, attr)

        assert entity.available is True

    def test_unavailable_with_no_data(self):
        coord = _make_coordinator(device_data={})
        attr = _get_attr("switch")
        entity = JebaoPumpSwitch(coord, SAMPLE_DEVICE, attr)

        assert entity.available is False

    def test_device_info(self):
        coord = _make_coordinator()
        attr = _get_attr("switch")
        entity = JebaoPumpSwitch(coord, SAMPLE_DEVICE, attr)
        info = entity.device_info

        assert info["name"] == "Test Pump"
        assert (DOMAIN, "test_device_001") in info["identifiers"]

    async def test_turn_on(self):
        coord = _make_coordinator()
        attr = _get_attr("switch")
        entity = JebaoPumpSwitch(coord, SAMPLE_DEVICE, attr)

        await entity.async_turn_on()

        coord.api.control_device.assert_called_once_with(
            "test_device_001", {"switch": True}
        )

    async def test_turn_off(self):
        coord = _make_coordinator()
        attr = _get_attr("switch")
        entity = JebaoPumpSwitch(coord, SAMPLE_DEVICE, attr)

        await entity.async_turn_off()

        coord.api.control_device.assert_called_once_with(
            "test_device_001", {"switch": False}
        )

    def test_translation_key(self):
        coord = _make_coordinator()
        attr = _get_attr("switch")
        entity = JebaoPumpSwitch(coord, SAMPLE_DEVICE, attr)

        assert entity.translation_key == "switch"


# ---------------------------------------------------------------------------
# Binary sensor tests
# ---------------------------------------------------------------------------

class TestJebaoPumpSensor:
    def test_init(self):
        coord = _make_coordinator()
        attr = _get_attr("Error")
        entity = JebaoPumpSensor(coord, SAMPLE_DEVICE, attr)

        assert entity._attr_unique_id == "test_device_001_error"

    def test_is_on_false_when_missing(self):
        coord = _make_coordinator()
        attr = _get_attr("Error")
        entity = JebaoPumpSensor(coord, SAMPLE_DEVICE, attr)

        # "Error" not in SAMPLE_DEVICE_DATA attrs, so should default False
        assert entity.is_on is False

    def test_is_on_true(self):
        data = {"test_device_001": {"attr": {"Error": True}}}
        coord = _make_coordinator(device_data=data)
        attr = _get_attr("Error")
        entity = JebaoPumpSensor(coord, SAMPLE_DEVICE, attr)

        assert entity.is_on is True

    def test_device_class(self):
        from homeassistant.components.binary_sensor import BinarySensorDeviceClass
        coord = _make_coordinator()
        attr = _get_attr("Error")
        entity = JebaoPumpSensor(coord, SAMPLE_DEVICE, attr)

        assert entity.device_class == BinarySensorDeviceClass.PROBLEM


# ---------------------------------------------------------------------------
# Select tests
# ---------------------------------------------------------------------------

class TestJebaoPumpSelect:
    def test_init(self):
        coord = _make_coordinator()
        attr = _get_attr("Mode")
        entity = JebaoPumpSelect(coord, SAMPLE_DEVICE, attr)

        assert entity._attr_unique_id == "test_device_001_mode"
        assert entity.options == ["Off", "Constant", "Pulse", "Wave"]

    def test_current_option(self):
        coord = _make_coordinator()
        attr = _get_attr("Mode")
        entity = JebaoPumpSelect(coord, SAMPLE_DEVICE, attr)

        # Mode is 2 in SAMPLE_DEVICE_DATA, which maps to "Pulse"
        assert entity.current_option == "Pulse"

    def test_current_option_none_when_missing(self):
        data = {"test_device_001": {"attr": {"Mode": 99}}}
        coord = _make_coordinator(device_data=data)
        attr = _get_attr("Mode")
        entity = JebaoPumpSelect(coord, SAMPLE_DEVICE, attr)

        assert entity.current_option is None

    async def test_select_option(self):
        coord = _make_coordinator()
        attr = _get_attr("Mode")
        entity = JebaoPumpSelect(coord, SAMPLE_DEVICE, attr)

        await entity.async_select_option("Wave")

        coord.api.control_device.assert_called_once_with(
            "test_device_001", {"Mode": 3}
        )

    def test_available(self):
        coord = _make_coordinator()
        attr = _get_attr("Mode")
        entity = JebaoPumpSelect(coord, SAMPLE_DEVICE, attr)
        assert entity.available is True


# ---------------------------------------------------------------------------
# Number tests
# ---------------------------------------------------------------------------

class TestJebaoPumpNumber:
    def test_init(self):
        coord = _make_coordinator()
        attr = _get_attr("Speed")
        entity = JebaoPumpNumber(coord, SAMPLE_DEVICE, attr)

        assert entity._attr_unique_id == "test_device_001_speed"
        assert entity._attr_native_min_value == 1
        assert entity._attr_native_max_value == 100
        assert entity._attr_native_step == 1
        assert entity._attr_native_unit_of_measurement == "%"

    def test_native_value(self):
        coord = _make_coordinator()
        attr = _get_attr("Speed")
        entity = JebaoPumpNumber(coord, SAMPLE_DEVICE, attr)

        assert entity.native_value == 50

    def test_native_value_fallback_to_min(self):
        data = {"test_device_001": {"attr": {}}}
        coord = _make_coordinator(device_data=data)
        attr = _get_attr("Speed")
        entity = JebaoPumpNumber(coord, SAMPLE_DEVICE, attr)

        assert entity.native_value == 1  # Falls back to min

    async def test_set_native_value(self):
        coord = _make_coordinator()
        attr = _get_attr("Speed")
        entity = JebaoPumpNumber(coord, SAMPLE_DEVICE, attr)

        await entity.async_set_native_value(75.0)

        coord.api.control_device.assert_called_once_with(
            "test_device_001", {"Speed": 75.0}
        )

    def test_available(self):
        coord = _make_coordinator()
        attr = _get_attr("Speed")
        entity = JebaoPumpNumber(coord, SAMPLE_DEVICE, attr)
        assert entity.available is True

    def test_unavailable(self):
        coord = _make_coordinator(device_data={})
        attr = _get_attr("Speed")
        entity = JebaoPumpNumber(coord, SAMPLE_DEVICE, attr)
        assert entity.available is False
