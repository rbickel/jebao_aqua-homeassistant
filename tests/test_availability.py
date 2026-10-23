"""Live availability must not be inferred from retained attribute snapshots."""

import asyncio
from copy import deepcopy
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custom_components.jebao_aqua import GizwitsDataUpdateCoordinator, local
from custom_components.jebao_aqua.api import AuthenticationError, GizwitsApi
from custom_components.jebao_aqua.const import (
    AVAILABILITY_FAILURES, AVAILABILITY_INTERVAL, AVAILABILITY_MAX_AGE, DOMAIN,
)
from custom_components.jebao_aqua.discovery import DiscoveryProtocol

from .conftest import MOCK_DEVICE, MOCK_DEVICE_DATA, MOCK_DEVICE_ID


def cloud_response(online):
    return {"devices": [{**MOCK_DEVICE, "is_online": online}]}


@pytest.fixture
def coordinator(hass, mock_api):
    instance = GizwitsDataUpdateCoordinator(hass, mock_api)
    instance.device_inventory = [MOCK_DEVICE.copy()]
    instance.device_data = deepcopy(MOCK_DEVICE_DATA)
    instance._mark_device_seen(MOCK_DEVICE_ID)
    return instance


async def miss_twice(coordinator):
    coordinator.api.ping_device.return_value = False
    coordinator.api.get_devices.return_value = cloud_response(False)
    await coordinator.async_check_availability()
    await coordinator.async_check_availability()


def test_configured_health_thresholds_are_bounded():
    assert AVAILABILITY_INTERVAL.total_seconds() == 15
    assert AVAILABILITY_FAILURES == 2
    assert AVAILABILITY_MAX_AGE == 45


def test_cached_values_and_cached_online_flags_are_not_liveness_proof(coordinator):
    coordinator._last_device_seen.clear()
    assert coordinator.device_inventory[0]["is_online"] is True
    assert coordinator.device_data[MOCK_DEVICE_ID]["attr"]
    assert coordinator.is_device_available(MOCK_DEVICE_ID) is False


@pytest.mark.asyncio
async def test_two_misses_mark_unavailable_but_preserve_values(coordinator):
    before = deepcopy(coordinator.device_data)
    coordinator.api.ping_device.return_value = False
    coordinator.api.get_devices.return_value = cloud_response(False)
    await coordinator.async_check_availability()
    assert coordinator.is_device_available(MOCK_DEVICE_ID) is True
    assert coordinator._availability_failures[MOCK_DEVICE_ID] == 1
    await coordinator.async_check_availability()
    assert coordinator.is_device_available(MOCK_DEVICE_ID) is False
    assert coordinator.device_data == before
    assert coordinator.api.get_devices.await_count == 2


@pytest.mark.asyncio
async def test_cached_cloud_attributes_cannot_resurrect_an_offline_device(coordinator):
    await miss_twice(coordinator)
    last_seen = coordinator._last_device_seen[MOCK_DEVICE_ID]
    coordinator.api.get_local_device_data.return_value = None
    await coordinator.async_refresh()
    assert coordinator.device_data[MOCK_DEVICE_ID]["attr"]
    assert coordinator.is_device_available(MOCK_DEVICE_ID) is False
    assert coordinator._last_device_seen[MOCK_DEVICE_ID] == last_seen
    coordinator.api.control_device.return_value = {}
    await coordinator.async_control_device(MOCK_DEVICE_ID, {"Flow": 50})
    assert coordinator.is_device_available(MOCK_DEVICE_ID) is False


@pytest.mark.asyncio
async def test_fresh_cloud_online_status_allows_cloud_fallback(coordinator):
    await miss_twice(coordinator)
    coordinator.device_inventory[0]["is_online"] = False
    coordinator.api.get_devices.return_value = cloud_response(True)
    await coordinator.async_check_availability()
    assert coordinator.is_device_available(MOCK_DEVICE_ID) is True
    assert coordinator._availability_failures[MOCK_DEVICE_ID] == 0


@pytest.mark.asyncio
async def test_cloud_only_device_requires_fresh_online_status(coordinator):
    coordinator.device_inventory[0]["lan_ip"] = None
    coordinator._last_device_seen.clear()
    coordinator.api.get_devices.return_value = cloud_response(False)
    await coordinator.async_check_availability()
    assert coordinator.is_device_available(MOCK_DEVICE_ID) is False
    coordinator.api.get_devices.return_value = cloud_response(True)
    await coordinator.async_check_availability()
    assert coordinator.is_device_available(MOCK_DEVICE_ID) is True
    coordinator.api.ping_device.assert_not_awaited()


@pytest.mark.asyncio
async def test_local_recovery_skips_cloud_and_resets_lan_backoff(coordinator):
    await miss_twice(coordinator)
    coordinator._lan_failure_counts[MOCK_DEVICE_ID] = 3
    coordinator.api.ping_device.return_value = True
    coordinator.api.get_devices.reset_mock()
    coordinator.api.get_devices.side_effect = AuthenticationError("cloud offline")
    await coordinator.async_check_availability()
    assert coordinator.is_device_available(MOCK_DEVICE_ID) is True
    assert coordinator._lan_failure_counts[MOCK_DEVICE_ID] == 0
    coordinator.api.get_devices.assert_not_awaited()
    await coordinator.get_device_data(MOCK_DEVICE_ID)
    coordinator.api.get_local_device_data.assert_awaited_once()
    coordinator.api.get_device_data.assert_not_awaited()


@pytest.mark.asyncio
async def test_verified_read_and_command_also_restore_health(coordinator):
    await miss_twice(coordinator)
    await coordinator.get_device_data(MOCK_DEVICE_ID)
    assert coordinator.is_device_available(MOCK_DEVICE_ID) is True
    await miss_twice(coordinator)
    coordinator.device_inventory[0]["product_key"] = local.PRODUCT_KEY
    coordinator.api.control_local_device = AsyncMock(return_value=deepcopy(MOCK_DEVICE_DATA[MOCK_DEVICE_ID]))
    await coordinator.async_control_device(MOCK_DEVICE_ID, {"Flow": 76})
    assert coordinator.is_device_available(MOCK_DEVICE_ID) is True


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [AuthenticationError("expired"), TimeoutError("cloud timeout")])
async def test_cloud_health_errors_do_not_keep_cached_entities_available(coordinator, error, caplog):
    coordinator.api.ping_device.return_value = False
    coordinator.api.get_devices.side_effect = error
    for _ in range(2):
        await coordinator.async_check_availability()
    assert coordinator.is_device_available(MOCK_DEVICE_ID) is False
    assert "Cloud availability check failed" in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("response", [
    None, True, {}, {"devices": []}, {"devices": [None]},
    {"devices": [{"did": MOCK_DEVICE_ID}]},
    {"devices": [{"did": MOCK_DEVICE_ID, "is_online": "true"}]},
    {"devices": [{"did": "another_device", "is_online": True}]},
])
async def test_invalid_or_unrelated_cloud_status_is_not_liveness_proof(coordinator, response):
    coordinator.api.ping_device.return_value = False
    coordinator.api.get_devices.return_value = response
    for _ in range(2):
        await coordinator.async_check_availability()
    assert coordinator.is_device_available(MOCK_DEVICE_ID) is False


@pytest.mark.asyncio
async def test_cloud_health_request_timeout_is_bounded(coordinator, caplog):
    coordinator.api.ping_device.return_value = False

    async def blocked():
        await asyncio.Event().wait()

    coordinator.api.get_devices.side_effect = blocked
    with patch("custom_components.jebao_aqua.AVAILABILITY_TIMEOUT", 0.01):
        for _ in range(2):
            await coordinator.async_check_availability()
    assert coordinator.is_device_available(MOCK_DEVICE_ID) is False
    assert "Cloud availability check failed" in caplog.text


@pytest.mark.asyncio
async def test_independent_device_health_and_one_cloud_query_per_cycle(coordinator):
    other_id = "another_device"
    coordinator.device_inventory.append({**MOCK_DEVICE, "did": other_id})
    coordinator.device_data[other_id] = {**deepcopy(MOCK_DEVICE_DATA[MOCK_DEVICE_ID]), "did": other_id}
    coordinator._mark_device_seen(other_id)
    coordinator.api.ping_device.side_effect = lambda ip, did: did == other_id
    coordinator.api.get_devices.return_value = {"devices": []}
    for _ in range(2):
        await coordinator.async_check_availability()
    assert coordinator.is_device_available(MOCK_DEVICE_ID) is False
    assert coordinator.is_device_available(other_id) is True
    assert coordinator.api.get_devices.await_count == 2


def test_stale_health_expires_without_deleting_values(coordinator):
    coordinator._last_device_seen[MOCK_DEVICE_ID] = time.monotonic() - AVAILABILITY_MAX_AGE - 1
    assert coordinator.is_device_available(MOCK_DEVICE_ID) is False
    assert coordinator.device_data[MOCK_DEVICE_ID]["attr"]


def test_live_device_without_attributes_is_not_an_available_entity(coordinator):
    coordinator.device_data.clear()
    assert coordinator.is_device_available(MOCK_DEVICE_ID) is False


@pytest.mark.asyncio
async def test_every_entity_platform_goes_unavailable_and_recovers(hass, mock_setup_entry):
    coordinator = mock_setup_entry["coordinator"]
    api = mock_setup_entry["api"]
    entity_ids = [
        state.entity_id for state in hass.states.async_all()
        if state.entity_id.startswith((
            "switch.test_pump_", "number.test_pump_",
            "select.test_pump_", "binary_sensor.test_pump_",
        ))
    ]
    assert {entity_id.split(".")[0] for entity_id in entity_ids} == {
        "switch", "number", "select", "binary_sensor",
    }
    initial = {entity_id: hass.states.get(entity_id).state for entity_id in entity_ids}
    assert "unavailable" not in initial.values()
    await miss_twice(coordinator)
    await hass.async_block_till_done()
    assert all(hass.states.get(entity_id).state == "unavailable" for entity_id in entity_ids)
    api.ping_device.return_value = True
    await coordinator.async_check_availability()
    await hass.async_block_till_done()
    assert {entity_id: hass.states.get(entity_id).state for entity_id in entity_ids} == initial


@pytest.mark.asyncio
async def test_health_timer_does_not_overlap_and_is_cancelled_on_unload(coordinator, mock_config_entry):
    started = asyncio.Event()

    async def blocked_ping(*_args):
        started.set()
        await asyncio.Event().wait()

    coordinator.api.ping_device.side_effect = blocked_ping
    unsubscribe = MagicMock()
    with patch("custom_components.jebao_aqua.async_track_time_interval", return_value=unsubscribe) as track:
        coordinator.async_start_availability_checks(mock_config_entry)
        await started.wait()
        interval_callback = track.call_args.args[1]
        assert track.call_args.args[2] == AVAILABILITY_INTERVAL
        interval_callback()
        interval_callback()
        assert coordinator.api.ping_device.await_count == 1
        await coordinator.async_stop_availability_checks()
        unsubscribe.assert_called_once()
        assert coordinator._availability_task is None
        interval_callback()
        assert coordinator.api.ping_device.await_count == 1


@pytest.mark.asyncio
async def test_health_does_not_wait_for_a_status_or_command_lock(coordinator):
    lock = asyncio.Lock()
    coordinator._device_update_locks[MOCK_DEVICE_ID] = lock
    await lock.acquire()
    try:
        async with asyncio.timeout(0.1):
            await coordinator.async_check_availability()
    finally:
        lock.release()
    coordinator.api.ping_device.assert_awaited_once()
    coordinator.api.get_devices.assert_not_awaited()


@pytest.mark.asyncio
async def test_completed_empty_checks_do_not_stall_the_timer(hass, coordinator, mock_config_entry):
    coordinator.device_inventory = []
    with patch("custom_components.jebao_aqua.async_track_time_interval") as track, patch.object(
        coordinator, "async_check_availability", wraps=coordinator.async_check_availability,
    ) as check:
        coordinator.async_start_availability_checks(mock_config_entry)
        await hass.async_block_till_done()
        track.call_args.args[1]()
        await hass.async_block_till_done()
        assert check.await_count == 2
        await coordinator.async_stop_availability_checks()
        assert coordinator._availability_task is None


@pytest.mark.asyncio
async def test_entry_unload_stops_health_before_closing_api(hass, mock_setup_entry):
    coordinator = mock_setup_entry["coordinator"]
    api = mock_setup_entry["api"]
    entry = hass.config_entries.async_entries(DOMAIN)[0]
    events = []
    original_stop = coordinator.async_stop_availability_checks

    async def stop():
        await original_stop()
        events.append("health-stopped")

    async def close():
        assert coordinator._availability_stopped
        assert coordinator._availability_task is None
        events.append("api-closed")

    with patch.object(coordinator, "async_stop_availability_checks", side_effect=stop), patch.object(
        api, "close", side_effect=close,
    ):
        assert await hass.config_entries.async_unload(entry.entry_id)
    assert events == ["health-stopped", "api-closed"]


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [TimeoutError("no reply"), local.ProbeError("wrong identity")])
async def test_api_ping_errors_are_not_success(error):
    api = GizwitsApi("", "", "", "")
    with patch("custom_components.jebao_aqua.api.ping_local", side_effect=error):
        assert await api.ping_device("192.0.2.1", MOCK_DEVICE_ID) is False


def test_discovery_uses_the_same_validated_identity_parser():
    protocol = DiscoveryProtocol()
    encoded = MOCK_DEVICE_ID.encode("ascii")
    packet = local.encode_frame(4, len(encoded).to_bytes(2, "big") + encoded)
    protocol.datagram_received(b"garbage" * 10, ("192.0.2.2", 12414))
    assert not protocol.results
    protocol.datagram_received(packet, ("192.0.2.1", 12414))
    assert protocol.results == {MOCK_DEVICE_ID: "192.0.2.1"}
