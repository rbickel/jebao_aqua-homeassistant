"""MDP10000 layout, legacy identity, command routing and identity-ping tests."""

from contextlib import nullcontext
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custom_components.jebao_aqua import GizwitsDataUpdateCoordinator, local
from custom_components.jebao_aqua.api import GizwitsApi
from custom_components.jebao_aqua.number import JebaoPumpNumber
from custom_components.jebao_aqua.switch import JebaoPumpSwitch


PROFILE = local.MDP10000
DID = "test_mdp10000"
IP = "192.0.2.1"
CHANGES = [
    ("SwitchON", False, 0), ("FeedSwitch", True, 2),
    ("Motor_Speed", 71, 5), ("FeedTime", 16, 6),
]


@pytest.fixture
def model():
    path = Path(__file__).resolve().parents[1] / "custom_components/jebao_aqua/models"
    return json.loads((path / f"{PROFILE.product_key}.json").read_text())


def snapshot(device_id=DID):
    raw = bytearray(index % 256 for index in range(PROFILE.status_size))
    raw[:5] = bytes([1, 70, 15, 70, 15])
    raw[301] = 0
    return local.Status(device_id, bytes(raw), PROFILE)


def changed(model, attributes):
    original = snapshot()
    payload = local.control_payload(original, attributes, model)
    return local.Status(DID, payload[9:] + original.raw[301:], PROFILE)


def discovery_packet(device_id=DID):
    encoded = device_id.encode("ascii")
    return local.encode_frame(4, len(encoded).to_bytes(2, "big") + encoded + bytes(20))


@pytest.mark.parametrize(("name", "value", "flag"), CHANGES)
def test_legacy_control_has_correct_mask_and_preserves_the_single_byte_bitfield(model, name, value, flag):
    original = snapshot()
    payload = local.control_payload(original, {name: value}, model)
    assert len(payload) == 310
    assert payload[:9] == b"\x01" + (1 << flag).to_bytes(8, "big")
    after = local.Status(DID, payload[9:] + original.raw[301:], PROFILE)
    expected = local.control_values(original, model)
    expected[name] = value
    assert local.control_values(after, model) == expected
    assert after.raw[3:] == original.raw[3:]
    assert local.encode_frame(0x0093, bytes(4) + payload)[:6] == bytes.fromhex("00000003bd02")


@pytest.mark.parametrize("attributes", [
    {"Motor_Speed": 29}, {"Motor_Speed": 101}, {"Motor_Speed": 1.5},
    {"FeedTime": 0}, {"FeedTime": 61}, {"TimerON": False}, {"Mode": False},
    {"Flow": 50}, {"SwitchON": 1},
])
def test_unexposed_or_invalid_mdp_commands_do_not_connect(model, attributes):
    with patch.object(local, "connect") as connect:
        with pytest.raises(local.InvalidCommand):
            local.control_device(IP, DID, attributes, model)
    connect.assert_not_called()


def test_legacy_uint8_accepts_integral_ha_number_values(model):
    assert local.normalize_attributes({"Motor_Speed": 71.0}, model) == {"Motor_Speed": 71}


def test_mcp_model_cannot_encode_a_legacy_snapshot():
    with pytest.raises(local.ProbeError, match="does not match"):
        local.decode_status(snapshot(), {"product_key": local.PRODUCT_KEY})
    with pytest.raises(local.InvalidCommand):
        local.flow_control(snapshot(), 71)


def test_legacy_login_binds_to_the_udp_device_identity():
    connection = MagicMock()
    connection.getpeername.return_value = (IP, 12416)
    client = local.GizwitsLocalClient(connection)
    info = b"00000000" * 6 + b"\x00\x00\x00\x20" + PROFILE.product_key.encode()
    with patch.object(client, "_response", side_effect=[
        local.Frame(0, 0x14, info),
        local.Frame(0, 0x07, b"\x00\x04test"),
        local.Frame(0, 0x09, b"\x00"),
    ]), patch.object(local, "discover_device", return_value=DID) as identify:
        client.login()
    identify.assert_called_once_with(IP)
    assert client.profile == PROFILE
    assert client.device_id == DID


def test_legacy_status_without_did_requires_an_identity_reply():
    client = local.GizwitsLocalClient(MagicMock())
    client.profile = PROFILE
    frame = local.Frame(0, 0x0094, b"\x00\x00\x00\x01\x03" + snapshot().raw)
    with patch.object(client, "_read_frame", return_value=frame):
        with pytest.raises(local.ProbeError, match="identified"):
            client.read_status()
    client.device_id = DID
    client.sequence = 0
    with patch.object(client, "_read_frame", return_value=frame):
        assert client.read_status() == snapshot()


def test_legacy_wrong_device_never_receives_a_command(model):
    client = MagicMock(spec=local.GizwitsLocalClient)
    client.read_status.return_value = snapshot("another_pump")
    with patch.object(local, "connect", return_value=nullcontext(client)):
        with pytest.raises(local.ProbeError, match="no command sent"):
            local.control_device(IP, DID, {"Motor_Speed": 71}, model)
    client.write_attributes.assert_not_called()


def test_discovery_parser_and_ping_validate_identity_and_sender():
    packet = discovery_packet()
    assert local.decode_discovery_response(packet) == DID
    connection = MagicMock()
    connection.recvfrom.return_value = (packet, (IP, local.DISCOVERY_PORT))
    with patch.object(local.socket, "socket", return_value=nullcontext(connection)):
        assert local.ping_device(IP, DID) is True
    connection.sendto.assert_called_once_with(local.DISCOVERY_REQUEST, (IP, local.DISCOVERY_PORT))
    with patch.object(local, "discover_device", return_value="another_pump"):
        with pytest.raises(local.ProbeError, match="different controller"):
            local.ping_device(IP, DID)


@pytest.mark.parametrize("packet", [
    b"", b"bad!", local.HEADER + b"\x80",
    local.HEADER + b"\x80\x80\x80\x80",
    local.encode_frame(5, b"\x00\x01a"),
    local.encode_frame(4, b"\x00\x00"),
    local.encode_frame(4, b"\x00\x04a"),
    local.encode_frame(4, b"\x00\x01\xff"),
    discovery_packet()[:-1],
])
def test_invalid_discovery_packets_are_not_health_proof(packet):
    with pytest.raises(local.ProbeError):
        local.decode_discovery_response(packet)


@pytest.mark.parametrize("address", [("192.0.2.2", 12414), (IP, 12345)])
def test_ping_does_not_accept_other_senders(address):
    connection = MagicMock()
    connection.recvfrom.side_effect = [(discovery_packet(), address), TimeoutError("no matching sender")]
    with patch.object(local.socket, "socket", return_value=nullcontext(connection)):
        with pytest.raises(TimeoutError):
            local.ping_device(IP, DID)


@pytest.mark.asyncio
@pytest.mark.parametrize(("name", "value", "_flag"), CHANGES)
async def test_every_mdp_entity_uses_local_commands_with_cloud_fallback(hass, mock_api, model, name, value, _flag):
    coordinator = GizwitsDataUpdateCoordinator(hass, mock_api)
    device = {"did": DID, "dev_alias": "MDP10000", "product_key": PROFILE.product_key, "lan_ip": IP}
    coordinator.device_inventory = [device]
    after = changed(model, {name: value})
    mock_api.control_local_device = AsyncMock(
        return_value={"did": DID, "attr": local.decode_status(after, model)},
    )
    attr = next(attr for attr in model["attrs"] if attr["name"] == name)
    with patch.object(
        coordinator, "async_request_refresh", new_callable=AsyncMock,
    ), patch("custom_components.jebao_aqua.switch.asyncio.sleep", new_callable=AsyncMock):
        if isinstance(value, bool):
            entity = JebaoPumpSwitch(coordinator, device, attr)
            if value:
                await entity.async_turn_on()
            else:
                await entity.async_turn_off()
            assert entity.is_on is value
        else:
            number = JebaoPumpNumber(coordinator, device, attr)
            await number.async_set_native_value(float(value))
            assert number.native_value == value
    mock_api.control_local_device.assert_awaited_once_with(IP, PROFILE.product_key, DID, {name: value})
    mock_api.control_device.assert_not_awaited()
    mock_api.control_local_device.side_effect = TimeoutError("offline")
    await coordinator.async_control_device(DID, {name: value})
    mock_api.control_device.assert_awaited_once_with(DID, {name: value})


@pytest.mark.asyncio
async def test_mdp_api_uses_the_shared_verified_transport(model):
    api = GizwitsApi("", "", "", "")
    api.add_attribute_models({PROFILE.product_key: model})
    after = changed(model, {"Motor_Speed": 71})
    with patch("custom_components.jebao_aqua.api.control_local", return_value=after) as command:
        result = await api.control_local_device(IP, PROFILE.product_key, DID, {"Motor_Speed": 71})
    command.assert_called_once_with(IP, DID, {"Motor_Speed": 71}, model)
    assert result["attr"]["Motor_Speed"] == 71
