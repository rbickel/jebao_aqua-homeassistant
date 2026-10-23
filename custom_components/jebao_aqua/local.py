"""Credential-free LAN protocol for verified Jebao pump products."""

from contextlib import contextmanager
from dataclasses import dataclass
import math
import socket
import time
from typing import Generator, Mapping


PRODUCT_KEY = "54114ccdac1e41c0bb17e222887c07ba"
PORT = 12416
HEADER = b"\x00\x00\x00\x03"
TIMEOUT = 5.0
MAX_FRAME_SIZE = 4096
STATUS_SIZE = 401
WRITABLE_SIZE = 400
FLOW_OFFSET = 2
FLOW_MASK = (1 << 8).to_bytes(8, "big")
CONTROL_NAMES = frozenset({
    "SwitchON", "PulseTide", "FeedSwitch", "TimerON",
    "Mode", "Linkage", "Flow", "Frequency", "FeedTime",
})
ControlValue = bool | int | float | str


@dataclass(frozen=True)
class ProductProfile:
    product_key: str
    name: str
    writable_size: int
    writable_attributes: int
    bitfield_bytes: int
    control_names: frozenset[str]
    schedule_range: tuple[int, int]
    clock_range: tuple[int, int]
    identified_status: bool = True

    @property
    def status_size(self) -> int:
        return self.writable_size + 1

    @property
    def flag_bytes(self) -> int:
        return (self.writable_attributes + 7) // 8


MCP150 = ProductProfile(
    PRODUCT_KEY, "MCP150", 400, 64, 2, CONTROL_NAMES, (8, 392), (392, 400),
)
MDP10000 = ProductProfile(
    "02039876751049deb404d1d89221ec4b", "MDP10000", 301, 59, 1,
    frozenset({"SwitchON", "FeedSwitch", "Motor_Speed", "FeedTime"}),
    (13, 301), (5, 13), identified_status=False,
)
PROFILES = {profile.product_key: profile for profile in (MCP150, MDP10000)}
DISCOVERY_PORT = 12414
DISCOVERY_REQUEST = bytes.fromhex("0000000303000003")
PING_TIMEOUT = 3.0


class ProbeError(Exception):
    """An unsupported response or failed verification."""


class RestorationError(ProbeError):
    """The original settings could not be confirmed after a test."""


class InvalidCommand(ProbeError, ValueError):
    """An invalid command must not be retried through the cloud."""


@dataclass(frozen=True)
class Frame:
    flag: int
    command: int
    payload: bytes


@dataclass(frozen=True)
class Status:
    device_id: str
    raw: bytes
    profile: ProductProfile = MCP150

    def __post_init__(self) -> None:
        if not self.device_id or len(self.raw) != self.profile.status_size:
            raise ProbeError(
                f"Expected a device ID and exactly {self.profile.status_size} status bytes"
            )

    @property
    def flow(self) -> int:
        if self.profile != MCP150:
            raise ProbeError("Flow is not a control on this product")
        return self.raw[FLOW_OFFSET]

    def summary(self) -> dict[str, int | bool]:
        if self.profile == MDP10000:
            return {
                "SwitchON": bool(self.raw[0] & 1),
                "FeedSwitch": bool(self.raw[0] & 4),
                "Motor_Speed": self.raw[1],
                "FeedTime": self.raw[2],
                "TimerON": bool(self.raw[0] & 8),
                "fault_bits": self.raw[self.profile.writable_size],
            }
        bits = int.from_bytes(self.raw[:2], "big")
        return {
            "SwitchON": bool(bits & 1),
            "PulseTide": bool(bits & 2),
            "FeedSwitch": bool(bits & 4),
            "TimerON": bool(bits & 8),
            "Mode": (bits >> 5) & 3,
            "Linkage": (bits >> 7) & 3,
            "Flow": self.flow,
            "Frequency": self.raw[3],
            "FeedTime": self.raw[4],
            "fault_bits": self.raw[400],
        }


def encode_frame(command: int, payload: bytes = b"") -> bytes:
    body = b"\x00" + command.to_bytes(2, "big") + payload
    length = len(body)
    if length > MAX_FRAME_SIZE:
        raise ProbeError("Outgoing frame is too large")
    encoded_length = bytearray()
    while True:
        byte = length & 0x7F
        length >>= 7
        encoded_length.append(byte | (0x80 if length else 0))
        if not length:
            return HEADER + encoded_length + body


def decode_discovery_response(packet: bytes) -> str:
    if not packet.startswith(HEADER):
        raise ProbeError("Invalid discovery frame header")
    index = len(HEADER)
    length = 0
    for shift in range(0, 28, 7):
        if index >= len(packet):
            raise ProbeError("Truncated discovery frame length")
        byte = packet[index]
        index += 1
        length |= (byte & 0x7F) << shift
        if not byte & 0x80:
            break
    else:
        raise ProbeError("Invalid discovery frame length encoding")
    body = packet[index:]
    if length != len(body) or not 5 <= length <= MAX_FRAME_SIZE:
        raise ProbeError("Invalid discovery frame size")
    if body[0] != 0 or int.from_bytes(body[1:3], "big") != 4:
        raise ProbeError("Unexpected discovery response command or flag")
    did_length = int.from_bytes(body[3:5], "big")
    if not 1 <= did_length <= 128 or 5 + did_length > len(body):
        raise ProbeError("Invalid discovery device ID length")
    try:
        return body[5:5 + did_length].decode("ascii")
    except UnicodeDecodeError as error:
        raise ProbeError("Invalid discovery device ID") from error


def discover_device(host: str, timeout: float = PING_TIMEOUT) -> str:
    """Identify one controller using a read-only unicast protocol request."""
    deadline = time.monotonic() + timeout
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
        connection.settimeout(timeout)
        connection.sendto(DISCOVERY_REQUEST, (host, DISCOVERY_PORT))
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Timed out waiting for the device identity ping")
            connection.settimeout(remaining)
            packet, address = connection.recvfrom(MAX_FRAME_SIZE)
            if address[0] == host and address[1] == DISCOVERY_PORT:
                return decode_discovery_response(packet)


def ping_device(host: str, device_id: str) -> bool:
    if discover_device(host) != device_id:
        raise ProbeError("Device identity ping answered by a different controller")
    return True


def parse_product_key(payload: bytes) -> str:
    if len(payload) < 52:
        raise ProbeError("Truncated device-info response")
    firmware_length = int.from_bytes(payload[48:50], "big")
    offset = 50 + firmware_length
    if offset + 2 > len(payload):
        raise ProbeError("Invalid firmware length in device-info response")
    key_length = int.from_bytes(payload[offset:offset + 2], "big")
    key = payload[offset + 2:offset + 2 + key_length]
    if key_length != 32 or len(key) != key_length:
        raise ProbeError("Invalid product key in device-info response")
    try:
        product_key = key.decode("ascii")
    except UnicodeDecodeError as error:
        raise ProbeError("Invalid product key encoding") from error
    if product_key not in PROFILES:
        raise ProbeError("Refusing this device: product key has not been verified")
    return product_key


def flow_control(status: Status, value: int) -> bytes:
    if status.profile != MCP150:
        raise InvalidCommand("Flow-only control is supported only for the MCP150")
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 100:
        raise ProbeError("Flow must be an integer from 1 to 100; zero is not tested")
    writable = bytearray(status.raw[:WRITABLE_SIZE])
    writable[FLOW_OFFSET] = value
    # Standard P0 control: action, big-endian attribute flags, fixed values block.
    return b"\x01" + FLOW_MASK + writable


def supports_local_control(product_key: str | None, attributes: Mapping[str, ControlValue]) -> bool:
    profile = PROFILES.get(product_key) if product_key is not None else None
    return profile is not None and bool(attributes) and attributes.keys() <= profile.control_names


def normalize_attributes(
    attributes: Mapping[str, ControlValue], model: dict,
) -> dict[str, bool | int | str]:
    if not supports_local_control(model.get("product_key"), attributes):
        raise InvalidCommand("Unsupported model or local control attribute")
    specs = {attr["name"]: attr for attr in model["attrs"] if attr["type"] == "status_writable"}
    result = {}
    for name, value in attributes.items():
        if name not in specs:
            raise InvalidCommand(f"Missing model definition for {name}")
        attr = specs[name]
        if attr["data_type"] == "bool":
            if not isinstance(value, bool):
                raise InvalidCommand(f"{name} must be a boolean")
            result[name] = value
        elif attr["data_type"] == "enum":
            if not isinstance(value, str) or value not in attr["enum"]:
                raise InvalidCommand(f"Invalid option for {name}")
            result[name] = value
        elif attr["data_type"] == "uint8":
            limits = attr["uint_spec"]
            if (
                isinstance(value, bool) or not isinstance(value, (int, float))
                or not limits["min"] <= value <= limits["max"]
                or not math.isfinite(value) or int(value) != value
            ):
                raise InvalidCommand(f"{name} must be a whole number in {limits['min']}..{limits['max']}")
            result[name] = int(value)
        else:
            raise InvalidCommand(f"Unsupported local data type for {name}")
    return result


def control_payload(
    status: Status, attributes: Mapping[str, ControlValue], model: dict,
) -> bytes:
    normalized = normalize_attributes(attributes, model)
    profile = status.profile
    if model.get("product_key") != profile.product_key:
        raise ProbeError("Device product does not match the configured model; no command sent")
    specs = {attr["name"]: attr for attr in model["attrs"]}
    writable = bytearray(status.raw[:profile.writable_size])
    flags = 0
    for name, value in normalized.items():
        attr = specs[name]
        attr_id = attr["id"]
        if not isinstance(attr_id, int) or not 0 <= attr_id < profile.writable_attributes:
            raise InvalidCommand(f"Invalid attribute flag for {name}")
        flags |= 1 << attr_id
        position = attr["position"]
        offset = position["byte_offset"]
        if attr["data_type"] == "uint8":
            writable[offset] = int(value)
        else:
            if offset != 0:
                raise InvalidCommand(f"Unsupported bit-field location for {name}")
            encoded = attr["enum"].index(value) if attr["data_type"] == "enum" else int(value)
            shift = position["bit_offset"]
            mask = ((1 << position["len"]) - 1) << shift
            size = profile.bitfield_bytes
            bits = int.from_bytes(writable[:size], "big")
            writable[:size] = ((bits & ~mask) | (encoded << shift)).to_bytes(size, "big")
    return b"\x01" + flags.to_bytes(profile.flag_bytes, "big") + writable


def decode_status(status: Status, model: dict) -> dict[str, bool | int | str]:
    if model.get("product_key") != status.profile.product_key:
        raise ProbeError("Device status does not match the configured product")
    result = {}
    for attr in model["attrs"]:
        position = attr["position"]
        offset = position["byte_offset"]
        data_type = attr["data_type"]
        if data_type in ("bool", "enum"):
            size = status.profile.bitfield_bytes
            bits = int.from_bytes(status.raw[:size], "big") if offset == 0 else status.raw[offset]
            value = (bits >> position["bit_offset"]) & ((1 << position["len"]) - 1)
            if data_type == "bool":
                result[attr["name"]] = bool(value)
            elif value < len(attr["enum"]):
                result[attr["name"]] = attr["enum"][value]
            else:
                raise ProbeError(f"Invalid enum value for {attr['name']}")
        elif data_type == "uint8":
            result[attr["name"]] = status.raw[offset]
        else:
            raise ProbeError(f"Unsupported status data type for {attr['name']}")
    return result


def control_values(status: Status, model: dict) -> dict[str, bool | int | str]:
    return {
        name: value for name, value in decode_status(status, model).items()
        if name in status.profile.control_names
    }


def verify_unchanged(before: Status, after: Status) -> None:
    if before.device_id != after.device_id:
        raise ProbeError("Device ID changed; refusing to continue")
    # YMDData/HMSData occupy bytes 392..399 and can tick during the test.
    changed = [
        index for index in range(STATUS_SIZE)
        if index != FLOW_OFFSET and not 392 <= index < 400
        and before.raw[index] != after.raw[index]
    ]
    if changed:
        raise ProbeError(f"Non-Flow status changed at byte offsets {changed}")


class GizwitsLocalClient:
    def __init__(self, connection: socket.socket) -> None:
        self.connection = connection
        self.sequence = 0
        self.wifi_firmware = ""
        self.profile = MCP150
        self.device_id: str | None = None

    def _read_exact(self, count: int, deadline: float) -> bytes:
        result = bytearray()
        while len(result) < count:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Timed out waiting for a complete LAN response")
            self.connection.settimeout(remaining)
            chunk = self.connection.recv(count - len(result))
            if not chunk:
                raise ProbeError("Pump closed the TCP connection")
            result.extend(chunk)
        return bytes(result)

    def _read_frame(self, deadline: float) -> Frame:
        if self._read_exact(4, deadline) != HEADER:
            raise ProbeError("Invalid LAN frame header")
        length = 0
        for shift in range(0, 28, 7):
            byte = self._read_exact(1, deadline)[0]
            length |= (byte & 0x7F) << shift
            if not byte & 0x80:
                break
        else:
            raise ProbeError("Invalid LAN frame length encoding")
        if not 3 <= length <= MAX_FRAME_SIZE:
            raise ProbeError(f"Invalid LAN frame size: {length}")
        body = self._read_exact(length, deadline)
        return Frame(body[0], int.from_bytes(body[1:3], "big"), body[3:])

    def _response(
        self, command: int, sequence: int | None = None, deadline: float | None = None,
    ) -> Frame:
        if deadline is None:
            deadline = time.monotonic() + TIMEOUT
        for _ in range(32):
            frame = self._read_frame(deadline)
            if frame.command != command:
                continue
            if sequence is not None:
                if len(frame.payload) < 4:
                    raise ProbeError("Missing serial number in LAN response")
                if int.from_bytes(frame.payload[:4], "big") != sequence:
                    continue
            return frame
        raise ProbeError("Too many unrelated frames without the requested response")

    def _send(self, command: int, payload: bytes = b"") -> None:
        self.connection.settimeout(TIMEOUT)
        self.connection.sendall(encode_frame(command, payload))

    def login(self) -> None:
        self._send(0x0013)
        info = self._response(0x0014)
        self.profile = PROFILES[parse_product_key(info.payload)]
        self.wifi_firmware = info.payload[8:16].decode("ascii", errors="replace")
        if not self.profile.identified_status:
            # Legacy status frames omit the DID; bind this session to a UDP identity reply.
            self.device_id = discover_device(self.connection.getpeername()[0])
        self._send(0x0006)
        binding = self._response(0x0007).payload
        if len(binding) < 2 or int.from_bytes(binding[:2], "big") != len(binding) - 2:
            raise ProbeError("Invalid binding response")
        self._send(0x0008, binding)
        if self._response(0x0009).payload != b"\x00":
            raise ProbeError("Pump rejected the local login")

    def _send_data(self, payload: bytes) -> int:
        self.sequence += 1
        self._send(0x0093, self.sequence.to_bytes(4, "big") + payload)
        return self.sequence

    def read_status(self, deadline: float | None = None) -> Status:
        sequence = self._send_data(b"\x02")
        frame = self._response(0x0094, sequence, deadline)
        offset = 4
        if frame.flag == 1:
            if len(frame.payload) < 7:
                raise ProbeError("Truncated identified status response")
            did_length = int.from_bytes(frame.payload[4:6], "big")
            offset = 6 + did_length
            if not did_length or offset >= len(frame.payload):
                raise ProbeError("Invalid device ID length in status response")
            try:
                device_id = frame.payload[6:offset].decode("ascii")
            except UnicodeDecodeError as error:
                raise ProbeError("Invalid device ID in status response") from error
            if self.device_id is not None and device_id != self.device_id:
                raise ProbeError("Status and discovery identities do not match")
        elif frame.flag == 0 and not self.profile.identified_status and self.device_id:
            device_id = self.device_id
            if offset >= len(frame.payload):
                raise ProbeError("Truncated legacy status response")
        else:
            raise ProbeError("Expected an identified status response")
        if frame.payload[offset] != 3:
            raise ProbeError("Expected a status-read response (P0 action 0x03)")
        return Status(device_id, frame.payload[offset + 1:], self.profile)

    def write_flow(self, status: Status, value: int) -> None:
        self._send_data(flow_control(status, value))

    def write_attributes(
        self, status: Status, attributes: Mapping[str, ControlValue], model: dict,
    ) -> None:
        self._send_data(control_payload(status, attributes, model))

    def wait_for_attributes(
        self, attributes: Mapping[str, ControlValue], device_id: str, model: dict,
    ) -> Status:
        expected = normalize_attributes(attributes, model)
        deadline = time.monotonic() + TIMEOUT
        while True:
            status = self.read_status(deadline)
            if status.device_id != device_id:
                raise ProbeError("Device ID changed while verifying commands")
            current = control_values(status, model)
            if all(current[name] == value for name, value in expected.items()):
                return status
            if time.monotonic() + 0.2 >= deadline:
                actual = {name: current[name] for name in expected}
                raise ProbeError(f"Command read-back is {actual}, expected {expected}")
            time.sleep(0.2)

    def wait_for_flow(self, value: int, device_id: str) -> Status:
        deadline = time.monotonic() + TIMEOUT
        while True:
            status = self.read_status(deadline)
            if status.device_id != device_id:
                raise ProbeError("Device ID changed while verifying Flow")
            if status.flow == value:
                return status
            if time.monotonic() + 0.2 >= deadline:
                raise ProbeError(f"Flow read-back is {status.flow}, expected {value}")
            time.sleep(0.2)


MCP150Client = GizwitsLocalClient


@contextmanager
def connect(host: str) -> Generator[GizwitsLocalClient, None, None]:
    with socket.create_connection((host, PORT), timeout=TIMEOUT) as connection:
        client = GizwitsLocalClient(connection)
        client.login()
        yield client


def read_device(host: str, device_id: str) -> Status:
    with connect(host) as client:
        status = client.read_status()
        if status.device_id != device_id:
            raise ProbeError("Device ID does not match the configured pump")
        return status


def confirm_device(
    host: str, device_id: str, attributes: Mapping[str, ControlValue], model: dict,
) -> Status:
    with connect(host) as client:
        return client.wait_for_attributes(attributes, device_id, model)


def control_device(
    host: str, device_id: str, attributes: Mapping[str, ControlValue], model: dict,
) -> Status:
    normalized = normalize_attributes(attributes, model)
    with connect(host) as client:
        status = client.read_status()
        if status.device_id != device_id:
            raise ProbeError("Device ID does not match the configured pump; no command sent")
        client.write_attributes(status, normalized, model)
        client.wait_for_attributes(normalized, device_id, model)
    return confirm_device(host, device_id, normalized, model)
