import aiohttp
import asyncio
import json
import logging
from typing import Tuple

from .const import (
    GIZWITS_APP_ID,
    TIMEOUT,
    LOGGER,
    LAN_PORT,
    LAN_CONNECT_TIMEOUT,
    LAN_COMMAND_TIMEOUT,
    GIZWITS_API_URLS,
    DEFAULT_REGION,
)

GIZWITS_ERROR_CODES = {
    "1000000": "user_not_exist",
    "1000033": "invalid_password",
}


class GizwitsApi:
    """Class to handle communication with the Gizwits API."""

    def __init__(
        self,
        login_url,
        devices_url,
        device_data_url,
        control_url,
        token: str = None,
        refresh_token_url: str = None,
    ):
        self._token = token
        self._attribute_models = None
        self.login_url = login_url
        self.devices_url = devices_url
        self.device_data_url = device_data_url
        self.control_url = control_url
        self.refresh_token_url = refresh_token_url

    async def async_init_session(self):
        """Initialize the aiohttp session. Must be called before making API requests."""
        if hasattr(self, '_session') and self._session and not self._session.closed:
            return  # Session already active
        connector = aiohttp.TCPConnector(resolver=aiohttp.resolver.ThreadedResolver())
        self._session = aiohttp.ClientSession(connector=connector)

    async def _ensure_session(self):
        """Ensure the aiohttp session is open, recreating if needed."""
        if not hasattr(self, '_session') or self._session is None or self._session.closed:
            LOGGER.warning("API session was closed, recreating...")
            await self.async_init_session()

    async def __aenter__(self):
        await self.async_init_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def async_login(self, email: str, password: str) -> dict:
        """Login to Gizwits and return token info.

        Returns:
            dict with keys:
                - "token": user token (str or None)
                - "refresh_token": refresh token (str or None)
                - "expires_at": token expiry epoch (int or None)
                - "error": error code string (str or None)
        """
        data = {
            "appKey": GIZWITS_APP_ID,
            "data": {
                "account": email,
                "password": password,
                "lang": "en",
                "refreshToken": True,
            },
            "version": "1.0",
        }
        headers = {
            "X-Gizwits-Application-Id": GIZWITS_APP_ID,
            "Content-Type": "application/json",
        }
        try:
            async with self._session.post(
                self.login_url, json=data, headers=headers, timeout=TIMEOUT
            ) as response:
                response_text = await response.text()
                LOGGER.debug("Login response status: %s", response.status)
                LOGGER.debug("Login response headers: %s", response.headers)
                LOGGER.debug("Login response body: %s", response_text)

                try:
                    json_response = json.loads(response_text)
                    LOGGER.debug("Parsed JSON response: %s", json_response)

                    # Check for error codes first
                    if json_response.get("error", False):
                        error_code = json_response.get("code")
                        if error_code in GIZWITS_ERROR_CODES:
                            return {"token": None, "refresh_token": None, "expires_at": None, "error": GIZWITS_ERROR_CODES[error_code]}
                        return {"token": None, "refresh_token": None, "expires_at": None, "error": "unknown_error"}

                    # If no error, process the token
                    if json_response and isinstance(json_response, dict):
                        data = json_response.get("data", {})
                        LOGGER.debug("Data field content: %s", data)

                        if isinstance(data, dict):
                            token = data.get("userToken")
                            refresh_token = data.get("refreshToken")
                            expires_at = data.get("expiredAt")
                            if token:
                                LOGGER.debug(
                                    "Login successful, token expires at %s, refresh token present: %s",
                                    expires_at,
                                    refresh_token is not None,
                                )
                                return {
                                    "token": token,
                                    "refresh_token": refresh_token,
                                    "expires_at": expires_at,
                                    "error": None,
                                }
                            else:
                                LOGGER.error("No userToken in data: %s", data)
                        else:
                            LOGGER.error(
                                "Data is not a dictionary: %s, type: %s",
                                data,
                                type(data),
                            )

                    return {"token": None, "refresh_token": None, "expires_at": None, "error": "invalid_response"}

                except json.JSONDecodeError as e:
                    LOGGER.error(
                        "Failed to decode JSON response: %s\nResponse text: %s",
                        e,
                        response_text,
                    )
                    return {"token": None, "refresh_token": None, "expires_at": None, "error": "invalid_json"}

        except Exception as e:
            LOGGER.error("Exception during login to Gizwits API: %s", e)
            return {"token": None, "refresh_token": None, "expires_at": None, "error": "connection_error"}

    def set_token(self, token: str):
        """Set the user token for the API."""
        self._token = token

    async def async_refresh_token(self, refresh_token: str) -> dict:
        """Exchange a refresh token for a new user token.

        Returns:
            dict with keys:
                - "token": new user token (str or None)
                - "expires_at": new expiry epoch (int or None)
                - "error": error string (str or None)
        """
        if not self.refresh_token_url:
            return {"token": None, "expires_at": None, "error": "no_refresh_url"}

        await self._ensure_session()
        headers = {
            "X-Gizwits-Application-Id": GIZWITS_APP_ID,
            "X-Gizwits-User-token": self._token,
            "Content-Type": "application/json",
        }
        try:
            async with self._session.post(
                self.refresh_token_url,
                json={"refresh_token": refresh_token},
                headers=headers,
                timeout=TIMEOUT,
            ) as response:
                response_text = await response.text()
                LOGGER.debug("Refresh token response status: %s", response.status)
                LOGGER.debug("Refresh token response body: %s", response_text)

                if response.status == 200:
                    result = json.loads(response_text)
                    new_token = result.get("token")
                    expires_at = result.get("expire_at")
                    if new_token:
                        self._token = new_token
                        LOGGER.info("Token refreshed successfully, new expiry: %s", expires_at)
                        return {"token": new_token, "expires_at": expires_at, "error": None}

                return {"token": None, "expires_at": None, "error": "refresh_failed"}

        except Exception as e:
            LOGGER.error("Exception during token refresh: %s", e)
            return {"token": None, "expires_at": None, "error": "connection_error"}

    def add_attribute_models(self, attribute_models):
        """Add attribute models to the API instance."""
        self._attribute_models = attribute_models

    async def get_devices(self):
        """Get a list of bound devices."""
        await self._ensure_session()
        headers = {
            "X-Gizwits-User-token": self._token,
            "X-Gizwits-Application-Id": GIZWITS_APP_ID,
            "Accept": "application/json",
        }
        LOGGER.debug("Trying to get devices - Headers are: %s", headers)
        try:
            async with self._session.get(
                self.devices_url, headers=headers, timeout=TIMEOUT
            ) as response:
                result = await response.text()
                LOGGER.debug("Response from Gizwits API: %s", result)
                if response.status == 200:
                    return json.loads(result)
                else:
                    LOGGER.error(
                        "Failed to fetch devices from Gizwits API: %s", response.status
                    )
                    return None
        except Exception as e:
            LOGGER.error("Exception while fetching devices from Gizwits API: %s", e)
            return None

    async def get_device_data(self, device_id: str):
        """Get the latest attribute status values from a device."""
        await self._ensure_session()
        url = self.device_data_url.format(device_id=device_id)
        LOGGER.debug("Trying to get device data from URL: %s", url)
        headers = {
            "X-Gizwits-User-token": self._token,
            "X-Gizwits-Application-Id": GIZWITS_APP_ID,
            "Accept": "application/json",
        }
        try:
            async with self._session.get(
                url, headers=headers, timeout=TIMEOUT
            ) as response:
                result = await response.text()
                LOGGER.debug("Response from Gizwits API - Device Data: %s", result)
                if response.status == 200:
                    return json.loads(result)
                else:
                    LOGGER.error(
                        "Failed to fetch device data from Gizwits API: %s",
                        response.status,
                    )
                    return None
        except Exception as e:
            LOGGER.error("Exception while fetching device data from Gizwits API: %s", e)
            return None

    async def control_device(self, device_id: str, attributes: dict):
        """Send a command to change an attribute value on a device."""
        url = self.control_url.format(device_id=device_id)
        headers = {
            "X-Gizwits-User-token": self._token,
            "X-Gizwits-Application-Id": GIZWITS_APP_ID,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        data = {"attrs": attributes}
        LOGGER.debug(
            "Sending control command to Gizwits API - URL: %s, Data: %s, Headers: %s",
            url,
            data,
            headers,
        )

        await self._ensure_session()
        try:
            async with self._session.post(
                url, json=data, headers=headers, timeout=TIMEOUT
            ) as response:
                result = await response.text()
                LOGGER.debug(
                    "Response from Gizwits API to Control Command - Device Data: %s",
                    result,
                )
                if response.status == 200:
                    return json.loads(result)
                else:
                    LOGGER.error(
                        "Failed to send control command to Gizwits API: %s",
                        response.status,
                    )
                    return None
        except Exception as e:
            LOGGER.error(
                "Exception while sending control command to Gizwits API: %s", e
            )
            return None

    async def _read_gizwits_frame(self, reader):
        """Read a complete Gizwits LAN protocol frame from the stream.

        Frame format: [4-byte header 00000003] [LEB128 length] [length bytes of data]
        Returns the complete frame as bytes, or None on error/EOF.
        """
        try:
            # Read 4-byte header
            header = await asyncio.wait_for(reader.readexactly(4), timeout=LAN_COMMAND_TIMEOUT)
            if header != b"\x00\x00\x00\x03":
                LOGGER.warning("Unexpected Gizwits frame header: %s", header.hex())
                return None

            # Read LEB128-encoded length byte by byte
            length = 0
            shift = 0
            leb_bytes = b""
            while True:
                byte_data = await asyncio.wait_for(
                    reader.readexactly(1), timeout=LAN_COMMAND_TIMEOUT
                )
                leb_bytes += byte_data
                byte_val = byte_data[0]
                length |= (byte_val & 0x7F) << shift
                if (byte_val & 0x80) == 0:
                    break
                shift += 7
                if shift > 35:  # Safety limit for LEB128
                    LOGGER.error("LEB128 length exceeds safety limit")
                    return None

            # Read exactly 'length' bytes of frame data
            data = await asyncio.wait_for(
                reader.readexactly(length), timeout=LAN_COMMAND_TIMEOUT
            )

            frame = header + leb_bytes + data
            return frame
        except asyncio.TimeoutError:
            LOGGER.warning("Timeout reading Gizwits frame")
            return None
        except asyncio.IncompleteReadError as e:
            LOGGER.warning("Incomplete read on Gizwits frame: %s", e)
            return None
        except Exception as e:
            LOGGER.error("Error reading Gizwits frame: %s", e)
            return None

    def _get_frame_command(self, frame):
        """Extract the 2-byte command code from a Gizwits frame.

        Returns the command as an int (e.g. 0x0007, 0x0009, 0x0094), or None.
        """
        if not frame or len(frame) < 8:
            return None
        # Skip header (4 bytes), then decode LEB128 to find data start
        idx = 4
        while idx < len(frame) and (frame[idx] & 0x80):
            idx += 1
        idx += 1  # past the last LEB128 byte
        # data starts at idx: flag (1 byte) + command (2 bytes) + payload
        if idx + 3 <= len(frame):
            return int.from_bytes(frame[idx + 1 : idx + 3], byteorder="big")
        return None

    async def _read_response_for_command(self, reader, expected_cmd, max_frames=5):
        """Read Gizwits frames until one with the expected command is found.

        Discards frames with non-matching commands (e.g. heartbeats).
        Returns the matching frame, or None if not found within max_frames.
        """
        for attempt in range(max_frames):
            frame = await self._read_gizwits_frame(reader)
            if frame is None:
                return None
            cmd = self._get_frame_command(frame)
            LOGGER.debug(
                "Read frame #%d: cmd=0x%04x, data=%s",
                attempt + 1,
                cmd if cmd else 0,
                frame.hex(),
            )
            if cmd == expected_cmd:
                return frame
            LOGGER.debug(
                "Discarding frame with cmd 0x%04x (expected 0x%04x)",
                cmd if cmd else 0,
                expected_cmd,
            )
        LOGGER.warning(
            "Expected response 0x%04x not received after %d frames",
            expected_cmd,
            max_frames,
        )
        return None

    async def get_local_device_data(self, device_ip, product_key, device_id):
        """Poll the local device for its status."""
        # Load attribute model for the product
        attribute_model = self._attribute_models.get(product_key)
        if not attribute_model:
            LOGGER.error(
                "Invalid product key or missing attribute model for product key: %s",
                product_key,
            )
            return None

        is_push_based = attribute_model.get("push_based", False)
        LOGGER.debug(
            "Attempting to get local device data - IP: %s, Device ID: %s, push_based: %s",
            device_ip,
            device_id,
            is_push_based,
        )

        try:
            # Establish a connection with the local device (with timeout)
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(device_ip, LAN_PORT),
                timeout=LAN_CONNECT_TIMEOUT,
            )

            try:
                # Step 1: Request device info (cmd 0x0006 → response 0x0007)
                await self._send_local_command(writer, b"\x00\x06")
                frame = await self._read_response_for_command(reader, 0x0007)
                if frame is None:
                    LOGGER.error(
                        "No device info response (0x0007) from %s", device_id
                    )
                    return None
                binding_key = frame[-12:]
                LOGGER.debug("Binding key for %s: %s", device_id, binding_key.hex())

                # Step 2: Send binding/login (cmd 0x0008 → response 0x0009)
                await self._send_local_command(writer, b"\x00\x08", binding_key)
                frame = await self._read_response_for_command(reader, 0x0009)
                if frame is None:
                    LOGGER.error(
                        "No binding response (0x0009) from %s", device_id
                    )
                    return None
                LOGGER.debug("Binding ACK for %s: %s", device_id, frame.hex())

                if is_push_based:
                    # Push-based devices: send 0x0093 trigger, then wait for
                    # unsolicited 0x0091 push frames instead of 0x0094 response.
                    await self._send_local_command(
                        writer, b"\x00\x93", b"\x00\x00\x00\x02\x02"
                    )
                    response = await self._wait_for_push_status(reader, device_id)
                else:
                    # Standard poll: cmd 0x0093 → response 0x0094
                    await self._send_local_command(
                        writer, b"\x00\x93", b"\x00\x00\x00\x02\x02"
                    )
                    response = await self._read_response_for_command(reader, 0x0094)

                if response is None:
                    LOGGER.error(
                        "No status response from %s (push_based=%s)",
                        device_id,
                        is_push_based,
                    )
                    return None

                LOGGER.debug(
                    "Status response for %s (%d bytes): %s",
                    device_id,
                    len(response),
                    response[:40].hex() + "...",
                )

                # Process the response
                device_status_payload = self._extract_device_status_payload(response)
                if device_status_payload:
                    parsed_data = self._parse_device_status(
                        device_status_payload, attribute_model
                    )
                    LOGGER.debug(
                        "Successfully parsed local device data: %s", parsed_data
                    )
                    return {"did": device_id, "attr": parsed_data}
                else:
                    LOGGER.error(
                        "Failed to retrieve or parse device status from local device: %s",
                        device_id,
                    )
                    return None
            finally:
                # Ensure the writer is closed properly
                writer.close()
                await writer.wait_closed()

        except asyncio.TimeoutError:
            LOGGER.error(
                "Timeout error while communicating with local device: %s", device_ip
            )
            return None
        except ConnectionError as e:
            LOGGER.error("Connection error with local device %s: %s", device_ip, e)
            return None
        except Exception as e:
            LOGGER.error(
                "Unexpected error while communicating with local device %s: %s",
                device_ip,
                e,
            )
            return None

    async def _wait_for_push_status(self, reader, device_id, timeout=15):
        """Wait for a 0x0091 push status frame from a push-based device.

        Push-based devices (e.g. Maxspect) send unsolicited 0x0091 frames
        after the handshake + 0x0093 trigger. The first usable frame has a
        large payload (>50 bytes); smaller frames are mode-block-only pushes
        that we skip.
        """
        import time
        deadline = time.monotonic() + timeout
        LOGGER.debug("Waiting up to %ds for 0x0091 push from %s", timeout, device_id)

        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                frame = await asyncio.wait_for(
                    self._read_gizwits_frame(reader),
                    timeout=min(remaining, LAN_COMMAND_TIMEOUT),
                )
            except asyncio.TimeoutError:
                # Individual read timed out — keep waiting until deadline
                continue

            if frame is None:
                # Stream returned None (e.g. internal timeout inside
                # _read_gizwits_frame) — brief pause then retry
                await asyncio.sleep(0.1)
                continue

            cmd = self._get_frame_command(frame)
            LOGGER.debug(
                "Push frame from %s: cmd=0x%04x, %d bytes",
                device_id,
                cmd if cmd else 0,
                len(frame),
            )

            if cmd == 0x0091:
                # Accept only full-size status frames (>50 byte payload)
                payload = self._extract_device_status_payload(frame)
                if payload and len(payload) > 50:
                    LOGGER.debug(
                        "Accepted 0x0091 push (%d byte payload) from %s",
                        len(payload),
                        device_id,
                    )
                    return frame
                LOGGER.debug(
                    "Skipping short 0x0091 push (%d byte payload) from %s",
                    len(payload) if payload else 0,
                    device_id,
                )
            elif cmd == 0x000D:
                # Heartbeat response — ignore
                pass
            else:
                LOGGER.debug(
                    "Ignoring frame cmd 0x%04x while waiting for 0x0091",
                    cmd if cmd else 0,
                )

        LOGGER.warning(
            "No push status (0x0091) received from %s within %ds", device_id, timeout
        )
        return None

    @staticmethod
    async def validate_lan_device(device_ip):
        """Validate a device is reachable on LAN and return its binding info.

        Returns (True, binding_key_hex) on success, (False, error_msg) on failure.
        Used by the config flow for manual device addition.
        """
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(device_ip, LAN_PORT),
                timeout=LAN_CONNECT_TIMEOUT,
            )
            try:
                # Send device info request (0x0006)
                header = b"\x00\x00\x00\x03"
                flag = b"\x00"
                command = b"\x00\x06"
                length = len(flag + command).to_bytes(1, byteorder="big")
                packet = header + length + flag + command
                writer.write(packet)
                await writer.drain()

                # Read response — expect 0x0007
                resp_header = await asyncio.wait_for(
                    reader.readexactly(4), timeout=LAN_COMMAND_TIMEOUT
                )
                if resp_header != b"\x00\x00\x00\x03":
                    return False, "invalid_response"

                # Read LEB128 length
                resp_length = 0
                shift = 0
                while True:
                    byte_data = await asyncio.wait_for(
                        reader.readexactly(1), timeout=LAN_COMMAND_TIMEOUT
                    )
                    byte_val = byte_data[0]
                    resp_length |= (byte_val & 0x7F) << shift
                    if (byte_val & 0x80) == 0:
                        break
                    shift += 7
                    if shift > 35:
                        return False, "invalid_response"

                data = await asyncio.wait_for(
                    reader.readexactly(resp_length), timeout=LAN_COMMAND_TIMEOUT
                )
                # Binding key is the last 12 bytes of the response data
                binding_key = data[-12:]
                return True, binding_key.hex()
            finally:
                writer.close()
                await writer.wait_closed()
        except asyncio.TimeoutError:
            return False, "timeout"
        except ConnectionError:
            return False, "cannot_connect"
        except Exception as e:
            LOGGER.error("Error validating LAN device %s: %s", device_ip, e)
            return False, "cannot_connect"

    async def _send_local_command(self, writer, command, payload=b""):
        """Send a command to the local device."""
        try:
            header = b"\x00\x00\x00\x03"
            flag = b"\x00"
            length = len(flag + command + payload).to_bytes(1, byteorder="big")
            packet = header + length + flag + command + payload

            LOGGER.debug(
                "Sending local command: %s, Payload: %s", command.hex(), payload.hex()
            )
            writer.write(packet)
            await writer.drain()
            LOGGER.debug("Command sent successfully")
        except Exception as e:
            LOGGER.error("Error sending command to local device: %s", e)
            raise

    def _extract_device_status_payload(self, response):
        """Extract the device status payload from the response.

        Gizwits LAN frame structure after header + LEB128 length:
          [flag 1B][cmd 2B][sn 4B]
          [if flag & 0x01: did_len 2B + did (did_len B)]
          [action 1B]
          [device status payload ...]
        """
        try:
            # Find the pattern 0x00 0x00 0x00 0x03 in the response
            pattern = b"\x00\x00\x00\x03"
            start_index = response.find(pattern)
            if start_index == -1:
                LOGGER.error(
                    "Pattern 0x00 0x00 0x00 0x03 not found in the device response"
                )
                return None

            # Decode LEB128 length after the header
            leb128_bytes = response[start_index + len(pattern) :]
            length, leb128_length = self._decode_leb128(leb128_bytes)
            if length is None:
                LOGGER.error(
                    "Failed to decode LEB128 encoded payload length from device response"
                )
                return None

            # Data portion starts after header + LEB128 bytes
            data_start = start_index + len(pattern) + leb128_length
            data = response[data_start : data_start + length]

            if len(data) < 8:
                LOGGER.error("Frame data too short: %d bytes", len(data))
                return None

            # Parse frame fields: flag(1) + cmd(2) + sn(4)
            flag = data[0]
            offset = 7  # past flag(1) + cmd(2) + sn(4)

            # If flag bit 0 is set, the frame includes a DID (device ID) field
            if flag & 0x01:
                if offset + 2 > len(data):
                    LOGGER.error("Frame too short for DID length field")
                    return None
                did_len = int.from_bytes(data[offset : offset + 2], byteorder="big")
                offset += 2 + did_len
                LOGGER.debug(
                    "Frame has DID field (flag=0x%02x), DID length: %d, "
                    "skipping to data offset %d",
                    flag,
                    did_len,
                    offset,
                )

            # Next byte is the action byte
            if offset >= len(data):
                LOGGER.error("Frame too short for action byte")
                return None
            offset += 1  # skip the action byte

            # Everything from offset onward is the device status payload
            device_status_payload = data[offset:]

            if len(device_status_payload) == 0:
                LOGGER.error("Empty device status payload")
                return None

            LOGGER.debug(
                "Extracted device status payload (%d bytes, flag=0x%02x): %s",
                len(device_status_payload),
                flag,
                device_status_payload[:20].hex() + ("..." if len(device_status_payload) > 20 else ""),
            )
            return device_status_payload
        except Exception as e:
            LOGGER.error(f"Error in extracting device status payload: {e}")
            return None

    def _decode_leb128(self, data):
        """Decode LEB128 encoded data and return the value and number of bytes read."""
        result = 0
        shift = 0
        for i, byte in enumerate(data):
            result |= (byte & 0x7F) << shift
            if (byte & 0x80) == 0:
                return result, i + 1
            shift += 7
        return None, 0

    def _swap_endian(self, hex_str):
        """Swap the endianness of the first two bytes of the hex string."""
        if len(hex_str) >= 4:
            swapped = hex_str[2:4] + hex_str[0:2] + hex_str[4:]
            return swapped
        return hex_str

    def _parse_device_status(self, payload, attribute_model):
        """Parse the device status payload based on the attribute model."""
        status_data = {}
        try:
            # Convert bytes payload to a hexadecimal string if needed
            if isinstance(payload, bytes):
                payload = payload.hex()

            # Check if endianness swap is needed
            swap_needed = any(
                attr["position"]["byte_offset"] == 0
                and (attr["position"]["bit_offset"] + attr["position"]["len"] > 8)
                for attr in attribute_model["attrs"]
            )

            # Perform endianness swap only once if needed
            if swap_needed:
                payload = self._swap_endian(payload)

            # Convert hex payload to a byte array
            payload_bytes = bytes.fromhex(payload)

            # Process each attribute in the attribute model
            for attr in attribute_model["attrs"]:
                byte_offset = attr["position"]["byte_offset"]
                bit_offset = attr["position"]["bit_offset"]
                length = attr["position"]["len"]
                data_type = attr.get("data_type", "unknown")

                # Extract value based on data type
                if data_type == "bool":
                    value = bool(
                        self._extract_bits(
                            payload_bytes[byte_offset], bit_offset, length
                        )
                    )
                elif data_type == "enum":
                    enum_values = attr.get("enum", [])
                    enum_index = self._extract_bits(
                        payload_bytes[byte_offset], bit_offset, length
                    )
                    value = (
                        enum_values[enum_index]
                        if enum_index < len(enum_values)
                        else None
                    )
                elif data_type == "uint8":
                    value = payload_bytes[byte_offset]
                elif data_type == "binary":
                    value = payload_bytes[byte_offset : byte_offset + length].hex()

                status_data[attr["name"]] = value
        except Exception as e:
            LOGGER.error(f"Error parsing device status payload: {e}")

        return status_data

    def _extract_bits(self, byte_val, bit_offset, length):
        """Extract specific bits from a byte value."""
        mask = (1 << length) - 1
        return (byte_val >> bit_offset) & mask

    async def close(self):
        """Close the session."""
        if self._session:
            await self._session.close()
