# Maxspect XF330CE Gyre — Gizwits LAN Protocol

> Reverse-engineered on 2026-03-08 from live captures against a Maxspect XF330CE
> Double Edition dual gyre pump at **192.168.50.180**.
> Product key: `cd01d1f3ab2647ea9da51e045cf53d61`
> Device ID: `G3C1A0005968`

---

## 1. Network Overview

| Item | Value |
|------|-------|
| LAN control port | **TCP 12416** |
| Discovery port | UDP 12414 (broadcast `did=000000000000000000000000…`) |
| Discovery response | **None** — the Maxspect does NOT reply to Gizwits UDP discovery broadcasts |
| IP must be known | Yes — enter it manually in the integration config flow |

Because the device ignores UDP 12414, the integration uses a **manual IP** config
flow (`async_step_manual_device`) rather than auto-discovery.

---

## 2. Gizwits LAN Frame Format

Every frame on TCP 12416 follows this structure:

```
┌──────────────┬──────────────┬──────────────────────────────────────┐
│ Header (4 B) │ Length (LEB128) │ Data (length bytes)               │
│ 00 00 00 03  │ variable     │ flag(1) + cmd(2) + sn(4) + payload  │
└──────────────┴──────────────┴──────────────────────────────────────┘
```

- **Header**: always `00 00 00 03`.
- **Length**: [LEB128](https://en.wikipedia.org/wiki/LEB128) encoded unsigned
  integer giving the byte count of the Data section that follows.
- **Data** starts with:
  - `flag` (1 byte) — if bit 0 is set, a DID field follows the SN.
  - `cmd` (2 bytes, big-endian) — the command code.
  - `sn` (4 bytes) — sequence number.
  - `[did_len (2 B) + did (did_len B)]` — only present when `flag & 0x01`.
  - Remaining bytes are command-specific payload.

### LEB128 Decoding

```python
length = 0; shift = 0
for byte in stream:
    length |= (byte & 0x7F) << shift
    if (byte & 0x80) == 0: break
    shift += 7
```

---

## 3. Connection Sequence

### 3.1 Handshake

```
Client                                    Maxspect (192.168.50.180:12416)
  │                                           │
  │  ──► TCP connect ─────────────────────►   │
  │                                           │
  │  ──► 0x0006 (device info request) ───►    │
  │  ◄── 0x0007 (device info response) ◄──   │
  │       └─ last 12 bytes = binding key      │
  │          (ASCII "CATDCLOVTE" padded)       │
  │          hex: 00 0a 43 41 54 44 43 4c      │
  │               4f 56 54 45                  │
  │                                           │
  │  ──► 0x0008 (bind request) ──────────►    │
  │       └─ payload = binding key (12 B)     │
  │  ◄── 0x0009 (bind ACK) ◄─────────────    │
  │                                           │
```

### 3.2 Status Request (trigger)

```
  │  ──► 0x0093 (status request) ────────►    │
  │       └─ payload: 00 00 00 02 02          │
```

### 3.3 Response — Standard vs Push-Based

**Standard Gizwits devices** (e.g. Jebao pumps) respond with:
```
  │  ◄── 0x0094 (status response) ◄──────    │
```

**Maxspect XF330CE** returns an **empty** 0x0094 and instead sends unsolicited
**0x0091 push frames**:

```
  │  ◄── 0x0094 (empty — no status data) ◄─  │
  │  ◄── 0x0091 push (1044 B payload) ◄────  │  ← first full status
  │  ◄── 0x0091 push (783 B or 1044 B)  ◄──  │  ← periodic pushes
  │       ...repeats every ~1-3 seconds...    │
```

The integration detects push-based devices via the `"push_based": true` flag
in the model JSON; see `api.py → get_local_device_data()`.

### 3.4 Heartbeat

```
  │  ──► 0x000C (heartbeat request) ─────►   │
  │  ◄── 0x000D (heartbeat response) ◄───    │
  │       ...every ~30 seconds...             │
```

Heartbeat frames are received interleaved with push frames and are discarded
by `_wait_for_push_status()`.

---

## 4. Command Reference

| Cmd (hex) | Direction | Description |
|-----------|-----------|-------------|
| `0x0006` | Client → Device | Request device info |
| `0x0007` | Device → Client | Device info response (binding key in last 12 bytes) |
| `0x0008` | Client → Device | Bind/login with binding key |
| `0x0009` | Device → Client | Bind acknowledgement |
| `0x000C` | Client → Device | Heartbeat ping |
| `0x000D` | Device → Client | Heartbeat pong |
| `0x0090` | Client → Device | Write attribute (control command) |
| `0x0091` | Device → Client | **Push status** (unsolicited — Maxspect uses this) |
| `0x0093` | Client → Device | Request status (trigger) — payload: `00 00 00 02 02` |
| `0x0094` | Device → Client | Status response (Maxspect returns empty) |

---

## 5. Status Payload — Memory Map

After stripping the Gizwits frame envelope (header + LEB128 + flag + cmd + sn
+ optional DID + action byte), the remaining bytes are the **device status
payload**. For the Maxspect XF330CE, full-size payloads are **1044 bytes**.

### 5.1 Confirmed / Observed Attributes

All offsets are byte offsets into the status payload (0-based).

| Offset (dec) | Offset (hex) | Len | Name | Type | R/W | Description |
|---|---|---|---|---|---|---|
| 0–20 | 0x00–0x14 | 21 | _header_ | — | — | Preamble: `ff ff 00 00 00 24 01 1e` + zeros |
| 21 | 0x15 | 12 | DeviceID | binary | R | ASCII device ID: `G3C1A0005968` |
| 33 | 0x21 | 1 bit | **Power** | bool | **R/W** | `0x01` = ON, `0x00` = OFF. **CONFIRMED** via ON/OFF test. |
| 34 | 0x22 | 6 | Timestamp | binary | R | `YY MM DD HH MM SS` |
| 40 | 0x28 | 3 | _unknown_ | — | R | Observed: `08 01 01` — constant across dumps |
| 43 | 0x2B | 1 | MinSpeed | uint8 | R | Min speed %. Observed: `0x0A` = 10% |
| 44 | 0x2C | 1 | MaxSpeed | uint8 | R | Max speed %. Observed: `0x44` = 68% |
| 45 | 0x2D | 1 | Mode | uint8 | R | Operating mode. Observed: `0x05` (alternating?) |
| 46 | 0x2E | 1 | _unknown_ | — | R | Observed: `0x01` |
| 47 | 0x2F | 1 | Interval | uint8 | R | Wave interval in seconds. Observed: `0x0A` = 10s |
| 48–101 | 0x30–0x65 | 54 | _padding_ | — | R | All zeros in observed dumps |
| 102 | 0x66 | 49–53 | ModeBlock | binary | R | Mode/program definition block (§5.2) |
| 155–920 | 0x9B–0x398 | 766 | _reserved_ | — | R | Mostly zeros, purpose unknown |
| 921 | 0x399 | 2 | CH1_RPM | uint16 BE | R | Channel 1 RPM. Example: `0x0AD3` = 2771 RPM |
| 923 | 0x39B | 2 | CH1_Voltage | uint16 BE | R | Ch1 voltage × 100. `0x0968` = 2408 → 24.08 V |
| 925 | 0x39D | 1 | _unknown_ | — | R | Observed: `0x00` |
| 926 | 0x39E | 1 | CH1_Power | uint8 | R | Channel 1 power in watts. Observed: `0x21` = 33W |
| 927–929 | 0x39F–0x3A1 | 3 | _unknown_ | — | R | Observed: `0xAA 0x05 0x00` |
| 930 | 0x3A2 | 2 | CH2_RPM | uint16 BE | R | Channel 2 RPM. Example: `0x0A80` = 2688 RPM |
| 932 | 0x3A4 | 2 | CH2_Voltage | uint16 BE | R | Ch2 voltage × 100. `0x0956` = 2390 → 23.90 V |
| 934 | 0x3A6 | 1 | _unknown_ | — | R | Observed: `0x00` |
| 935 | 0x3A7 | 1 | CH2_Power | uint8 | R | Channel 2 power in watts. Observed: `0x1B` = 27W |
| 936–937 | 0x3A8–0x3A9 | 2 | _unknown_ | — | R | Observed: `0xBB 0x0B` |
| 938 | 0x3AA | 1 | _unknown_ | — | R | Observed: `0x0B` |
| 939–1043 | 0x3AB–0x413 | 105 | _tail_ | — | R | All zeros |

### 5.2 Mode Block (offset 102 / 0x66)

The mode block starts with a length byte followed by mode/program definitions.
Observed structure:

```
Byte 0:  length of the block (e.g. 0x31 = 49 bytes)
Byte 1:  program count or sub-mode selector (0x04)
Byte 2+: pairs of (speed, interval?) definitions for each program step
```

Example from dump #1 (ON, mode 5):
```
  0066: 31 04 01 00 00 04 01 05 06 01 05 02 06 00 04 01
  0076: 08 06 01 08 03 0a 00 03 01 0a 4a 4a 00 0a 44 44
  0086: 03 01 0a 4a 4a 00 0a 44 44 04 12 00 04 01 08 06
  0096: 01 08 00 00 00 00 00 00 00 00
```

The exact encoding of each program step is not yet fully decoded. The block
varies when the operating mode is changed via the app.

### 5.3 Short Push Frames (783 bytes)

Some 0x0091 pushes contain only **783 bytes** of payload. These appear to
carry just the mode block and lack the sensor telemetry section (offsets
921+). The integration filters these out in `_wait_for_push_status()` by
requiring `len(payload) > 50` before accepting a frame (the full 1044-byte
payloads always qualify). Short frames are:

- **Dumps 15–19**: Captured during ON→OFF transition
- **Dumps 22–26**: Subsequent mode-block-only pushes

---

## 6. ON / OFF Test Results

Performed a controlled power toggle test via the Maxspect app:

| Dump Range | State | Payload Size | Power byte (0x21) |
|---|---|---|---|
| 1–14 | ON (steady state) | 1044 B | `0x01` |
| 15–19 | Transition (ON→OFF) | 783 B | _not present (short frame)_ |
| 20–21 | OFF (steady state) | 1044 B | `0x00` |
| 22–26 | OFF (continued) | 783 B | _not present (short frame)_ |

**Key finding**: offset 33 (0x21) is the power state. `0x01` = ON, `0x00` = OFF.

### Sensor changes during ON → OFF:

| Attribute | ON (dump #1) | OFF (dump #20) | Notes |
|---|---|---|---|
| CH1_RPM | `0x0AD3` (2771) | `0x07EB` (2027) | RPM drops (pump spinning down) |
| CH1_Voltage | `0x0968` (24.08V) | `0x096A` (24.10V) | Stable |
| CH1_Power | `0x21` (33W) | `0x14` (20W) | Power draw drops |
| CH2_RPM | `0x0A80` (2688) | `0x0605` (1541) | RPM drops |
| CH2_Voltage | `0x0956` (23.90V) | `0x095A` (23.94V) | Stable |
| CH2_Power | `0x1B` (27W) | `0x0D` (13W) | Power draw drops |

---

## 7. Binding Key

The binding key is extracted from the last 12 bytes of the 0x0007 response:

```
Hex:   00 0a 43 41 54 44 43 4c 4f 56 54 45
        └─ length prefix (0x000A = 10 bytes of ASCII)
ASCII:       C  A  T  D  C  L  O  V  T  E
```

The full 12 bytes are echoed back as the 0x0008 bind payload.

---

## 8. Writing Attributes (Control)

To write an attribute (e.g. toggle power), send cmd **0x0090** with the
attribute data in the payload. The payload format mirrors the status layout —
set the target byte(s) and the device applies the change.

Currently only **Power** (offset 33) is confirmed writable. The other settings
(MinSpeed, MaxSpeed, Mode, Interval) are marked `status_readonly` in the model
file and need individual write tests before enabling.

### Control payload example — Power OFF:

```
Offset 33 (0x21) in the status map → set to 0x00
Payload: [action byte 0x01] [attr data with byte 33 = 0x00]
```

> ⚠️ **The exact 0x0090 write payload format has not been fully tested yet.**
> The integration currently relies on cloud `POST /app/control/{did}` for write
> commands. LAN write support requires further protocol analysis.

---

## 9. Integration Architecture

### 9.1 Model File

[models/cd01d1f3ab2647ea9da51e045cf53d61.json](custom_components/jebao_aqua/models/cd01d1f3ab2647ea9da51e045cf53d61.json)

Key properties:
- `product_key`: `cd01d1f3ab2647ea9da51e045cf53d61` (filename must match)
- `push_based`: `true` — triggers the 0x0091 push path in `api.py`
- `attrs[]`: Each attribute defines `position.byte_offset`, `data_type`, and
  `type` (`status_writable` or `status_readonly`)

### 9.2 Push-Based Polling Flow (`api.py`)

```
get_local_device_data()
  ├── if attribute_model["push_based"]:
  │     ├── TCP connect → 0x0006/0x0007 → 0x0008/0x0009 (handshake)
  │     ├── 0x0093 trigger
  │     └── _wait_for_push_status()
  │           ├── Loop up to 15s reading frames
  │           ├── Accept 0x0091 with payload > 50 bytes
  │           ├── Skip heartbeats (0x000D) and short 0x0091s
  │           └── On None from _read_gizwits_frame: sleep(0.1) + continue
  │
  └── else (standard):
        ├── TCP connect → handshake
        ├── 0x0093 → wait for 0x0094 response
        └── Parse status payload
```

### 9.3 LAN-Only Config Entry (`__init__.py`)

When added via "Add device by IP", the config entry has `lan_only: True`:
- No cloud token or region required
- Device inventory populated from `entry.data["devices"]`
- LAN failure counter never disables retries (always retries LAN)
- Cloud fallback is skipped entirely
- First refresh failure is tolerated (warning only, not `ConfigEntryNotReady`)

### 9.4 Config Flow (`config_flow.py`)

```
async_step_user()           → Menu: "Cloud Login" | "Add device by IP"
async_step_cloud_login()    → Country + email + password → discovery → device_setup
async_step_manual_device()  → IP + device type dropdown + optional name
                              → validate_lan_device() → create entry (lan_only=True)
```

---

## 10. What Still Needs Testing

| Item | Status | Notes |
|---|---|---|
| Power ON/OFF toggle | ✅ Confirmed | Offset 33, bool, bit 0 |
| LAN write via 0x0090 | ❌ Not tested | Need to capture app performing a LAN write |
| MinSpeed write | ❌ Not tested | Change min speed in app and compare dumps |
| MaxSpeed write | ❌ Not tested | Same |
| Mode selection | ❌ Not tested | Mode byte at offset 45 — enumerate all modes |
| Interval write | ❌ Not tested | Change interval in app and compare |
| Feeding mode | ❌ Not tested | Activate feeding mode and capture the payload |
| Mode block decode | ❌ Partial | First byte is length; rest needs more samples |
| Unknown bytes at 0x28 | ❌ Not mapped | `08 01 01` — constant? firmware version? |
| Mystery `0xAA`/`0xBB` markers | ❌ Not mapped | At offsets 927, 936 — channel delimiters? |
| UDP discovery | ❌ Doesn't respond | May need a different discovery packet |

---

## 11. Reproduction: Capture Script

To capture more frames for analysis, connect and dump push frames:

```python
import asyncio, time

async def capture(ip, port=12416, count=10, output_dir="dumps"):
    reader, writer = await asyncio.open_connection(ip, port)

    async def read_frame():
        header = await asyncio.wait_for(reader.readexactly(4), timeout=5)
        assert header == b"\x00\x00\x00\x03"
        length, shift = 0, 0
        while True:
            b = (await reader.readexactly(1))[0]
            length |= (b & 0x7F) << shift
            if not (b & 0x80): break
            shift += 7
        data = await reader.readexactly(length)
        return header + bytes([length & 0xFF]) + data  # simplified

    async def send(cmd, payload=b""):
        pkt = b"\x00\x00\x00\x03" + bytes([len(payload)+3]) + b"\x00" + cmd + payload
        writer.write(pkt)
        await writer.drain()

    # Handshake
    await send(b"\x00\x06")
    f = await read_frame(); key = f[-12:]
    await send(b"\x00\x08", key)
    await read_frame()  # bind ack

    # Trigger push
    await send(b"\x00\x93", b"\x00\x00\x00\x02\x02")

    # Collect pushes
    import os; os.makedirs(output_dir, exist_ok=True)
    for i in range(count):
        frame = await read_frame()
        # ... extract payload, write to file ...

    writer.close()

asyncio.run(capture("192.168.50.180"))
```

> Note: This is a simplified skeleton. The actual capture script used
> for the initial analysis handled LEB128 properly and wrote formatted
> hex dumps to the `dumps/` directory.

---

## 12. Dump File Index

All dumps are in the `dumps/` directory. Each is a formatted hex dump of the
**status payload only** (after stripping the Gizwits frame envelope).

| File | Timestamp | Size | State | Notes |
|---|---|---|---|---|
| dump_001.txt – dump_014.txt | 16:58–18:15 | 1044 B | ON steady | Full status with sensors |
| dump_015.txt – dump_019.txt | 18:17 | 783 B | ON→OFF transition | Mode block only |
| dump_020.txt – dump_021.txt | 18:18 | 1044 B | OFF steady | Full status, power=0x00 |
| dump_022.txt – dump_026.txt | 18:19–18:20 | 783 B | OFF continued | Mode block only |
