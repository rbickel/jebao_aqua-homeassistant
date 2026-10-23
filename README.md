# Jebao Aqua Aquarium Pumps for Home Assistant

[![hacs_badge](https://img.shields.io/badge/HACS-Custom-41BDF5.svg)](https://github.com/hacs/integration)

Home Assistant custom integration for [Jebao](https://www.jebao-aqua.com/) Wi-Fi enabled aquarium wavemakers, pumps, and dosing pumps — with local LAN polling, verified MCP150/MDP10000 local control, and Gizwits cloud fallback.

## How It Works

Jebao Wi-Fi devices use the **Gizwits IoT platform** under the hood.

| Capability | How it works |
|---|---|
| Device discovery | Bound to your Jebao Aqua / Gizwits account via cloud API |
| State polling | Local LAN (TCP port 12416), with cloud fallback on failure |
| Commands (on/off, mode, flow) | Verified MCP150/MDP10000: local LAN first, then cloud fallback; other models: cloud |
| Availability | Live device-identity ping; fresh cloud online status when LAN is unreachable |

**Cloud credentials are still required** for setup/startup device discovery and cloud fallback. Once set up, supported pump commands and normal status updates use the LAN.

## Supported Devices

> [!IMPORTANT]
> As of late 2024, some model series ship in two hardware variants. The newer variant adds Bluetooth (BLE) alongside Wi-Fi and uses an ESP32C3 microcontroller instead of the legacy ESP8266. **The MCP150 product key listed below has verified LAN support; other WiFi+BLE variants remain unverified and are not generally supported.**

| Status | Device | Type |
|---|---|---|
| ✅ Confirmed | Jebao MCP Series Crossflow Wavemaker | Pump |
| ✅ Confirmed | MCP150 ESP32-C3 (`54114ccdac1e41c0bb17e222887c07ba`), including local commands | Pump |
| ✅ Confirmed | MDP10000 ESP8266 (`02039876751049deb404d1d89221ec4b`), including local commands | Return pump |
| ✅ Confirmed | Jebao MLW Series Wavemaker | Pump |
| ✅ Confirmed | Jebao Smart Doser 3.1 | Dosing pump |
| ✅ Confirmed | Jebao MD 4.4 Dosing Pump | Dosing pump |
| ✅ Confirmed | Jebao MD 2.4 Dosing Pump | Dosing pump |
| ⚠️ Untested | Jebao SLW Series Wavemaker | Pump |
| ⚠️ Untested | Jebao EP Series Pump | Pump |
| ❌ Unsupported | Other unverified WiFi+Bluetooth (BLE) Jebao variants | — |

**Legend:** ✅ tested and confirmed working — ⚠️ implemented but untested — ❌ not yet supported

## Features

- **Local LAN polling** — device state is read directly over your network every 2 s; no cloud polling during normal operation
- **Local-first MCP150/MDP10000 control** — verified commands use LAN, with automatic Gizwits cloud fallback; other models retain cloud control
- **Live availability** — cached state alone cannot keep a disconnected device marked available
- **Multi-region support** — EU, US, and CN Gizwits endpoints automatically selected by country
- **Pump control** — switch entities for power and other boolean states
- **Flow & frequency** — numeric slider entities for fine-grained speed control
- **Mode selection** — select entities for operating modes (wave, constant, pulse, etc.)
- **Fault monitoring** — sensor and binary sensor entities expose error/fault states reported by the device
- **No scheduling lock-in** — native app schedules are not imported; use Home Assistant automations instead

### MCP150 local commands

Local-first control is enabled automatically when a LAN IP is known and the
device product key is `54114ccdac1e41c0bb17e222887c07ba`. It was tested on an
MCP150 with ESP32-C3 Wi-Fi firmware `04D30Q29`.

All nine existing controls are supported:

- **Switches:** power, pulse/tide, feeding, and timer.
- **Selections:** all four modes and all three linkage options.
- **Numbers:** flow, frequency, and feeding time.

Commands verify the device identity, preserve unselected values, and confirm the
requested settings over a separate TCP connection. Reads and writes are
serialized per device. A timeout, protocol error, or unconfirmed change triggers
cloud fallback; invalid values are rejected rather than retried. If cloud
control also fails, Home Assistant reports a service error.

**Native timer:** this pump's own timer can immediately overwrite manual settings.
For example, Flow reverted to its scheduled value with `TimerON` enabled, but
persisted when the timer was disabled. Turn off the device's Timer switch before
manual control when needed. Normal integration commands never disable it
automatically, and cloud fallback cannot be assumed to override a native schedule.

Devices without a LAN IP and other product keys continue using cloud commands.
This does not provide fully offline startup or add local dosing/schedule writes.
See [the development guide](DEV.md#standalone-mcp150-lan-control-experiment)
for the reversible hardware probe.

### MDP10000 local commands

The MDP10000 product key `02039876751049deb404d1d89221ec4b` was verified with
ESP8266 Wi-Fi firmware `04020B28`. All four existing HA controls support local
commands with cloud fallback: power, feeding, motor speed, and feed duration.
Each was tested with reversible changes and independent confirmation.

Its legacy status frames omit the device ID, so a validated UDP identity reply
binds each session to the correct controller before any write. Allow **TCP
12416 and UDP 12414** between HA and the device. Unverified product keys and
unexposed native schedule settings are not enabled for local writes.

### Availability

Each configured device is checked every **15 seconds** with a read-only Gizwits
UDP request, validating the source address, protocol response and device ID.
This is an application-level identity ping, not an ICMP echo or a port-open check.

If LAN does not answer, one fresh cloud device-inventory request checks
`is_online` for the affected devices. A local reply **or** a fresh cloud online
indication counts as available. Two failed checks mark its entities unavailable,
typically within 30–40 seconds depending on network timeouts.

Last-known values are retained for recovery but are **not** liveness proof.
Cached `/latest` cloud attributes and old inventory flags cannot refresh
availability; health evidence also expires after 45 seconds. Successful direct
LAN reads/commands and subsequent identity pings restore availability. A local
ping also clears LAN backoff so status polling can recover without cloud access.
Health checks run independently of slow polling and are stopped on unload.

This measures **controller reachability**, not water flow or motor health.
A powered-off motor can still have an available Wi-Fi controller; use the
device-reported fault entities to monitor motor problems.

## Installation

### HACS (Recommended)

1. Open HACS in Home Assistant
2. Go to **Integrations** → **⋮** → **Custom repositories**
3. Add this repository URL and select **Integration** as the category
4. Search for "Jebao Aqua" and install
5. Restart Home Assistant

### Manual

1. Copy the `custom_components/jebao_aqua` folder to your Home Assistant `config/custom_components/` directory
2. Restart Home Assistant

## Configuration

1. Make sure your pumps are already set up in the **Jebao Aqua** app and connected to a Wi-Fi network reachable from your Home Assistant host
2. Note the local IP address of each pump (open the pump in the app → **Settings** icon → **Device Information**)
3. Go to **Settings** → **Devices & Services** → **Add Integration**
4. Search for **Jebao Aqua Aquarium Pump**
5. Enter your Jebao Aqua app credentials (email, password, and region/country)
6. The integration will discover all devices linked to your account and prompt you to enter the local IP for each

## Entities

The entities created for each device depend on its model definition. Common entities include:

| Entity | Type | Description |
|--------|------|-------------|
| Power | `switch` | Turn the pump on or off |
| Flow rate | `number` | Pump flow speed (%) |
| Frequency | `number` | Wave frequency setting |
| Mode | `select` | Operating mode (constant, wave, pulse, …) |
| Status / fault | `sensor` / `binary_sensor` | Device health and error indicators |

## Troubleshooting

Enable debug logging to capture detailed output:

```yaml
logger:
  default: warning
  logs:
    custom_components.jebao_aqua: debug
```

Restart Home Assistant and check **Settings → System → Logs**. If you encounter a bug or your device is not working, please [open an issue](../../issues/new) and include the debug log.

## Contributing & Adding New Devices

If you own a device listed as ⚠️ untested, please try the integration and open an issue with:
- Your device model and the product key shown in the debug log
- Which entities appeared and whether they responded correctly

Code contributions are welcome. Key files:

- API & cloud: [`api.py`](custom_components/jebao_aqua/api.py)
- Entity platforms: [`switch.py`](custom_components/jebao_aqua/switch.py), [`select.py`](custom_components/jebao_aqua/select.py), [`number.py`](custom_components/jebao_aqua/number.py), [`binary_sensor.py`](custom_components/jebao_aqua/binary_sensor.py)
- Device models: [`models/`](custom_components/jebao_aqua/models)

---

MIT
