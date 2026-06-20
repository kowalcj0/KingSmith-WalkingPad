# coordinator.py
import asyncio
import logging
from bleak import BleakClient
from bleak.backends.device import BLEDevice
from homeassistant.components import bluetooth
from homeassistant.components.bluetooth import async_ble_device_from_address
try:
    from bleak_retry_connector import establish_connection
    _HAS_RETRY_CONNECTOR = True
except ImportError:
    _HAS_RETRY_CONNECTOR = False
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from .const import (
    # UUID_TREADMILL_DATA,
    # UUID_CONTROL_POINT,
    # UUID_TREADMILL_STATUS,
    MODEL_UUIDS,
    CMD_CONTROL_REQUEST,
    CMD_START,
    CMD_STOP,
    CMD_FINISH,
    CONF_WATCH_HR_ENTITY,
    CONF_WATCH_STEPS_ENTITY,
    CONF_WATCH_CALORIES_ENTITY,
    cmd_set_speed,
    SPEED_MIN,
    SPEED_MAX,
    CMD_MC21_START,
    CMD_MC21_PAUSE,
    CMD_MC21_STOP,
    UUID_MC21_AUTH,
    CMD_MC21_AUTH,
    P1_FRAME_SYNC,
    P1_PKT_TYPE_DATA,
    P1_PKT_SIZE,
)

_LOGGER = logging.getLogger(__name__)


class WalkingPadCoordinator(DataUpdateCoordinator):
    def __init__(self, hass, config):
        super().__init__(hass, _LOGGER, name="WalkingPadCoordinator")
        self.mac = (config.get("mac_address") or config.get("mac") or "").upper()
        self.device_name = config.get("device_name")
        # self.model = config.get("model", "unknown")
        self.model = (config.get("model") or "WalkingPad").strip()
        model_config = MODEL_UUIDS.get(self.model, MODEL_UUIDS["WalkingPad"])
        self.uuids = model_config
        # Per-model speed limits — used by send_set_speed and the number entity
        self.speed_min: float = model_config.get("speed_min", SPEED_MIN)
        self.speed_max: float = model_config.get("speed_max", SPEED_MAX)
        for key in ("data", "control", "status"):
            if key not in self.uuids:
                raise ValueError(
                    f"Missing UUID '{key}' for model '{self.model}'"
                )

        # Known GATT model number strings for P1 detection
        self._p1_model_numbers = ("WLT8266M",)

        self.client = None
        self._retry_task = None
        self.data = {
            "speed": 0.0,
            "distance": 0,
            "energy": 0,
            "elapsed_time": 0,
            "training_status": "unknown",
            "training_status_raw": None,
            "countdown_number": None,
            # Watch session data (populated when use_watch=True)
            "watch_session_steps": 0,
            "watch_session_calories": 0,
            "watch_heart_rate": None,
        }
        self.control_state = None
        self.control_state_last = None

        # Watch integration — entity IDs loaded from options on setup
        self.watch_hr_entity: str | None = None
        self.watch_steps_entity: str | None = None
        self.watch_calories_entity: str | None = None

        # Runtime toggle — controlled by the switch entity
        self.use_watch: bool = False

        # Snapshot values captured at session start for delta calculation
        self._watch_steps_snapshot: float | None = None
        self._watch_calories_snapshot: float | None = None

        _LOGGER.info("WalkingPad model detected: %s", self.model)
    
    @property
    def is_connected(self):
        return bool(self.client and self.client.is_connected)

    @property
    def is_mc21(self) -> bool:
        """True for MC21 — skips Request Control before all commands."""
        return self.model == "WalkingPad MC21"

    async def async_start(self):
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                await asyncio.wait_for(self.async_connect(), timeout=15)
                _LOGGER.info("Connected to WalkingPad on attempt %d", attempt)
                return True
            except asyncio.TimeoutError:
                _LOGGER.warning("Connect attempt %d timed out", attempt)
            except Exception as exc:
                _LOGGER.warning("Connect attempt %d failed: %s", attempt, exc)
            await asyncio.sleep(5)

        _LOGGER.warning("All connect attempts failed; falling back to saved data and scheduling retries.")
        if not self._retry_task or self._retry_task.done():
            self._retry_task = self.hass.loop.create_task(self._retry_loop())
        return False


    async def async_connect(self):
        """Establish BLE connection and subscribe to notifications.
        Uses bleak_retry_connector.establish_connection() when available,
        which is HA's recommended approach for reliable BLE connections.
        Falls back to raw BleakClient.connect() if not available.
        """
        if self.is_connected:
            _LOGGER.info("Already connected to WalkingPad")
            return

        _LOGGER.debug("Connecting to WalkingPad at %s", self.mac)
        try:
            ble_device = async_ble_device_from_address(
                self.hass, self.mac, connectable=True
            )
            if not ble_device:
                raise RuntimeError(f"BLE device {self.mac} not found by HA Bluetooth stack")

            if _HAS_RETRY_CONNECTOR:
                # Preferred path — handles retries, stale connections, concurrent attempts
                _LOGGER.debug("Using bleak_retry_connector for reliable connection")
                self.client = await establish_connection(
                    BleakClient,
                    ble_device,
                    self.mac,
                    disconnected_callback=self._on_disconnected,
                )
            else:
                # Fallback — raw Bleak (works but less reliable on marginal BLE environments)
                _LOGGER.debug("bleak_retry_connector not available, using raw BleakClient")
                self.client = BleakClient(ble_device, disconnected_callback=self._on_disconnected)
                await self.client.connect()

        except Exception as exc:
            self.client = None
            _LOGGER.error("Failed to connect to device: %s", exc)
            raise

        # Detect P1 model from GATT model number BEFORE subscribing.
        # The P1 (WLT8266M) has no FTMS characteristics; subscribing to
        # them fails. We must know we're talking to a P1 first.
        await self._detect_p1_model()

        # Subscribe with staggered delays — KingSmith firmware silently drops
        # CCCD writes that arrive within ~30ms of each other.
        # KS Fit staggers 100/200/300ms between subscriptions; we mirror this.
        # Reference: walkingpad-controller docs/ftms-protocol-reference.md §2.2
        #
        # For P1: data on 0000fe01 (N+R), control on 0000fe02 (W+N+R)
        if self.is_p1:
            subscriptions = [
                (self.uuids["data"],    self._notification_handler,      "P1 Data",         0.10),
                (self.uuids["control"], self.handle_response,            "P1 Control",      0.20),
            ]
            for uuid, handler, label, delay in subscriptions:
                try:
                    await self.client.start_notify(uuid, handler)
                    _LOGGER.debug("Subscribed to %s", label)
                except Exception as exc:
                    _LOGGER.error("Failed to subscribe to %s: %s", label, exc)
                if delay:
                    await asyncio.sleep(delay)
            _LOGGER.info("Subscribed to P1 notifications")
        else:
            subscriptions = [
                (self.uuids["data"],    self._notification_handler,      "Treadmill Data",    0.10),
                (self.uuids["status"],  self._training_status_handler,   "Machine Status",    0.20),
                (self.uuids["control"], self.handle_response,            "Control Point",     0.0),
            ]
            for uuid, handler, label, delay in subscriptions:
                try:
                    await self.client.start_notify(uuid, handler)
                    _LOGGER.debug("Subscribed to %s", label)
                except Exception as exc:
                    _LOGGER.error("Failed to subscribe to %s: %s", label, exc)
                if delay:
                    await asyncio.sleep(delay)
            _LOGGER.info("Subscribed to all notifications")

        # MC21: send initial ODM pre-amble once at connect (mirrors KS Fit behaviour)
        # This is also sent before each command in send_start/pause/finish/set_speed
        if self.is_mc21:
            await self.send_mc21_auth()

    # async def async_stop(self):
    #     """Disconnect BLE client."""
    #     await self.disconnect()
    async def async_stop(self):
        """Disconnect BLE client and cancel retry loop."""
        if self._retry_task and not self._retry_task.done():
            self._retry_task.cancel()
            self._retry_task = None
        await self.disconnect()

    async def disconnect(self):
        if self.client and self.client.is_connected:
            try:
                await self.client.disconnect()
                _LOGGER.info("Disconnected from WalkingPad")
            except Exception as exc:
                _LOGGER.debug("Error during disconnect: %s", exc)
        self.client = None
    
    def _on_disconnected(self, client):
        """Called by Bleak when the BLE connection drops unexpectedly."""
        _LOGGER.warning("WalkingPad disconnected unexpectedly, scheduling retry")
        self.client = None
        if not self._retry_task or self._retry_task.done():
            self._retry_task = self.hass.loop.create_task(self._retry_loop())

    # ------------------------------------------------------------------
    # P1 model detection
    # ------------------------------------------------------------------

    @property
    def is_p1(self) -> bool:
        """True for WalkingPad P1 — proprietary protocol."""
        return self.model == "WalkingPad P1"

    async def _detect_p1_model(self) -> None:
        """Read the GATT Model Number String (0x2A24) to detect P1 variant.

        The P1 (WLT8266M / M30 platform) does not advertise a distinctive
        BLE name — it just says "WalkingPad", the same as the generic
        fallback model.  We read the Device Information service model
        number characteristic to distinguish it.
        """
        if not self.is_connected:
            return
        try:
            model_bytes = await self.client.read_gatt_char("00002a24-0000-1000-8000-00805f9b34fb")
            model_str = model_bytes.strip(b"\x00").decode("utf-8", errors="replace").strip()
            if model_str in self._p1_model_numbers:
                _LOGGER.info(
                    "Detected WalkingPad P1 via GATT model number: %s",
                    model_str,
                )
                self.model = "WalkingPad P1"
                self.uuids = MODEL_UUIDS["WalkingPad P1"]
                self.speed_min = self.uuids.get("speed_min", SPEED_MIN)
                self.speed_max = self.uuids.get("speed_max", SPEED_MAX)
        except Exception as exc:
            _LOGGER.debug("Could not read GATT model number: %s", exc)

    # ------------------------------------------------------------------
    # P1 packet parsing
    # ------------------------------------------------------------------

    def _parse_p1_packet(self, data: bytearray) -> dict | None:
        """Parse a WalkingPad P1 proprietary 20-byte notification packet.

        Packet structure (20 bytes, little-endian where applicable):
          byte 0: 0xF8  — frame sync
          byte 1: 0xA2  — data packet type
          byte 2: speed × 0.1 km/h
          byte 3: elapsed seconds (low byte)
          byte 4: distance in meters
          byte 5: 0x01  — constant
          byte 6-7: reserved / padding
          byte 8:  energy (calories)
          byte 9:  distance high byte (continuation)
          byte 10: energy high byte (continuation)
          byte 11: exercise flag (0x00=idle, 0x01=active)
          byte 12-13: elapsed time in seconds (little-endian uint16)
          byte 14-17: reserved
          byte 18: checksum (two's complement of sum of bytes 0-17)
          byte 19: 0xFD  — frame end
        """
        if len(data) < P1_PKT_SIZE:
            return None

        # Validate frame
        if data[0] != P1_FRAME_SYNC:
            return None
        if data[19] != 0xFD:
            return None

        # Verify checksum (two's complement of sum of bytes 0-17)
        pkt_sum = sum(data[0:18])
        expected_checksum = (-pkt_sum) & 0xFF
        if data[18] != expected_checksum:
            _LOGGER.debug(
                "P1 packet checksum mismatch: got 0x%02X, expected 0x%02X",
                data[18], expected_checksum,
            )
            return None

        speed_raw = data[2] / 10.0
        distance = data[4]
        energy = data[8]
        elapsed = int.from_bytes(data[12:14], byteorder="little")
        exercise_flag = data[11]

        # Derive training status from exercise flag
        # 0x01 = active/running, 0x00 = idle/stopped
        if exercise_flag == 0x01:
            training_status = "playing"
        else:
            training_status = "idle"

        return {
            "speed": round(speed_raw, 1),
            "distance": distance,
            "energy": energy,
            "elapsed_time": elapsed,
            "training_status": training_status,
            "training_status_raw": training_status,
        }

    def _is_p1_data_packet(self, data: bytearray) -> bool:
        """Quick check whether this is a P1 data packet."""
        return (
            len(data) >= P1_PKT_SIZE
            and data[0] == P1_FRAME_SYNC
            and data[1] == P1_PKT_TYPE_DATA
        )

    async def _send_p1_command(self, command_byte: int, extra_bytes: bytes = b"") -> None:
        """Send a proprietary P1 control command.

        Constructs a P1-style control packet and writes it to the
        data characteristic (0000fe01).  The packet format mirrors
        the data packet structure but uses a control-oriented
        command byte in position 2.

        Args:
            command_byte:  Command identifier (e.g. start, stop, speed).
            extra_bytes:   Additional payload bytes (e.g. speed value).
        """
        if not self.is_connected or not self.client:
            _LOGGER.debug("Cannot send P1 command: not connected")
            return
        try:
            # Build control packet
            #   byte 0:  0xF8  — sync
            #   byte 1:  0xA2  — packet type (same as data, P1 uses single char)
            #   byte 2:  command
            #   bytes 3-17: payload / zeroed
            #   byte 18: checksum
            #   byte 19: 0xFD  — frame end
            pkt = bytearray(P1_PKT_SIZE)
            pkt[0] = P1_FRAME_SYNC
            pkt[1] = P1_PKT_TYPE_DATA
            pkt[2] = command_byte
            # Copy extra bytes into the payload area
            for i, b in enumerate(extra_bytes):
                if i + 3 < P1_PKT_SIZE - 1:
                    pkt[3 + i] = b
            # Calculate checksum: two's complement of sum of bytes 0-17
            pkt_sum = sum(pkt[0:18])
            pkt[18] = (-pkt_sum) & 0xFF
            pkt[19] = 0xFD

            await self.client.write_gatt_char(
                self.uuids["control"],
                bytes(pkt),
                response=True,
            )
            _LOGGER.debug("P1 command sent: 0x%02X %s", command_byte, " ".join(f"{b:02X}" for b in extra_bytes))
        except Exception as exc:
            _LOGGER.warning("Failed to send P1 command: %s", exc)

    async def _send_p1_set_speed(self, kmh: float) -> None:
        """Send a P1 speed control command.

        Speed is encoded as an integer in bytes 3-4 (little-endian,
        representing speed × 100, i.e. 6.0 km/h → 600 → 0x02 0x58).
        """
        speed_val = int(round(kmh * 100))
        speed_bytes = speed_val.to_bytes(2, byteorder="little")
        await self._send_p1_command(0x06, speed_bytes)  # 0x06 = set speed for P1

    async def _send_p1_start(self) -> None:
        """Send a P1 start/resume command."""
        await self._send_p1_command(0x07)  # 0x07 = start/resume for P1

    async def _send_p1_stop(self) -> None:
        """Send a P1 stop command."""
        await self._send_p1_command(0x08, bytes([0x01]))  # 0x01 = stop

    async def _send_p1_pause(self) -> None:
        """Send a P1 pause command."""
        await self._send_p1_command(0x08, bytes([0x02]))  # 0x02 = pause

    def _notification_handler(self, sender, data: bytearray):
        """Parse treadmill data notifications."""
        _LOGGER.debug("Received treadmill data notification")

        # --- WalkingPad P1: proprietary 20-byte packet format ---
        if self.is_p1 and self._is_p1_data_packet(data):
            parsed = self._parse_p1_packet(data)
            if parsed is None:
                _LOGGER.debug("P1 packet parse failed, skipping")
                return
            prev_status = self.data.get("training_status")
            self.data.update(parsed)
            new_status = self.data["training_status"]

            # Watch session lifecycle — same logic as _training_status_handler
            if self.use_watch:
                if new_status == "playing" and prev_status != "playing":
                    self.start_watch_session()
                elif new_status == "idle" and prev_status not in ("idle", "unknown"):
                    self.reset_watch_session()
                self.update_watch_data()

            try:
                self.async_set_updated_data(self.data)
            except Exception:
                return
            return

        try:
            speed_raw = int.from_bytes(data[2:4], byteorder="little") / 100

            # MC11 sends 17-byte packets (distance = 3 bytes at b[4:7])
            # MC21 sends 14-byte packets (distance = 2 bytes at b[4:6])
            if len(data) >= 17:
                # MC11 format
                distance = int.from_bytes(data[4:7], byteorder="little")
                energy   = data[7]
                elapsed  = int.from_bytes(data[12:14], byteorder="little")
            elif len(data) >= 14:
                # MC21 format
                distance = int.from_bytes(data[4:6], byteorder="little")
                energy   = data[7]
                elapsed  = int.from_bytes(data[12:14], byteorder="little")
            else:
                _LOGGER.debug("Short data packet (%d bytes), skipping", len(data))
                return
        except Exception as exc:
            _LOGGER.debug("Failed parsing treadmill notification: %s", exc)
            return

        self.data.update({
            "speed": speed_raw,
            "distance": distance,
            "energy": energy,
            "elapsed_time": elapsed,
        })
        # Refresh watch data on every treadmill notification
        self.update_watch_data()
        try:
            self.async_set_updated_data(self.data)
        except Exception:
            pass


    # Control commands (no changes, just cleaned logs)
    async def send_mc21_auth(self) -> None:
        """Send the KingSmith ODM pre-amble before every Control Point command on MC21.

        KS Fit sends this before EACH command (start/stop/pause/speed), not just
        once at connect time. Without it, the MC21 returns CONTROL_NOT_PERMITTED.
        The payload is a static ODMSupplement.propertyList() frame — a device
        handshake/unlock sequence. Confirmed from HCI snoop: 41 identical writes
        in one session, one per Control Point operation.

        Reference: walkingpad-controller docs/ftms-protocol-reference.md §2.4
        """
        if not self.is_mc21 or not self.is_connected:
            return
        try:
            await self.client.write_gatt_char(
                UUID_MC21_AUTH,
                CMD_MC21_AUTH,
                response=True,
            )
            _LOGGER.debug("MC21 ODM pre-amble sent")
        except Exception as exc:
            _LOGGER.warning("Failed to send MC21 ODM pre-amble: %s", exc)

    async def send_control_request(self):
        """Send FTMS Request Control (0x00).

        MC11: required before every command — device grants control.
        MC21: always rejected (OPERATION_FAILED) but KS Fit sends it anyway
              and proceeds regardless. We mirror this behaviour.
        """
        if not self.is_connected:
            _LOGGER.debug("Cannot send CONTROL REQUEST, client not connected")
            return
        try:
            # await self.client.write_gatt_char(UUID_CONTROL_POINT, CMD_CONTROL_REQUEST, response=True)
            await self.client.write_gatt_char(
                self.uuids["control"],
                CMD_CONTROL_REQUEST,
                response=True
            )
        except Exception as e:
            # MC21 always rejects this — log at debug not warning
            _LOGGER.debug("CONTROL REQUEST response: %s (MC21 rejection is normal)", e)

    async def send_start(self):
        """Start the treadmill.
        P1:   Send proprietary start command via 0000fe01
        MC21: ODM preamble → Request Control (tolerate rejection) → START_OR_RESUME [0x07]
        MC11: Request Control → START_OR_RESUME [0x07, 0x01]
        """
        if not self.is_connected:
            _LOGGER.debug("Cannot send START, client not connected")
            return
        if self.is_p1:
            await self._send_p1_start()
            return
        # MC21: send ODM preamble before each command (KS Fit does this every time)
        if self.is_mc21:
            await self.send_mc21_auth()
        await self.send_control_request()  # MC21 will reject this, that's expected
        cmd = CMD_MC21_START if self.is_mc21 else CMD_START
        try:
            await self.client.write_gatt_char(self.uuids["control"], cmd, response=True)
            _LOGGER.info("Start command sent")
        except Exception as e:
            _LOGGER.debug("Error sending START: %s", e)

    async def send_pause(self):
        """Pause the treadmill.
        P1:   Send proprietary pause command via 0000fe01
        MC21: ODM preamble → Request Control (tolerate rejection) → STOP_OR_PAUSE [0x08, 0x02]
        MC11: Request Control → STOP_OR_PAUSE [0x08, 0x02]
        """
        if not self.is_connected:
            _LOGGER.debug("Cannot send PAUSE, client not connected")
            return
        if self.is_p1:
            await self._send_p1_pause()
            return
        if self.is_mc21:
            await self.send_mc21_auth()
        await self.send_control_request()
        cmd = CMD_MC21_PAUSE if self.is_mc21 else CMD_STOP
        try:
            await self.client.write_gatt_char(self.uuids["control"], cmd, response=True)
            _LOGGER.info("Pause command sent")
        except Exception as e:
            _LOGGER.debug("Error sending PAUSE: %s", e)

    async def send_finish(self):
        """Stop the treadmill completely.
        P1:   Send proprietary stop command via 0000fe01
        MC21: ODM preamble → Request Control (tolerate rejection) → STOP_OR_PAUSE [0x08, 0x01]
        MC11: Request Control → STOP_OR_PAUSE [0x08, 0x01]
        """
        if not self.is_connected:
            _LOGGER.debug("Cannot send FINISH, client not connected")
            return
        if self.is_p1:
            await self._send_p1_stop()
            return
        if self.is_mc21:
            await self.send_mc21_auth()
        await self.send_control_request()
        cmd = CMD_MC21_STOP if self.is_mc21 else CMD_FINISH
        try:
            await self.client.write_gatt_char(self.uuids["control"], cmd, response=True)
            _LOGGER.info("Finish command sent")
        except Exception as e:
            _LOGGER.debug("Error sending FINISH: %s", e)

    async def send_set_speed(self, kmh: float) -> None:
        """Set treadmill belt speed while running.
        Clamps to SPEED_MIN–SPEED_MAX and rounds to 0.1 km/h resolution.
        Only sends if treadmill is actively playing.
        """
        if not self.is_connected:
            _LOGGER.warning("Cannot set speed: device not connected")
            return
        if self.data.get("training_status") != "playing":
            _LOGGER.warning("Cannot set speed: treadmill is not actively playing")
            return
        if self.is_p1:
            # Clamp and round to 0.1 resolution
            kmh = round(max(self.speed_min, min(self.speed_max, kmh)), 1)
            await self._send_p1_set_speed(kmh)
            return
        # Clamp and round to 0.1 resolution
        kmh = round(max(self.speed_min, min(self.speed_max, kmh)), 1)
        # MC21: send ODM preamble before speed command (same as start/stop)
        if self.is_mc21:
            await self.send_mc21_auth()
        await self.send_control_request()  # MC21 tolerates rejection
        try:
            await self.client.write_gatt_char(
                self.uuids["control"],
                cmd_set_speed(kmh),
                response=True,
            )
            _LOGGER.debug("Speed set to %.1f km/h", kmh)
        except Exception as exc:
            _LOGGER.error("Failed to set speed: %s", exc)

    def handle_response(self, sender, data):
        """Parse control point responses and update state.

        P1 uses the same characteristic for data and control, so P1 data
        packets will also arrive here.  Skip them — they are handled by
        _notification_handler.
        """
        # Skip P1 data packets
        if self.is_p1 and self._is_p1_data_packet(data):
            return

        _LOGGER.debug("Control point response: %s", " ".join(f"{b:02X}" for b in data))
        try:
            if len(data) >= 2 and data[0] == 0x80:
                opcode = data[1]
                if opcode == 0x07:
                    self.control_state = "playing"
                elif opcode == 0x08:
                    tail = data[2:]
                    if 0x02 in tail:
                        self.control_state = "paused"
                    elif 0x01 in tail:
                        self.control_state = "idle"
                    else:
                        self.control_state = "paused"
                self.control_state_last = self.control_state
        except Exception as exc:
            _LOGGER.debug("Error parsing control response: %s", exc)

        try:
            self.async_set_updated_data(self.data)
        except Exception:
            pass
    
    def _training_status_handler(self, sender, data: bytearray):
        """Handle training status notifications.

        MC11 uses UUID 2AD3 (Training Status) with proprietary byte format.
        MC21 uses UUID 2ADA (Fitness Machine Status) with FTMS standard format.
        P1 uses the same characteristic as data — skip P1 data packets here
        since they are handled exclusively by _notification_handler.
        """
        # Skip P1 data packets — they are handled by _notification_handler
        if self.is_p1 and self._is_p1_data_packet(data):
            return

        hex_data = " ".join(f"{b:02X}" for b in data)
        _LOGGER.debug("Training Status raw data: %s", hex_data)

        status_str = "unknown"
        countdown_number = None

        if len(data) >= 2:
            # ---- MC21 / FTMS Fitness Machine Status (2ADA) format ----
            # b[0]=0x04 → Playing
            # b[0]=0x02, b[1]=0x02 → Paused
            # b[0]=0x02, b[1]=0x01 → Stopped/Idle
            # b[0]=0x05 → Speed update notification (not a state change)
            if data[0] == 0x04:
                status_str = "playing"
            elif data[0] == 0x02 and data[1] == 0x02:
                status_str = "stopping/paused"
            elif data[0] == 0x02 and data[1] == 0x01:
                status_str = "idle"
            elif data[0] == 0x05:
                # Speed notification from MC21 — not a state change, ignore for status
                # Speed is already read from the 2ACD Treadmill Data notifications
                _LOGGER.debug("MC21 speed notification: %s (handled by data handler)", hex_data)
                return

            # ---- MC11 / Proprietary Training Status (2AD3) format ----
            # Check for countdown messages
            elif data[0] == 0x03 and len(data) >= 3 and data[1] == 0x0E:
                countdown_map = {
                    0x33: "countdown 3",
                    0x32: "countdown 2",
                    0x31: "countdown 1",
                }
                status_str = countdown_map.get(data[2], f"mode unknown ({data[2]:02X})")
                if status_str.startswith("countdown"):
                    countdown_number = int(status_str.split()[1])
            # ---- MC11 format (b[0]=0x01) ----
            # Playing
            elif data[0] == 0x01 and data[1] == 0x0D:
                status_str = "playing"
            # Stopping / Paused
            elif data[0] == 0x01 and data[1] == 0x0F:
                status_str = "stopping/paused"
            # Idle
            elif data[0] == 0x01 and data[1] == 0x01:
                status_str = "idle"

            # ---- MC21 2AD3 format (b[0]=0x00) ----
            # Quick Start / Manual Mode → treat as playing
            elif data[0] == 0x00 and data[1] == 0x0D:
                status_str = "playing"
            # PostWorkout → treat as stopping/paused
            elif data[0] == 0x00 and data[1] == 0x0F:
                status_str = "stopping/paused"
            # Pre-Workout → treat as idle (ready state)
            elif data[0] == 0x00 and data[1] == 0x0E:
                status_str = "idle"
            # Idle
            elif data[0] == 0x00 and data[1] == 0x01:
                status_str = "idle"

            else:
                _LOGGER.debug(
                    "Unrecognised status bytes: %s — treating as unknown. "
                    "Please report this for model support.", hex_data
                )

        _LOGGER.debug("Training Status update: %s", status_str)

        self.data["training_status_raw"] = status_str
        self.data["countdown_number"] = countdown_number

        # Determine previous status before overwriting
        prev_status = self.data.get("training_status")

        # Normalize status for other components
        if "countdown" in status_str:
            self.data["training_status"] = "countdown"
        elif status_str == "playing":
            self.data["training_status"] = "playing"
        elif status_str == "stopping/paused":
            self.data["training_status"] = "paused"
        elif status_str == "idle":
            self.data["training_status"] = "idle"
        else:
            self.data["training_status"] = "unknown"

        new_status = self.data["training_status"]

        # Watch session lifecycle — snapshot on first "playing", reset on "idle"
        if self.use_watch:
            if new_status == "playing" and prev_status != "playing":
                self.start_watch_session()
            elif new_status == "idle" and prev_status not in ("idle", "unknown"):
                self.reset_watch_session()
            self.update_watch_data()

        try:
            self.async_set_updated_data(self.data)
        except Exception:
            pass

    
    # ------------------------------------------------------------------
    # Watch integration helpers
    # ------------------------------------------------------------------

    def load_watch_entities(self, options: dict) -> None:
        """Load watch entity IDs from config entry options. Called on setup and reload."""
        from .const import CONF_WATCH_HR_ENTITY, CONF_WATCH_STEPS_ENTITY, CONF_WATCH_CALORIES_ENTITY
        self.watch_hr_entity = options.get(CONF_WATCH_HR_ENTITY)
        self.watch_steps_entity = options.get(CONF_WATCH_STEPS_ENTITY)
        self.watch_calories_entity = options.get(CONF_WATCH_CALORIES_ENTITY)
        _LOGGER.debug(
            "Watch entities loaded — HR: %s  Steps: %s  Calories: %s",
            self.watch_hr_entity, self.watch_steps_entity, self.watch_calories_entity,
        )

    def _get_watch_value(self, entity_id: str | None) -> float | None:
        """Read a numeric state from a HA entity. Returns None if unavailable."""
        if not entity_id:
            return None
        state = self.hass.states.get(entity_id)
        if not state or state.state in (None, "unknown", "unavailable"):
            return None
        try:
            return float(state.state)
        except (ValueError, TypeError):
            return None

    def start_watch_session(self) -> None:
        """Snapshot watch cumulative values at the start of a session.
        The session sensors will show (current - snapshot), starting from 0.
        """
        self._watch_steps_snapshot = self._get_watch_value(self.watch_steps_entity)
        self._watch_calories_snapshot = self._get_watch_value(self.watch_calories_entity)
        # Reset session counters in data dict
        self.data["watch_session_steps"] = 0
        self.data["watch_session_calories"] = 0
        _LOGGER.info(
            "Watch session started — steps snapshot: %s  calories snapshot: %s",
            self._watch_steps_snapshot, self._watch_calories_snapshot,
        )

    def reset_watch_session(self) -> None:
        """Clear snapshot when session ends, ready for next session."""
        self._watch_steps_snapshot = None
        self._watch_calories_snapshot = None
        self.data["watch_session_steps"] = 0
        self.data["watch_session_calories"] = 0
        _LOGGER.debug("Watch session reset")

    def update_watch_data(self) -> None:
        """Pull latest watch values and compute session deltas.
        Called from _notification_handler and _training_status_handler when use_watch=True.
        """
        if not self.use_watch:
            return

        # Heart rate — always live passthrough, no delta needed
        self.data["watch_heart_rate"] = self._get_watch_value(self.watch_hr_entity)

        # Steps session delta
        current_steps = self._get_watch_value(self.watch_steps_entity)
        if current_steps is not None and self._watch_steps_snapshot is not None:
            delta = current_steps - self._watch_steps_snapshot
            self.data["watch_session_steps"] = max(0, delta)

        # Calories session delta
        current_calories = self._get_watch_value(self.watch_calories_entity)
        if current_calories is not None and self._watch_calories_snapshot is not None:
            delta = current_calories - self._watch_calories_snapshot
            self.data["watch_session_calories"] = max(0, delta)

    async def _retry_loop(self):
        """Background loop to retry connection until successful."""
        while not self.is_connected:
            _LOGGER.debug("Retry loop: attempting reconnect...")
            try:
                await self.async_connect()
                if self.is_connected:
                    _LOGGER.info("Successfully connected in retry loop")
                    break
            except Exception as e:
                _LOGGER.debug("Retry loop connection failed: %s", e)
            await asyncio.sleep(60)
