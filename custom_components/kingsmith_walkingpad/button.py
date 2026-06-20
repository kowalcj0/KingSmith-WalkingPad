from homeassistant.components.button import ButtonEntity
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.helpers.entity import DeviceInfo
from .const import DOMAIN, SPEED_STEP
import logging

_LOGGER = logging.getLogger(__name__)

async def async_setup_entry(hass, entry, async_add_entities):
    _LOGGER.info("WalkingPad buttons async_setup_entry called")
    coordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([
        WalkingPadConnectButton(coordinator),
        WalkingPadSpeedDownButton(coordinator),
        WalkingPadSpeedUpButton(coordinator),
    ])


class WalkingPadConnectButton(CoordinatorEntity, ButtonEntity):
    def __init__(self, coordinator):
        super().__init__(coordinator)
        self._attr_name = "WalkingPad Connect"
        self._attr_unique_id = f"{coordinator.mac}_connect"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, coordinator.mac)},
            name=coordinator.device_name,
            manufacturer="KingSmith",
            model=coordinator.model,
        )

    async def async_press(self):
        """Handle the button press: attempt to connect to the device."""
        if self.coordinator.is_connected:
            _LOGGER.info("Device already connected, no need to connect again.")
            return
        _LOGGER.info("Connect button pressed, attempting connection...")
        try:
            await self.coordinator.async_connect()
            _LOGGER.info("Connection attempt finished.")
        except Exception as e:
            _LOGGER.error("Connection attempt failed: %s", e)


class WalkingPadSpeedDownButton(CoordinatorEntity, ButtonEntity):
    """Button to decrease treadmill speed by one SPEED_STEP increment."""

    def __init__(self, coordinator):
        super().__init__(coordinator)
        self._attr_name = "WalkingPad Speed Down"
        self._attr_unique_id = f"{coordinator.mac}_speed_down"
        self._attr_icon = "mdi:minus"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, coordinator.mac)},
            name=coordinator.device_name,
            manufacturer="KingSmith",
            model=coordinator.model,
        )

    async def async_press(self):
        """Decrease the speed by one step, clamped to speed_min."""
        current = self.coordinator.data.get("speed", self.coordinator.speed_min)
        new_speed = round(max(self.coordinator.speed_min, current - SPEED_STEP), 1)
        _LOGGER.debug("Speed Down pressed: %.1f -> %.1f", current, new_speed)
        await self.coordinator.send_set_speed(new_speed)


class WalkingPadSpeedUpButton(CoordinatorEntity, ButtonEntity):
    """Button to increase treadmill speed by one SPEED_STEP increment."""

    def __init__(self, coordinator):
        super().__init__(coordinator)
        self._attr_name = "WalkingPad Speed Up"
        self._attr_unique_id = f"{coordinator.mac}_speed_up"
        self._attr_icon = "mdi:plus"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, coordinator.mac)},
            name=coordinator.device_name,
            manufacturer="KingSmith",
            model=coordinator.model,
        )

    async def async_press(self):
        """Increase the speed by one step, clamped to speed_max."""
        current = self.coordinator.data.get("speed", self.coordinator.speed_min)
        new_speed = round(min(self.coordinator.speed_max, current + SPEED_STEP), 1)
        _LOGGER.debug("Speed Up pressed: %.1f -> %.1f", current, new_speed)
        await self.coordinator.send_set_speed(new_speed)