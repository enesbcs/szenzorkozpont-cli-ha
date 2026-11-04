"""SzenzorKozpont — "Sync now" button: forced cycle now.

Pull → command exec → report on single press, no wait.
"""
from __future__ import annotations

from homeassistant.components.button import ButtonEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .backends import BACKEND_SZK
from .const import DOMAIN


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    coordinator = hass.data[DOMAIN][entry.entry_id]
    entities = [SzkSyncButton(coordinator, entry)]
    # OTA belongs to SZKP native protocol only (on foreign backend
    # updates go via HA manager).
    if getattr(coordinator, "backend_kind", BACKEND_SZK) == BACKEND_SZK:
        entities.append(SzkOtaButton(coordinator, entry))
    async_add_entities(entities)


class SzkSyncButton(ButtonEntity):
    _attr_has_entity_name = True
    _attr_translation_key = "sync_now"
    _attr_icon = "mdi:sync"

    def __init__(self, coordinator, entry: ConfigEntry) -> None:
        self.coordinator = coordinator
        self._attr_unique_id = f"{entry.entry_id}_sync_now"
        self._attr_device_info = {
            "identifiers": {(DOMAIN, entry.entry_id)},
            "name": "SzenzorKozpont",
            "manufacturer": "Bitekmindenhol",
            "model": "SZKP HA kliens",
        }

    async def async_press(self) -> None:
        await self.coordinator.async_full_refresh()


class SzkOtaButton(ButtonEntity):
    """Instant OTA update: check now + download + install."""

    _attr_has_entity_name = True
    _attr_translation_key = "ota_update_now"
    _attr_icon = "mdi:download"

    def __init__(self, coordinator, entry: ConfigEntry) -> None:
        self.coordinator = coordinator
        self._attr_unique_id = f"{entry.entry_id}_ota_update"
        self._attr_device_info = {
            "identifiers": {(DOMAIN, entry.entry_id)},
            "name": "SzenzorKozpont",
            "manufacturer": "Bitekmindenhol",
            "model": "SZKP HA kliens",
        }

    async def async_press(self) -> None:
        await self.coordinator.ota_update_now()
