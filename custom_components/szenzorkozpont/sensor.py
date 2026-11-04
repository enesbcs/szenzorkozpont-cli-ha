"""SzenzorKozpont diagnostic entities (do NOT mirror HA sensors)."""
from __future__ import annotations

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EntityCategory, UnitOfTime
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    coordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        [SzkDiagSensor(coordinator, entry, kind)
         for kind in ("last_report", "next_poll", "drift", "sent")]
    )


class SzkDiagSensor(CoordinatorEntity, SensorEntity):
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_has_entity_name = True

    _ICONS = {
        "last_report": "mdi:cloud-upload",
        "next_poll": "mdi:timer-outline",
        "drift": "mdi:clock-outline",
        "sent": "mdi:chart-line",
    }

    def __init__(self, coordinator, entry: ConfigEntry, kind: str) -> None:
        super().__init__(coordinator)
        self._kind = kind
        # Display name comes from strings.json / translations via this key.
        self._attr_translation_key = kind
        self._attr_icon = self._ICONS.get(kind)
        self._attr_unique_id = f"{entry.entry_id}_{kind}"
        if kind in ("next_poll", "drift"):
            self._attr_device_class = SensorDeviceClass.DURATION
            self._attr_native_unit_of_measurement = UnitOfTime.SECONDS
        self._attr_device_info = {
            "identifiers": {(DOMAIN, entry.entry_id)},
            "name": "SzenzorKozpont",
            "manufacturer": "Bitekmindenhol",
            "model": "SZKP HA kliens",
        }

    @property
    def native_value(self) -> str | int | None:
        data = self.coordinator.data or {}
        if self._kind == "last_report":
            return data.get("last_report_ok")
        if self._kind == "next_poll":
            return data.get("next_poll_s")
        if self._kind == "drift":
            return data.get("clock_drift")
        if self._kind == "sent":
            return data.get("sent")
        return None
