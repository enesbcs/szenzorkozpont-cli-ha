"""SzenzorKozpont — HA custom integration entry point."""
from __future__ import annotations

import logging
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.storage import Store

from .api import ApiClient
from .backends import (
    BACKEND_SZK,
    BACKEND_THINGSBOARD,
    ThingsboardBackend,
)
from .protocol import poll_cfg, poll_cfg_changed
from .const import DOMAIN, STORE_KEY, STORE_VERSION
from .coordinator import SzkCoordinator

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[str] = ["sensor", "button"]


def _manifest_version() -> str:
    """Manifest version read from disk (always visible at startup)."""
    import json as _json
    import pathlib as _pl

    try:
        mf = _json.loads((_pl.Path(__file__).resolve().parent / "manifest.json").read_text(encoding="utf-8"))
        return str(mf.get("version", "?"))
    except Exception:
        return "?"


def _default_state() -> dict:
    return {
        "config_rev": 0,
        "config_fp": "",
        "allowed": [],
        "denied": [],
        "poll_override": {},
        "failures": 0,
        "buffer": {},
        "unavail_counts": {},
        "key_entity": {},
        "ota": {"last_check": 0, "seen": ""},
        "poll_cfg_last": None,
        "sysinfo_pending": False,
        "triggers": [],
    }


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    session = async_get_clientsession(hass)
    store: Store = Store(hass, STORE_VERSION, f"{STORE_KEY}_{entry.entry_id}")
    state = await store.async_load()
    if not isinstance(state, dict):
        state = _default_state()

    api = None
    if str(entry.data.get("backend") or BACKEND_SZK) == BACKEND_SZK:
        # ApiClient only for SZKP native protocol (https-enforced!);
        # foreign backend has no pull/commands, so no ApiClient.
        api = ApiClient(
            entry.data["server_url"],
            entry.data["api_key"],
            verify_ssl=entry.options.get("verify_ssl", True),
            session=session,
        )
    # Startup banner — debug mode only (keep prod log clean).
    # Disk read runs in executor (blocking calls banned on event loop).
    from .const import VERSION as CODE_VERSION

    def _build_backend() -> tuple[str, Any]:
        """(backend_kind, backend) from entry settings.

        Foreign backend needs no ApiClient — no pull/commands with it.
        """
        kind = str(entry.data.get("backend") or BACKEND_SZK)
        if kind == BACKEND_THINGSBOARD:
            return kind, ThingsboardBackend(
                entry.data.get("server_url", ""),
                entry.data.get("api_key", ""),
                verify_ssl=entry.options.get("verify_ssl", True),
                chunking=entry.options.get("tb_chunking", True),
            )
        return BACKEND_SZK, None

    backend_kind, backend = _build_backend()
    if backend is not None:
        # Save location for failed sends (debugging): HA config dir.
        try:
            backend.debug_path = hass.config.path(
                f"szenzorkozpont_{backend_kind}_{entry.entry_id[:8]}_last.json")
        except Exception:
            backend.debug_path = None

    disk_ver = await hass.async_add_executor_job(_manifest_version)
    if disk_ver != CODE_VERSION:
        _LOGGER.warning(
            "SzenzorKozpont VERZIÓ-ELTÉRÉS: manifest=%s, kód=%s — keveredett fájlok?",
            disk_ver, CODE_VERSION,
        )
    if entry.options.get("debug", False):
        masked = backend.masked if backend is not None else (
            api.masked if api is not None else "?")
        _LOGGER.warning(
            "SzenzorKozpont v%s elindult -> %s (kulcs: %s)",
            disk_ver, entry.data.get("server_url"), masked,
        )
    # Polling changed in HA options UI detected:
    # compare against value seen at previous startup. If changed,
    # client sends sysinfo itself next cycle (beyond server request) — server
    # adopts actual values from it. No send on first startup (server
    # asks then, if it never got sysinfo).
    cur_poll = poll_cfg(entry.options, entry.data)
    if poll_cfg_changed(state.get("poll_cfg_last"), cur_poll):
        state["sysinfo_pending"] = True
        _LOGGER.warning(
            "SzenzorKozpont polling módosult a beállítófelületen: "
            "nappal %s mp, éjszaka %s mp — sysinfo küldése a következő ciklusban",
            cur_poll["day_s"], cur_poll["night_s"],
        )
    state["poll_cfg_last"] = dict(cur_poll)
    await store.async_save(state)

    coordinator = SzkCoordinator(
        hass, entry, api, session, store, state,
        backend_kind=backend_kind, backend=backend,
    )
    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator

    await coordinator.async_config_entry_first_refresh()
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    entry.async_on_unload(entry.add_update_listener(_update_listener))
    # Trigger watch: every HA state change goes through coordinator
    # handler; it filters to watched binary sensor ON<->OFF transitions,
    # and sends out-of-band report on firing (regardless of polling).
    entry.async_on_unload(
        hass.bus.async_listen("state_changed", coordinator._on_state_event)
    )
    return True


async def _update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    # Note: unload takes platforms one by one (no list allowed).
    ok = True
    for platform in PLATFORMS:
        ok = ok and await hass.config_entries.async_forward_entry_unload(entry, platform)
    if ok:
        hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
    return ok
