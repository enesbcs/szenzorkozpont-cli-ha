"""SzenzorKozpont — config flow: backend pick + instant test POST."""
from __future__ import annotations

import logging
import re
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigEntry, ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import ApiClient, ApiError
from .const import (
    API_KEY_RE,
    BACKEND_CHOICES,
    BACKEND_LABELS,
    BACKEND_SZK,
    BACKEND_THINGSBOARD,
    DEFAULT_POLL_DAY_S,
    DEFAULT_POLL_NIGHT_S,
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)


def _clean_url(url: str) -> str:
    return (url or "").strip().rstrip("/")


def _cred_hash(*parts: Any) -> str:
    """Device-ID hash for config ID (token/channel must not
    appear in plaintext)."""
    import hashlib as _hl

    return _hl.sha256("|".join(str(p or "") for p in parts).encode()).hexdigest()[:12]


def _entry_unique_id(user_input: dict[str, Any]) -> str:
    """Unique ID for config entry.

    SZKP: backend + URL (unchanged behavior: one instance
    per server). Foreign backends also hash device credential,
    since one TB server may host several devices (token) —
    those are separate instances.
    """
    backend = user_input.get("backend") or BACKEND_SZK
    url = str(user_input.get("server_url", "")).lower()
    if backend == BACKEND_THINGSBOARD:
        # Same normalize as at send time (trailing /api/v1 stripped),
        # else two spellings would look like two instances.
        from .backends import tb_base

        url = tb_base(url)
    uid = f"{backend}:{url}"
    if backend == BACKEND_THINGSBOARD:
        uid += ":" + _cred_hash(user_input.get("api_key"))
    return uid


def _parse_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _looks_like_tb_user_key(key: str) -> bool:
    """Looks like user REST API key (tb_… JWT), not device token?

    Only used AFTER rejected (401) probe to refine message,
    never to block — so heuristic misses are harmless.
    """
    k = key or ""
    return k.startswith("tb_") or "." in k or len(k) > 64


def _validate(entry: dict[str, Any]) -> dict[str, str]:
    """Returns: HA errors-dict (empty = valid).

    One instance has one backend, so credential travels in single
    shared `api_key` field (SZKP key / TB token).
    Format checked only for SZKP key (fixed shape we issue);
    for foreign systems non-empty suffices — real check is
    probe send (401 → auth_failed) plus backoff retry at runtime.
    """
    backend = entry.get("backend") or BACKEND_SZK
    url = _clean_url(entry.get("server_url", ""))
    key = (entry.get("api_key") or "").strip()
    errors: dict[str, str] = {}
    if backend == BACKEND_SZK:
        if not url.startswith("https://"):
            errors["server_url"] = "invalid_url"
        if not re.match(API_KEY_RE, key):
            errors["api_key"] = "invalid_key"
    elif backend == BACKEND_THINGSBOARD:
        if not url.startswith(("http://", "https://")):
            errors["server_url"] = "invalid_url"
        if not key:
            errors["api_key"] = "missing_key"
    return errors


async def _probe(entry: dict[str, Any], session: Any) -> dict[str, str]:
    """Instant probe toward backend. Returns: HA errors-dict (empty = OK)."""
    backend = entry.get("backend") or BACKEND_SZK
    try:
        day_s = _parse_int(entry.get("poll_day_s"), DEFAULT_POLL_DAY_S)
        night_s = _parse_int(entry.get("poll_night_s"), DEFAULT_POLL_NIGHT_S)
    except (TypeError, ValueError):
        day_s, night_s = DEFAULT_POLL_DAY_S, DEFAULT_POLL_NIGHT_S
    if not 60 <= day_s <= 86400 or not 60 <= night_s <= 86400:
        return {"base": "unknown"}
    if backend == BACKEND_SZK:
        api = ApiClient(entry["server_url"], entry["api_key"], session=session)
        try:
            await api.pull(session, 0)
        except ApiError as err:
            msg = str(err)
            if "401" in msg or "kulcs" in msg:
                return {"api_key": "auth_failed"}
            if "Elérhetetlen" in msg or "elérhetetlen" in msg:
                return {"base": "cannot_connect"}
            _LOGGER.debug("SzenzorKozpont config-teszt hiba: %s", type(err).__name__)
            return {"base": "unknown"}
        return {}
    # Foreign backends have no pull: probe is a real (tiny)
    # telemetry send — 401 response qualifies auth.
    from .backends import ThingsboardBackend

    url = _clean_url(entry.get("server_url", ""))
    key = (entry.get("api_key") or "").strip()
    try:
        if backend == BACKEND_THINGSBOARD:
            api = ThingsboardBackend(url, key)
            await api.send_telemetry([{"n": "probe", "vb": True}], 1, session=session)
    except ApiError as err:
        msg = str(err)
        if "401" in msg or "403" in msg:
            if backend == BACKEND_THINGSBOARD and _looks_like_tb_user_key(key):
                # Common mistake: user REST API key (tb_…)
                # is not a device token — device telemetry endpoint
                # always rejects it with 401.
                return {"api_key": "tb_user_key"}
            return {"base": "auth_failed"}
        if "Elérhetetlen" in msg or "elérhetetlen" in msg:
            return {"base": "cannot_connect"}
        _LOGGER.debug("SzenzorKozpont idegen backend próba: %s", type(err).__name__)
        return {"base": "unknown"}
    return {}


class SzkConfigFlow(ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            errors = _validate(user_input)
            if not errors:
                await self.async_set_unique_id(_entry_unique_id(user_input))
                self._abort_if_unique_id_configured()
                session = async_get_clientsession(self.hass)
                errors = await _probe(user_input, session)
            if not errors:
                backend = user_input.get("backend") or BACKEND_SZK
                data: dict[str, Any] = {
                    "backend": backend,
                    "server_url": _clean_url(user_input.get("server_url", "")),
                    "api_key": (user_input.get("api_key") or "").strip(),
                    "poll_day_s": _parse_int(user_input.get("poll_day_s"), DEFAULT_POLL_DAY_S),
                    "poll_night_s": _parse_int(user_input.get("poll_night_s"), DEFAULT_POLL_NIGHT_S),
                }
                if backend == BACKEND_THINGSBOARD:
                    data["channel_id"] = (user_input.get("channel_id") or "").strip()
                    data["workspace_id"] = (user_input.get("workspace_id") or "").strip()
                return self.async_create_entry(
                    title=f"{BACKEND_LABELS[backend]} ({data['server_url']})",
                    data=data,
                )
        schema = vol.Schema(
            {
                vol.Required("backend", default=BACKEND_SZK): vol.In(
                    {b: BACKEND_LABELS[b] for b in BACKEND_CHOICES}
                ),
                vol.Required("server_url", default="https://"): str,
                # One instance has one backend, so credential is a
                # shared field (SZKP key / TB token). Only this
                # and server URL required; rest optional.
                vol.Required("api_key"): str,
                vol.Required("poll_day_s", default=DEFAULT_POLL_DAY_S): int,
                vol.Required("poll_night_s", default=DEFAULT_POLL_NIGHT_S): int,
            }
        )
        return self.async_show_form(step_id="user", data_schema=schema, errors=errors)

    @staticmethod
    def async_get_options_flow(entry: ConfigEntry) -> OptionsFlow:
        return SzkOptionsFlow(entry)


class SzkOptionsFlow(OptionsFlow):
    def __init__(self, entry: ConfigEntry) -> None:
        self._entry = entry

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)
        opts = self._entry.options
        data = self._entry.data
        backend = str(data.get("backend") or BACKEND_SZK)
        schema_dict: dict[Any, Any] = {
            vol.Required("poll_day_s", default=opts.get("poll_day_s", data.get("poll_day_s", DEFAULT_POLL_DAY_S))): int,
            vol.Required("poll_night_s", default=opts.get("poll_night_s", data.get("poll_night_s", DEFAULT_POLL_NIGHT_S))): int,
        }
        if backend == BACKEND_SZK:
            # Change-based report, OTA and debug are SZKP-specific.
            schema_dict.update({
                vol.Required("change_only", default=opts.get("change_only", True)): bool,
                vol.Required("allow_ota_on_metered", default=opts.get("allow_ota_on_metered", False)): bool,
                vol.Required("auto_update", default=opts.get("auto_update", False)): bool,
                vol.Required("ota_channel", default=opts.get("ota_channel", "stable")): vol.In(["stable", "beta"]),
                vol.Required("verify_ssl", default=opts.get("verify_ssl", True)): bool,
                vol.Required("debug", default=opts.get("debug", False)): bool,
            })
        else:
            schema_dict.update({
                vol.Required("verify_ssl", default=opts.get("verify_ssl", True)): bool,
                vol.Required("debug", default=opts.get("debug", False)): bool,
            })
            if backend == BACKEND_THINGSBOARD:
                # Chunking: demo.thingsboard.io drops empty
                # 500 above 20 keys (may be wasteful on own server).
                schema_dict.update({
                    vol.Required("tb_chunking", default=opts.get("tb_chunking", True)): bool,
                })
        return self.async_show_form(step_id="init", data_schema=vol.Schema(schema_dict))
