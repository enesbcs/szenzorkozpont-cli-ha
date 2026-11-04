"""SzenzorKozpont — DataUpdateCoordinator: pull → exec → report."""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any

from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from . import api as api_mod
from .backends import BACKEND_SZK
from .const import (
    DEFAULT_DAY_FROM,
    DEFAULT_DAY_TO,
    DEFAULT_POLL_DAY_S,
    DEFAULT_POLL_NIGHT_S,
    DOMAIN,
    FULL_REPORT_EVERY_S,
    MAX_ATTRS_PER_SENSOR,
    MAX_ATTR_BYTES,
    SKIP_ATTRS,
    VERSION,
)
from .protocol import (
    apply_triggers,
    bool_of_state,
    build_config_items,
    classify,
    config_fingerprint,
    clock_drift_srv,
    control_service_call,
    encode_value,
    poll_cfg,
    poll_interval,
    semver_gt,
    sensor_key_for,
    split_changed,
    trigger_transition,
)

_LOGGER = logging.getLogger(__name__)


def _manifest_version(pkg_dir: Any) -> str:
    import json as _json
    import pathlib as _pl

    mf = _json.loads((_pl.Path(pkg_dir) / "manifest.json").read_text(encoding="utf-8"))
    return str(mf.get("version", ""))


def _ha_version(hass: Any) -> str:
    try:
        from homeassistant.const import __version__ as hv

        return str(hv)
    except (ImportError, AttributeError):
        return "?"


def _climate_expectation(calls: list) -> dict | None:
    """Thermostat service calls → expected report fragment.
    Form: {"v": target} and/or {"p": {hvac_mode?, preset_mode?}}, and
    on turn-off {"raw": "off"} (mode string is the state itself)."""
    ov: dict[str, Any] = {}
    p: dict[str, Any] = {}
    for service, extra in calls:
        if service == "turn_off":
            return {"raw": "off"}
        if not isinstance(extra, dict):
            continue
        if service == "set_temperature" and isinstance(extra.get("temperature"), (int, float)):
            if not isinstance(extra["temperature"], bool):
                ov["v"] = float(extra["temperature"])
        if service == "set_hvac_mode" and isinstance(extra.get("hvac_mode"), str) and extra["hvac_mode"]:
            p["hvac_mode"] = extra["hvac_mode"][:32]
        if service == "set_preset_mode" and isinstance(extra.get("preset_mode"), str) and extra["preset_mode"]:
            p["preset_mode"] = extra["preset_mode"][:32]
    if p:
        ov["p"] = p
    return ov or None


def _switch_expectation(service: str, pdata: object, pre: bool | None) -> dict | None:
    """Switch/light service call → expected report fragment.
    Form: {"vb": bool} + for lamp {"p": params}. Toggle from
    unknown start, plus press/scene have no expectation (None)."""
    if service == "turn_on":
        ov: dict[str, Any] = {"vb": True}
        if isinstance(pdata, dict) and pdata:
            ov["p"] = dict(pdata)
        return ov
    if service == "turn_off":
        return {"vb": False}
    if service == "toggle" and pre is not None:
        return {"vb": not pre}
    return None


def _host_info(path: str) -> dict:
    """Sync host discovery for sysinfo — stdlib only, runs in executor.

    Returns: {cpu, runtime, arch, storage_free_mb}; on error missing
    key dropped (caller fills placeholder). Same fields on ESP32:
    cpu=chip (e.g. "ESP32-S3"), runtime="esp-idf vX.Y",
    arch="xtensa"/"riscv", storage_free_mb=free flash in MB.
    """
    import platform as _pl
    import shutil as _sh

    out: dict = {}
    try:
        cpu = (_pl.system() or "") + "/" + (_pl.machine() or "")
        cpu = cpu.strip("/")
        if cpu:
            out["cpu"] = cpu[:200]
    except Exception:
        pass
    try:
        out["runtime"] = ("python " + _pl.python_version())[:200]
    except Exception:
        pass
    try:
        if _pl.machine():
            out["arch"] = _pl.machine()[:64]
    except Exception:
        pass
    try:
        if path:
            out["storage_free_mb"] = int(_sh.disk_usage(path).free // (1024 * 1024))
    except Exception:
        pass
    return out


def _report_items(report: list) -> list:
    """_collect() report items -> sendable items [{n, v|vs|vb[, p]}].

    Shared builder for SZKP and foreign paths (backends make
    their own payload from this).
    """
    items = []
    for en in report:
        if not isinstance(en, dict):
            continue
        for field in ("v", "vs", "vb"):
            if field in en:
                item = {"n": en["n"], field: en[field]}
                if isinstance(en.get("p"), dict) and en["p"]:
                    item["p"] = en["p"]
                items.append(item)
                break
    return items


class SzkCoordinator(DataUpdateCoordinator[dict]):
    """FIXED cycle: pull → command exec → state read → report."""

    def __init__(self, hass: Any, entry: Any, api: Any, session: Any, store: Any, state: dict,
                 backend_kind: str = BACKEND_SZK, backend: Any = None) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN} ({entry.entry_id[:6]})",
            update_interval=timedelta(seconds=DEFAULT_POLL_DAY_S),
        )
        self.entry = entry
        self.api = api
        self.session = session
        self.store = store
        self.state = state
        # Foreign backend (thingsboard): telemetry send only,
        # no pull/commands/config/sysinfo/OTA. backend=None + non-szk
        # kind stops cycle with error (config error).
        self.backend_kind = backend_kind or BACKEND_SZK
        self.backend = backend
        self.last_report_ok: str | None = None
        self.last_error: str | None = None
        self.clock_drift: int = 0
        # Boot time for uptime_s (in-memory; restart = new count).
        self._boot_ts = time.time()
        # First cycle: forced config in debug mode (for testing).
        self._startup = True
        # Next report must be full (first report after restart
        # always full; manual sync button also sets this).
        self._force_full = True
        # First successful send done? (in-memory; restart = from scratch).
        # After empty initial send next cycle comes sooner.
        self._first_send_done = False

    # -- helpers ---------------------------------------------------------

    def _debug_on(self) -> bool:
        try:
            return bool(self.entry.options.get("debug", False))
        except AttributeError:
            return False

    def apply_log_level(self) -> None:
        """Debug switch → own logger level (call on option change too)."""
        pkg_log = logging.getLogger("custom_components.szenzorkozpont")
        if self._debug_on():
            pkg_log.setLevel(logging.DEBUG)
        else:
            pkg_log.setLevel(logging.NOTSET)

    def _exc(self, msg: str, *args: Any) -> None:
        """Log exception: with traceback in debug, one line otherwise."""
        if self._debug_on():
            _LOGGER.exception(msg, *args)
        else:
            _LOGGER.debug(msg, *args)

    def _opts(self, key: str, default: Any) -> Any:
        try:
            return self.entry.options.get(key, self.entry.data.get(key, default))
        except AttributeError:
            return default

    def _poll_cfg(self) -> tuple[int, int]:
        cfg = poll_cfg(self.entry.options, self.entry.data)
        return cfg["day_s"], cfg["night_s"]

    def _interval(self) -> tuple[int, str]:
        """(s, 'day'|'night') — from entry settings + set_poll override."""
        day_s, night_s = self._poll_cfg()
        return poll_interval(
            datetime.now().astimezone(), day_s, night_s,
            DEFAULT_DAY_FROM, DEFAULT_DAY_TO,
            self.state.get("poll_override", {}),
        )

    async def _save(self) -> None:
        try:
            await self.store.async_save(self.state)
        except Exception as err:  # defensive: save error must not stop cycle
            _LOGGER.warning("SzenzorKozpont állapotmentés sikertelen: %s", type(err).__name__)

    def _fail(self, msg: str, interval: int | None = None) -> UpdateFailed:
        fails = int(self.state.get("failures", 0)) + 1
        self.state["failures"] = fails
        self.last_error = msg
        if interval is not None:
            self.update_interval = timedelta(seconds=interval)
        if fails >= 3:
            self._repair("server_unreachable")
            self._maybe_rollback_offer()
        return UpdateFailed(msg)

    def _backoff(self, normal_s: int) -> int:
        """Progressive wait: normal×2^failures, max 1 h (spares sick server)."""
        fails = int(self.state.get("failures", 0))
        return min(normal_s * (2 ** min(fails, 3)), 3600)

    def _fail_auth(self) -> UpdateFailed:
        """Bad/revoked key: no point spinning, long pause + separate issue."""
        self.state["failures"] = int(self.state.get("failures", 0)) + 1
        self.last_error = "hibás API-kulcs"
        self.update_interval = timedelta(seconds=3600)
        self._repair("bad_key")
        return UpdateFailed("hibás API-kulcs")

    def _fail_rate(self, retry_after: int, normal_s: int, msg: str = "rate limited") -> UpdateFailed:
        """Rate-limit: rest as long as server asks (max 1 h).
        Message passes to HA log (e.g. quota exhaustion distinct)."""
        self.update_interval = timedelta(seconds=min(max(retry_after, normal_s), 3600))
        self.last_error = msg
        return UpdateFailed(msg)

    def _maybe_rollback_offer(self) -> None:
        """3 failures within 24 h after auto-update → offer backup restore."""
        import pathlib as _pl

        ota = self.state.get("ota", {})
        try:
            updated_at = int(ota.get("updated_at", 0) or 0)
        except (TypeError, ValueError):
            return
        backup = ota.get("backup", "")
        if not backup or time.time() - updated_at > 24 * 3600:
            return
        if not _pl.Path(backup).is_dir():
            return
        self._repair("update_rollback", backup=str(backup))

    def _ok(self) -> None:
        self.state["failures"] = 0
        self.last_error = None
        self._dismiss_repair("server_unreachable")
        self._dismiss_repair("bad_key")

    def _repair(self, issue_id: str, **placeholders: Any) -> None:
        try:
            from homeassistant.helpers import issue_registry as ir

            ir.async_create_issue(
                self.hass,
                DOMAIN,
                f"{self.entry.entry_id[:8]}_{issue_id}",
                is_fixable=False,
                severity=ir.IssueSeverity.WARNING,
                translation_key=issue_id,
                translation_placeholders=placeholders or None,
            )
        except Exception as err:
            _LOGGER.debug("repair-issue hiba: %s", type(err).__name__)

    def _dismiss_repair(self, issue_id: str) -> None:
        try:
            from homeassistant.helpers import issue_registry as ir

            ir.async_delete_issue(
                self.hass, DOMAIN, f"{self.entry.entry_id[:8]}_{issue_id}"
            )
        except Exception:
            pass

    # -- state read ---------------------------------------------------

    def _registries(self) -> tuple[dict, dict, dict]:
        """entity_id → (device_id, area_id); device/area names."""
        try:
            from homeassistant.helpers import (
                area_registry as ar,
                device_registry as dr,
                entity_registry as er,
            )

            ent_reg = er.async_get(self.hass)
            dev_reg = dr.async_get(self.hass)
            area_reg = ar.async_get(self.hass)
        except Exception:
            return {}, {}, {}
        ent_map: dict[str, tuple[str | None, str | None]] = {}
        try:
            for eid, ent in ent_reg.entities.items():
                ent_map[eid] = (ent.device_id, ent.area_id)
        except AttributeError:
            pass
        dev_names: dict[str, str] = {}
        try:
            # Device-registry mapping API deprecated (gone in 2027.9):
            # ITERATE `devices` view, it yields entries themselves
            # (.items() already warns). Entity-/area-registry
            # are plain mappings (unaffected).
            for dev in dev_reg.devices:
                did = getattr(dev, "id", None)
                if did is None:
                    continue
                if getattr(dev, "name", None):
                    dev_names[did] = dev.name
        except AttributeError:
            pass
        area_names: dict[str, str] = {}
        try:
            for aid, area in area_reg.areas.items():
                if area.name:
                    area_names[aid] = area.name
        except AttributeError:
            pass
        return ent_map, dev_names, area_names

    def _remember_buffer(self, report: list[dict]) -> None:
        """Refresh offline buffer with given items (latest/sensor)."""
        buf: dict[str, dict] = dict(self.state.get("buffer", {}))
        for en in report:
            for field in ("v", "vs", "vb"):
                if field in en:
                    buf[en["n"]] = {"f": field, "v": en[field]}
                    break
        if len(buf) > 500:
            buf = dict(list(buf.items())[-500:])
        self.state["buffer"] = buf

    def _collect(self, only_eids: set | None = None) -> tuple[list[dict], list[dict], bool]:
        """Returns: (report items, config items, has new key).

        Each entity in try/except: one bad sensor must not kill cycle.
        With only_eids only requested entities re-read (for
        post-control refresh): then no registry read,
        diagnostics, config build or state save — report items only.
        """
        from .const import DEFAULT_INCLUDED_DOMAINS

        if only_eids is not None:
            states = []
            for eid in only_eids:
                try:
                    st = self.hass.states.get(eid)
                except Exception:
                    st = None
                if st is not None:
                    states.append(st)
            ent_map, dev_names, area_names = {}, {}, {}
            dom_counts: dict = {}
        else:
            domains = tuple(self._opts("included_domains", list(DEFAULT_INCLUDED_DOMAINS)))
            ent_map, dev_names, area_names = self._registries()
        taken: set[str] = set(self.state.get("key_entity", {}).keys())
        allowed: list[str] = list(self.state.get("allowed", []))
        denied: list[str] = list(self.state.get("denied", []))
        report: list[dict] = []
        config: list[dict] = []
        new_key = False
        key_entity: dict[str, str] = dict(self.state.get("key_entity", {}))
        counts: dict[str, int] = dict(self.state.get("unavail_counts", {}))

        if only_eids is None:
            try:
                states = self.hass.states.async_all(domains)
            except Exception as err:
                _LOGGER.warning("SzenzorKozpont állapotolvasás sikertelen: %s", type(err).__name__)
                return [], [], False

            # Diagnostics for skipped entities (debug log + state).
            try:
                from collections import Counter as _Counter

                _all = self.hass.states.async_all()
                dom_counts = dict(_Counter(
                    s.entity_id.split(".")[0] for s in _all if "." in s.entity_id
                ))
            except Exception:
                dom_counts = {}
        skipped_unavail: list[str] = []

        for st in states:
            try:
                eid = st.entity_id
                domain, _, object_id = eid.partition(".")
                if not object_id:
                    continue
                key = key_entity.get(eid)
                if key is None:
                    key = sensor_key_for(domain, object_id, taken)
                    taken.add(key)
                    key_entity[eid] = key
                else:
                    taken.add(key)
                if key in denied:
                    continue
                # Only allowed key carries value in report; config
                # still goes for EVERY non-denied key, so new
                # sensor shows as pending on server.
                send_value = not allowed or key in allowed
                if not send_value:
                    new_key = True
                raw_state = st.state if isinstance(st.state, str) else str(st.state)
                if raw_state in ("unknown", "unavailable"):
                    n = counts.get(eid, 0) + 1
                    counts[eid] = n
                    if n != 3 and len(skipped_unavail) < 50:
                        skipped_unavail.append(f"{eid}={raw_state}")
                    if n < 3:
                        continue
                    # Dead since 3rd cycle: single unavailable notice
                    if n > 3:
                        continue
                    if not send_value:
                        continue
                    report.append({"n": key, "vs": "unavailable"})
                    continue
                counts.pop(eid, None)
                attrs = st.attributes if isinstance(st.attributes, dict) else {}
                dtype_override = None
                entry_params = None
                if domain == "climate":
                    from .protocol import climate_state as _cs

                    field, value, cparams = _cs(raw_state, attrs)
                    if field is None:
                        continue
                    dtype_override = "float" if field == "v" else "string"
                    if cparams:
                        entry_params = cparams
                else:
                    enc = encode_value(raw_state)
                    if enc is None:
                        continue
                    field, value = enc
                    if domain == "light" and field == "vb":
                        from .protocol import light_state_params

                        _mem = self.state.get("light_last", {})
                        _last = _mem.get(key) if isinstance(_mem, dict) else None
                        _p = light_state_params(
                            attrs, _last if isinstance(_last, dict) else None)
                        if _p:
                            entry_params = _p
                        if value is True:
                            # Remember on-state: these go in report when off too
                            # (server/app needs last brightness/color).
                            _keep = {k: _p[k] for k in (
                                "brightness", "rgb_color", "rgbw_color", "rgbww_color")
                                if k in _p}
                            if _keep:
                                if not isinstance(_mem, dict):
                                    _mem = {}
                                    self.state["light_last"] = _mem
                                _mem[key] = _keep
                if send_value:
                    entry: dict[str, Any] = {"n": key, field: value}
                    if entry_params:
                        entry["p"] = entry_params
                    report.append(entry)

                # config-meta (rarely changes, but cheap to rebuild)
                device_id, area_id = ent_map.get(eid, (None, None))
                group_key = group_label = ""
                if device_id and device_id in dev_names:
                    from .protocol import normalize_key as _nk

                    group_key = _nk(f"dev_{device_id[:12]}")
                    group_label = dev_names[device_id]
                elif area_id and area_id in area_names:
                    from .protocol import normalize_key as _nk

                    group_key = _nk(f"area_{str(area_id)[:20]}")
                    group_label = area_names[area_id]
                meta_attrs: dict[str, Any] = {}
                for an, av in attrs.items():
                    if len(meta_attrs) >= MAX_ATTRS_PER_SENSOR:
                        break
                    if an in SKIP_ATTRS:
                        continue
                    if isinstance(av, bool):
                        meta_attrs[an] = av
                    elif isinstance(av, (int, float)):
                        meta_attrs[an] = av
                    elif isinstance(av, str) and len(av.encode("utf-8")) <= MAX_ATTR_BYTES:
                        meta_attrs[an] = av
                unit = attrs.get("unit_of_measurement", "")
                unique_id = ""
                try:
                    from homeassistant.helpers import entity_registry as er2

                    ent = er2.async_get(self.hass).entities.get(eid)
                    if ent and ent.unique_id:
                        unique_id = str(ent.unique_id)[:100]
                except Exception:
                    pass
                if unique_id:
                    meta_attrs["unique_id"] = unique_id
                if device_id:
                    meta_attrs["ha_device_id"] = str(device_id)[:100]
                if area_id:
                    meta_attrs["ha_area"] = str(area_id)[:100]
                if "device_class" in attrs and isinstance(attrs["device_class"], str):
                    meta_attrs["device_class"] = attrs["device_class"][:100]
                item: dict[str, Any] = {
                    "n": key,
                    "label": str(attrs.get("friendly_name", object_id))[:100],
                    "u": "°C" if domain == "climate" else (str(unit)[:24] if isinstance(unit, str) else ""),
                    "kind": classify(domain),
                    "dtype": dtype_override or {"v": "float", "vs": "string", "vb": "bool"}[field],
                    "attrs": meta_attrs,
                }
                if group_key:
                    item["group_key"] = group_key
                    item["group_label"] = group_label[:100]
                if domain == "light":
                    from .protocol import light_ctl

                    _ctl = light_ctl(attrs)
                    if _ctl:
                        item["ctl"] = _ctl
                elif domain == "climate":
                    from .protocol import climate_ctl

                    _cctl = climate_ctl(attrs)
                    if _cctl:
                        item["ctl"] = _cctl
                # Targeted re-read skips config build (first
                # snapshot's valid) — report items only needed.
                if only_eids is None:
                    config.append(item)
            except Exception as err:  # one entity never stops cycle
                _LOGGER.debug("SzenzorKozpont entitás-kihagyás: %s", type(err).__name__)
                continue

        # buffer: latest per sensor for offline fill
        self._remember_buffer(report)
        if only_eids is not None:
            # Targeted re-read: no diagnostics, config cache
            # or state save — fresh report items only.
            # (Unavail counting stays with full scan, so
            # it does not double per cycle.)
            return report, [], False
        self.state["unavail_counts"] = {k: v for k, v in counts.items() if v >= 3}
        self.state["key_entity"] = {eid: key_entity[eid] for eid in key_entity if key_entity[eid] in taken}
        self.state["last_scan"] = {
            "ts": int(time.time()),
            "domains": dom_counts,
            "collected": len(report),
            "skipped_unavail": skipped_unavail[:50],
        }
        _LOGGER.debug(
            "SzenzorKozpont scan: %d entitás, %d beküldve, %d nem elérhető (%s)",
            sum(dom_counts.values()), len(report), len(skipped_unavail),
            ",".join(skipped_unavail[:10]),
        )
        self._config_cache = config
        return report, config, new_key

    # -- command exec ------------------------------------------------

    async def _exec_control(self, sensor_key: str, state: str, params: dict | None = None) -> bool:
        from .protocol import LIGHT_PARAM_KEYS, climate_calls, control_service_call

        domain = sensor_key.split(".")[0] if "." in sensor_key else ""
        if domain == "climate":
            calls = climate_calls(sensor_key, state, params if isinstance(params, dict) else {})
            if not calls:
                _LOGGER.warning("SzenzorKozpont ismeretlen termosztát-parancs: %s", sensor_key)
                return False
            key_entity: dict[str, str] = self.state.get("key_entity", {})
            eid = next((e for e, k in key_entity.items() if k == sensor_key), None)
            if eid is None:
                _LOGGER.warning("SzenzorKozpont vezérlés: nincs entitás %s mögött", sensor_key)
                return False
            try:
                for service, extra in calls:
                    data: dict[str, Any] = {"entity_id": eid}
                    data.update(extra)
                    await self.hass.services.async_call(domain, service, data, blocking=True)
                self._record_expected(eid, _climate_expectation(calls))
                return True
            except Exception as err:
                _LOGGER.warning("SzenzorKozpont termosztát-hiba %s: %s", sensor_key, type(err).__name__)
                return False
        clean = {k: v for k, v in (params or {}).items() if k in LIGHT_PARAM_KEYS} if isinstance(params, dict) else {}
        call = control_service_call(sensor_key, state, clean)
        if call is None:
            _LOGGER.warning("SzenzorKozpont ismeretlen vezérlés: %s", sensor_key)
            return False
        domain, service, pdata = call
        key_entity: dict[str, str] = self.state.get("key_entity", {})
        eid = next((e for e, k in key_entity.items() if k == sensor_key), None)
        if eid is None:
            _LOGGER.warning("SzenzorKozpont vezérlés: nincs entitás %s mögött", sensor_key)
            return False
        # Pre-toggle start state needed — read BEFORE call.
        pre: bool | None = None
        if service == "toggle":
            try:
                cur = self.hass.states.get(eid)
                pre = bool_of_state(cur.state if cur is not None else None)
            except Exception:
                pre = None
        try:
            data: dict[str, Any] = {"entity_id": eid}
            data.update(pdata if isinstance(pdata, dict) else {})
            await self.hass.services.async_call(domain, service, data, blocking=True)
            self._record_expected(eid, _switch_expectation(service, pdata, pre))
            return True
        except Exception as err:
            _LOGGER.warning("SzenzorKozpont vezérlés-hiba %s: %s", sensor_key, type(err).__name__)
            return False

    def _record_expected(self, eid: str, ov: dict | None) -> None:
        """Expected state of exec'd control — report mirrors it if HA
        state machine not yet updated (e.g. slow Zigbee/MQTT device)."""
        if not ov:
            return
        exp = getattr(self, "_expected", None)
        if not isinstance(exp, dict):
            exp = {}
            self._expected = exp
        exp[eid] = ov

    @staticmethod
    def _expected_met(st: object, ov: dict) -> bool:
        """Does entity current state match expected value?"""
        raw = st.state if isinstance(getattr(st, "state", None), str) else str(getattr(st, "state", ""))
        attrs = getattr(st, "attributes", None)
        if not isinstance(attrs, dict):
            attrs = {}
        if "raw" in ov and isinstance(ov["raw"], str):
            return raw.strip().lower() == ov["raw"].strip().lower()
        if "vb" in ov:
            return bool_of_state(raw) == bool(ov["vb"])
        if "v" in ov:
            try:
                return abs(float(attrs.get("temperature", raw)) - float(ov["v"])) < 0.05
            except (TypeError, ValueError):
                return False
        if "p" in ov and isinstance(ov["p"], dict):
            p = ov["p"]
            if "hvac_mode" in p and str(raw).strip().lower() != str(p["hvac_mode"]).strip().lower():
                return False
            if "preset_mode" in p:
                cur = attrs.get("preset_mode")
                if not isinstance(cur, str) or cur.strip().lower() != str(p["preset_mode"]).strip().lower():
                    return False
            return True
        return True

    async def _await_expected(self, timeout: float = 4.0) -> None:
        """Wait for controlled entities to settle (bounded).
        If all expected values arrive, continue at once; on timeout
        report goes with optimistic (commanded) value."""
        import asyncio as _aio

        if not getattr(self, "_expected", None):
            return
        t0 = time.monotonic()
        while True:
            missing = self._unmatched_expected()
            if not missing:
                return
            if time.monotonic() - t0 >= timeout:
                _LOGGER.debug(
                    "SzenzorKozpont állapotbevárás lejárt: %s", ",".join(missing))
                return
            await _aio.sleep(0.25)

    def _unmatched_expected(self) -> list[str]:
        """Which watched entities missed expected state yet?"""
        exp = getattr(self, "_expected", None)
        if not isinstance(exp, dict):
            return []
        out = []
        for eid, ov in exp.items():
            try:
                st = self.hass.states.get(eid)
            except Exception:
                st = None
            if st is None or not self._expected_met(st, ov):
                out.append(eid)
        return out

    async def _handle_cmds(self, cmds: list[dict]) -> tuple[list[int], bool, bool]:
        """Returns: (ack ids, need config, need sysinfo)."""
        ack: list[int] = []
        need_config = need_sysinfo = False
        for cmd in cmds:
            try:
                ctype = cmd.get("type")
                cid = int(cmd.get("id", 0))
                arg = cmd.get("arg") or {}
                if ctype == "request_config":
                    need_config = True
                    ack.append(cid)
                elif ctype == "request_sysinfo":
                    need_sysinfo = True
                    ack.append(cid)
                elif ctype == "set_poll":
                    if arg.get("scope") in ("day", "night"):
                        try:
                            v = int(arg.get("value_s", 0))
                            if 60 <= v <= 86400:
                                ov = dict(self.state.get("poll_override", {}))
                                ov[arg["scope"]] = v
                                self.state["poll_override"] = ov
                                ack.append(cid)
                            else:
                                _LOGGER.warning("SzenzorKozpont set_poll érvénytelen: %s", arg.get("value_s"))
                        except (TypeError, ValueError):
                            _LOGGER.warning("SzenzorKozpont set_poll hibás szám")
                    else:
                        _LOGGER.warning("SzenzorKozpont set_poll ismeretlen scope")
                elif ctype == "set_triggers":
                    if self._apply_triggers_cmd(arg):
                        ack.append(cid)
                    # meaningless command: no ack → server resends
                elif ctype == "request_triggers":
                    self.state["triggers_send"] = True
                    ack.append(cid)
                elif ctype == "control":
                    n = arg.get("n", "")
                    if n and await self._exec_control(
                        n, arg.get("state", ""), arg if isinstance(arg, dict) else {}
                    ):
                        ack.append(cid)
                    # failed control: no ack → server resends
                elif ctype == "ota_notice":
                    self._ota_notice(arg.get("version", ""))
                    ack.append(cid)
                elif ctype == "ota_update":
                    if await self._ota_update(arg.get("version", ""), arg.get("url", "")):
                        ack.append(cid)
                    # failed install: no ack → server resends
                else:
                    _LOGGER.warning("SzenzorKozpont ismeretlen parancs: %s", ctype)
            except Exception as err:
                _LOGGER.debug("SzenzorKozpont parancs-kihagyás: %s", type(err).__name__)
        return ack, need_config, need_sysinfo

    # -- triggers ---------------------------------------------------------

    def _apply_triggers_cmd(self, arg: object) -> bool:
        """Apply set_triggers command + persist. Returns: success."""
        try:
            new = apply_triggers(self.state.get("triggers", []), arg)
        except Exception as err:
            _LOGGER.debug("SzenzorKozpont trigger-parancs hiba: %s", type(err).__name__)
            return False
        if new is None:
            _LOGGER.warning("SzenzorKozpont értelmetlen set_triggers parancs")
            return False
        self.state["triggers"] = new
        _LOGGER.warning(
            "SzenzorKozpont trigger-lista frissítve: %d db (%s)",
            len(new), ",".join(t["n"] for t in new) or "üres",
        )
        return True

    def _triggers_payload(self) -> dict:
        """Trigger list to report (on request_triggers)."""
        return {"triggers": self._collect_triggers()}

    def _collect_triggers(self) -> list:
        """Trigger list stored in state (`n` keys).

        Backend-independent: SZKP server only syncs list
        (set_triggers), watching and instant report run on every backend
        from state-stored list.
        """
        out = []
        for t in self.state.get("triggers", []) or []:
            if isinstance(t, dict) and t.get("n"):
                out.append({"n": t["n"]})
        return out

    def _trigger_entities(self) -> set[str]:
        """Watched entity ids from trigger keys (key_entity reversed)."""
        key_entity = self.state.get("key_entity", {}) or {}
        rev = {v: k for k, v in key_entity.items() if isinstance(v, str)}
        out: set[str] = set()
        for t in self.state.get("triggers", []) or []:
            if isinstance(t, dict) and t.get("n") in rev:
                out.add(rev[t["n"]])
        return out

    async def _on_state_event(self, event: object) -> None:
        """HA state_changed event: watched binary transition → instant report."""
        try:
            data = getattr(event, "data", None) or {}
            if not isinstance(data, dict):
                return
            eid = data.get("entity_id", "")
            if not eid or eid not in self._trigger_entities():
                return
            old = data.get("old_state")
            new = data.get("new_state")
            old_s = getattr(old, "state", None)
            new_s = getattr(new, "state", None)
            if not trigger_transition(old_s, new_s):
                return
            _LOGGER.warning(
                "SzenzorKozpont trigger élesedett: %s (%s→%s) — azonnali riport",
                eid, old_s, new_s,
            )
            await self._trigger_refresh()
        except Exception as err:
            _LOGGER.debug("SzenzorKozpont trigger-esemény hiba: %s", type(err).__name__)

    async def _trigger_refresh(self) -> None:
        """Out-of-band full cycle (pull + report) with overlap guard."""
        import asyncio as _aio

        lock = getattr(self, "_trigger_lock", None)
        if lock is None:
            lock = _aio.Lock()
            self._trigger_lock = lock
        if lock.locked():
            _LOGGER.debug("SzenzorKozpont trigger-riport már folyamatban, kihagyva")
            return
        async with lock:
            try:
                await self.async_request_refresh()
            except Exception as err:
                _LOGGER.debug("SzenzorKozpont trigger-riport hiba: %s", type(err).__name__)

    # -- OTA -----------------------------------------------------------------

    def _ota_notice(self, version: str) -> None:
        if not version:
            return
        seen = self.state.get("ota", {}).get("seen", "")
        if seen == version:
            return
        ota = dict(self.state.get("ota", {}))
        ota["seen"] = version
        self.state["ota"] = ota
        _LOGGER.warning("SzenzorKozpont frissítés elérhető: %s", version)
        self._repair("update_available")

    async def _ota_update(self, version: str, url: str, force: bool = False) -> bool:
        """Remote OTA: meta check → download → verify → backup → swap.

        Runs fully only with auto_update (or force=True on manual call);
        otherwise notify only.
        True = closable (ack ok), False = retryable later.
        After file swap entry-reload starts (clean state); NEW CODE
        takes effect at user HA restart.
        """
        import re as _re

        opts = self.entry.options if hasattr(self.entry, "options") else {}
        try:
            metered = bool(opts.get("net_metered", False))
        except AttributeError:
            metered = False
        if metered and not opts.get("allow_ota_on_metered", False):
            _LOGGER.warning("SzenzorKozpont OTA halasztva (mobilnet): %s", version)
            self._ota_notice(version)
            return True  # acknowledged, not an error
        if not _re.match(r"^\d+\.\d+\.\d+$", version or "") or not url:
            _LOGGER.warning("SzenzorKozpont OTA elutasítva (rossz verzió/URL)")
            return True
        if not force and not opts.get("auto_update", False):
            self._ota_notice(version)
            return True
        if not semver_gt(version, VERSION) and version != VERSION:
            _LOGGER.warning("SzenzorKozpont OTA elutasítva (downgrade tiltva): %s", version)
            return True
        try:
            meta = await self.api.ota_check(self.session, "ha", VERSION)
        except api_mod.ApiError as err:
            _LOGGER.warning("SzenzorKozpont OTA meta-lekérés sikertelen: %s", err)
            return False
        if not isinstance(meta, dict) or meta.get("update") is not True:
            return True  # nothing to install
        if str(meta.get("version", "")) != version:
            _LOGGER.warning("SzenzorKozpont OTA verzió-eltérés (parancs vs check)")
            return True
        try:
            data = await self._download_ota(url, meta)
            manifest = await self._run(self._verify_ota_zip, data, version)
            backup = await self._run(self._swap_files, data, manifest)
        except api_mod.ApiError as err:
            _LOGGER.warning("SzenzorKozpont OTA sikertelen: %s", err)
            return False
        except Exception as err:  # file-op error: do not ack
            _LOGGER.warning("SzenzorKozpont OTA fájlhiba: %s: %s", type(err).__name__, err)
            return False
        ota = dict(self.state.get("ota", {}))
        ota.update({"updated_to": version, "backup": backup, "updated_at": int(time.time())})
        self.state["ota"] = ota
        await self._save()
        _LOGGER.warning(
            "SzenzorKozpont %s telepítve (backup: %s). Újraindítás szükséges!",
            version, backup,
        )
        self._repair("restart_required")
        try:
            self.hass.async_create_task(self._reload_self())
        except Exception as err:
            _LOGGER.debug("SzenzorKozpont reload-ütemezés sikertelen: %s", type(err).__name__)
        return True

    async def _reload_self(self) -> None:
        try:
            await self.hass.config_entries.async_reload(self.entry.entry_id)
        except Exception as err:
            _LOGGER.debug("SzenzorKozpont entry-reload sikertelen: %s", type(err).__name__)

    async def async_full_refresh(self) -> None:
        """Forced cycle now, with full report (manual sync button)."""
        self._force_full = True
        await self.async_request_refresh()

    async def ota_update_now(self) -> None:
        """Manual OTA: check now (no 30-day limit) + install.
        Explicit request overrides auto_update switch; metered-net
        guard and version checks still apply.
        No OTA on foreign backend (button not even created then)."""
        if self.backend_kind != BACKEND_SZK:
            _LOGGER.debug("SzenzorKozpont OTA nincs: idegen backend")
            return
        try:
            meta = await self.api.ota_check(self.session, "ha", VERSION)
        except api_mod.ApiError as err:
            _LOGGER.warning("SzenzorKozpont kézi OTA-ellenőrzés sikertelen: %s", err)
            return
        if not isinstance(meta, dict) or meta.get("update") is not True:
            _LOGGER.warning("SzenzorKozpont kézi OTA: nincs elérhető frissítés")
            return
        version = str(meta.get("version", ""))
        url = str(meta.get("url", ""))
        ok = await self._ota_update(version, url, force=True)
        if not ok:
            _LOGGER.warning("SzenzorKozpont kézi OTA sikertelen, később újrapróbálható")

    async def _run(self, func: Any, *args: Any) -> Any:
        import asyncio as _aio

        return await _aio.to_thread(func, *args)

    async def _download_ota(self, url: str, meta: dict) -> bytes:
        """Download with API key, size caps. Accepts relative URL too."""
        import ipaddress as _ip
        from urllib.parse import urlparse as _up

        full = self.api.full_url(url)
        parts = _up(full)
        try:
            verify = bool(self.entry.options.get("verify_ssl", True))
        except AttributeError:
            verify = True
        if parts.scheme != "https":
            # Exception: local testnet (loopback) with verify_ssl=false.
            # Prod always https.
            if verify or (parts.hostname or "") not in ("127.0.0.1", "localhost", "::1"):
                raise api_mod.ApiError("csak https URL")
        try:
            is_ip = bool(_ip.ip_address(parts.hostname or ""))
        except ValueError:
            is_ip = False
        if is_ip:
            if verify:
                raise api_mod.ApiError("IP-című URL tiltva")
        if "." not in (parts.hostname or ""):
            raise api_mod.ApiError("rossz hosztnév")
        expected = int(meta.get("size", 0) or 0)
        timeout = api_mod.aiohttp.ClientTimeout(total=120)
        buf = bytearray()
        try:
            async with self.session.get(
                full,
                headers=self.api.auth_headers(),
                ssl=(parts.scheme == "https"),
                timeout=timeout,
            ) as resp:
                if resp.status != 200:
                    raise api_mod.ApiError(f"letöltés HTTP {resp.status}")
                clen = resp.headers.get("Content-Length")
                if expected and clen is not None:
                    try:
                        if abs(int(clen) - expected) > max(1, expected // 20):
                            raise api_mod.ApiError("méret-eltérés")
                    except (TypeError, ValueError) as err:
                        raise api_mod.ApiError("rossz Content-Length") from err
                async for chunk in resp.content.iter_chunked(65536):
                    buf.extend(chunk)
                    if len(buf) > 8 * 1024 * 1024:
                        raise api_mod.ApiError("túl nagy fájl (8 MB plafon)")
        except api_mod.ApiError:
            raise
        except Exception as err:
            raise api_mod.ApiError(f"letöltési hiba: {type(err).__name__}") from err
        if expected and abs(len(buf) - expected) > max(1, expected // 20):
            raise api_mod.ApiError("méret-eltérés (tényleges)")
        if not buf:
            raise api_mod.ApiError("üres fájl")
        return bytes(buf)

    @staticmethod
    def _verify_ota_zip(data: bytes, version: str) -> dict:
        """ZIP integrity + manifest match. Returns manifest."""
        import io as _io
        import json as _json
        import zipfile as _zf

        try:
            zf = _zf.ZipFile(_io.BytesIO(data))
            bad = zf.testzip()
            if bad is not None:
                raise api_mod.ApiError(f"sérült ZIP: {bad}")
            names = zf.namelist()
            mf = "manifest.json" if "manifest.json" in names else None
            if mf is None:
                for n in names:
                    if n.endswith("/manifest.json") and n.count("/") == 1:
                        mf = n
                        break
            if mf is None:
                raise api_mod.ApiError("nincs manifest.json")
            manifest = _json.loads(zf.read(mf).decode("utf-8"))
        except api_mod.ApiError:
            raise
        except Exception as err:
            raise api_mod.ApiError(f"ZIP-hiba: {type(err).__name__}") from err
        if not isinstance(manifest, dict) or manifest.get("domain") != DOMAIN:
            raise api_mod.ApiError("rossz manifest (domain)")
        if str(manifest.get("version", "")) != version:
            raise api_mod.ApiError("manifest-verzió nem egyezik")
        return manifest

    @staticmethod
    def _swap_files(data: bytes, manifest: dict) -> str:
        """Backup + unpack into package dir. Returns backup path."""
        import io as _io
        import pathlib as _pl
        import shutil as _sh
        import zipfile as _zf

        pkg = _pl.Path(__file__).resolve().parent
        try:
            cur_ver = _manifest_version(pkg)
        except Exception:
            cur_ver = VERSION
        backup = pkg.parent / f"szenzorkozpont_backup_{cur_ver}"
        if backup.exists():
            _sh.rmtree(backup, ignore_errors=True)
        _sh.copytree(pkg, backup, ignore=_sh.ignore_patterns("__pycache__"))
        with _zf.ZipFile(_io.BytesIO(data)) as zf:
            names = zf.namelist()
            rooted = [n for n in names if n.endswith("/manifest.json")]
            if rooted and "manifest.json" not in names:
                root = rooted[0].rsplit("/", 1)[0] + "/"
                for n in names:
                    if n.startswith(root) and not n.endswith("/"):
                        target = pkg / n[len(root):]
                        target.parent.mkdir(parents=True, exist_ok=True)
                        with zf.open(n) as src, open(target, "wb") as dst:
                            _sh.copyfileobj(src, dst)
            else:
                zf.extractall(pkg)
        if _manifest_version(pkg) != manifest.get("version"):
            raise api_mod.ApiError("csere utáni verzió-ellenőrzés sikertelen")
        return str(backup)

    async def _maybe_ota_check(self) -> None:
        last = int(self.state.get("ota", {}).get("last_check", 0))
        if time.time() - last < 30 * 86400:
            return
        try:
            data = await self.api.ota_check(self.session, "ha", VERSION)
        except api_mod.ApiError as err:
            _LOGGER.debug("SzenzorKozpont ota/check hiba: %s", err)
            return
        ota = dict(self.state.get("ota", {}))
        ota["last_check"] = int(time.time())
        self.state["ota"] = ota
        if isinstance(data, dict) and data.get("update") is True:
            self._ota_notice(str(data.get("version", "")))

    # -- main cycle -------------------------------------------------------------

    async def _async_update_foreign(self) -> dict:
        """Foreign-backend cycle: HA scan → telemetry send, no pull.

        SZKP protocol (pull/commands/config/sysinfo/OTA/quota) does not
        exist on foreign cloud; polling settings live on HA options
        UI (entry.options), trigger watch on state-stored
        list — both backend-independent.
        """
        interval, _scope = self._interval()
        self.update_interval = timedelta(seconds=interval)
        if self.backend is None:
            raise self._fail(
                f"hiányzó {self.backend_kind} backend-konfiguráció",
                self._backoff(interval),
            )
        report, _config, _new_key = self._collect()
        bt = int(time.time())
        items = _report_items(report)
        try:
            await self.backend.send_telemetry(items, bt, session=self.session)
        except api_mod.ApiAuthError as err:
            raise self._fail_auth() from err
        except api_mod.ApiRateLimitError as err:
            raise self._fail_rate(err.retry_after, interval, str(err)) from err
        except api_mod.ApiError as err:
            raise self._fail(f"riport-hiba: {err}", self._backoff(interval)) from err
        next_s = interval
        if not items and not self._first_send_done:
            # After empty initial send earlier next cycle
            # (max 120 s) — see SZKP path.
            next_s = min(interval, 120)
            self.update_interval = timedelta(seconds=next_s)
        self._first_send_done = True
        self._ok()
        self.last_report_ok = datetime.now().astimezone().isoformat(timespec="seconds")
        await self._save()
        _LOGGER.debug(
            "SzenzorKozpont idegen backend ciklus OK (%s): %d tétel",
            self.backend_kind, len(items),
        )
        return {
            "last_report_ok": self.last_report_ok,
            "next_poll_s": next_s,
            "clock_drift": self.clock_drift,
            "sent": len(items),
            "same": 0,
            "pending_cmds": 0,
        }

    async def _async_update_data(self) -> dict:
        self.apply_log_level()
        # Leftover expectation from prev cycle (e.g. error
        # abort) must not carry over: always start fresh.
        self._expected = {}
        interval, _scope = self._interval()
        self.update_interval = timedelta(seconds=interval)

        if self.backend_kind != BACKEND_SZK:
            return await self._async_update_foreign()

        try:
            pull = await self.api.pull(self.session, int(self.state.get("config_rev", 0)))
        except api_mod.ApiAuthError as err:
            raise self._fail_auth() from err
        except api_mod.ApiRateLimitError as err:
            raise self._fail_rate(err.retry_after, interval, str(err)) from err
        except api_mod.ApiError as err:
            raise self._fail(f"pull-hiba: {err}", self._backoff(interval)) from err

        srv_ts = pull.get("srv_ts")
        if isinstance(srv_ts, int):
            drift = clock_drift_srv(srv_ts, int(time.time()))
            self.clock_drift = drift
            if abs(drift) > 300:
                _LOGGER.warning("SzenzorKozpont óraeltolódás: %d mp", drift)
        cmds = pull.get("cmds", [])
        if not isinstance(cmds, list):
            raise self._fail("rossz pull-alak")

        ack, need_config, need_sysinfo = await self._handle_cmds(cmds)

        # After control wait until HA state machine settles to
        # commanded value (slow device, e.g. Zigbee/MQTT: service
        # return does not mean entity state refreshed yet).
        # If arrives in time, it goes in report; if not, report
        # carries actually read (old) value, no guessing —
        # next polling cycle sends real value.
        if getattr(self, "_expected", None):
            await self._await_expected()

        # Recalc right after set_poll: HA scheduler and
        # "Next polling" sensor must show fresh value (calc above
        # ran BEFORE command processing).
        interval, _scope = self._interval()
        self.update_interval = timedelta(seconds=interval)

        report, config, new_key = self._collect()
        fp = config_fingerprint(config)
        force_config = self._startup and self._debug_on()
        self._startup = False
        # SEND CONFIG = (1) server explicitly asks (request_config),
        # or (2) config STRUCTURE changed vs last successfully
        # sent (new/gone sensor, label, unit, group,
        # control descriptor). Same content NOT resent: config
        # attrs carry per-cycle measurements (rssi,
        # median, mean, last_mean_of, last_packet_id, battery, current
        # temp...), which made old rev- and "new key"-based
        # condition resend every cycle. Rev diff alone
        # is NOT a reason: server always sends request_config for that.
        changed = fp != str(self.state.get("config_fp", ""))
        if need_config or changed:
            _LOGGER.debug(
                "SzenzorKozpont config küldése: %s",
                "szerver kérte" if need_config else "változott a tartalom",
            )
            try:
                last = await self.api.config(self.session, build_config_items(config))
                if isinstance(last.get("allowed"), list):
                    if last["allowed"] != self.state.get("allowed"):
                        # Sendable set changed (e.g. approve/deny):
                        # server did not store dropped values so far,
                        # so next report must be full.
                        self._force_full = True
                    self.state["allowed"] = last["allowed"]
                if isinstance(last.get("denied"), list):
                    self.state["denied"] = last["denied"]
                if isinstance(last.get("cfg_rev"), int):
                    self.state["config_rev"] = last["cfg_rev"]
                elif isinstance(pull.get("cfg_rev"), int):
                    # Response lacks revision: record server rev seen in
                    # pull, else next pull looks stale again and server
                    # asks config again (infinite loop).
                    self.state["config_rev"] = pull["cfg_rev"]
                # Fingerprint updates only after SUCCESSFUL send →
                # retry next cycle on error.
                self.state["config_fp"] = fp
            except api_mod.ApiAuthError as err:
                raise self._fail_auth() from err
            except api_mod.ApiRateLimitError as err:
                raise self._fail_rate(err.retry_after, interval, str(err)) from err
            except api_mod.ApiError as err:
                raise self._fail(f"config-hiba: {err}", self._backoff(interval)) from err

        elif force_config:
            _LOGGER.debug(
                "SzenzorKozpont indulási kényszer-config kihagyva "
                "(a tartalom nem változott a legutóbbi küldés óta)"
            )

        # sysinfo send = (1) server asks (request_sysinfo), OR
        # (2) day/night polling changed on HA options UI
        # (flag `sysinfo_pending` — set by `__init__` at restart).
        pending_poll = bool(self.state.get("sysinfo_pending"))
        if need_sysinfo or pending_poll:
            try:
                await self.api.sysinfo(self.session, await self._async_sysinfo())
                if pending_poll:
                    self.state["sysinfo_pending"] = False
                    _LOGGER.warning(
                        "SzenzorKozpont sysinfo elküldve (a beállítófelületen "
                        "módosult a polling)"
                    )
            except api_mod.ApiError as err:
                _LOGGER.debug("SzenzorKozpont sysinfo-hiba: %s", err)

        # Report trigger list on server request_triggers command
        # (flag kept until successful send — next cycle retries).
        if self.state.get("triggers_send"):
            try:
                await self.api.triggers(self.session, self._triggers_payload())
                self.state["triggers_send"] = False
            except api_mod.ApiError as err:
                _LOGGER.debug("SzenzorKozpont trigger-jelentés hiba: %s", err)

        # Report from items of first (full) snapshot, controlled
        # entities refreshed targeted (no second full scan).
        # Whatever stays stale, next cycle fixes.
        if self._expected:
            # Targeted re-read of controlled entities, no full scan;
            # first snapshot sufficed for config decision. Same key:
            # fresher item wins; new key goes last.
            _fresh, _, _ = self._collect(only_eids=set(self._expected))
            _by_n = {}
            for _e in _fresh:
                if isinstance(_e, dict) and isinstance(_e.get("n"), str):
                    _by_n[_e["n"]] = _e
            if _by_n:
                _seen: set[str] = set()
                _merged = []
                for _e in report:
                    _n = _e.get("n") if isinstance(_e, dict) else None
                    if _n in _by_n:
                        _merged.append(_by_n[_n])
                        _seen.add(_n)
                    else:
                        _merged.append(_e)
                for _n, _e in _by_n.items():
                    if _n not in _seen:
                        _merged.append(_e)
                report = _merged
        self._expected = {}
        bt = int(time.time())
        items = _report_items(report)
        # Change-based report: only changed values go, rest
        # go to `same` list (server refreshes last_seen on them
        # without point storage). Full report: change_only off,
        # manual sync / first after restart / 12 h heartbeat.
        change_only = bool(self._opts("change_only", True))
        full = (not change_only) or bool(getattr(self, "_force_full", False))
        if change_only and not full:
            try:
                last_full = int(self.state.get("last_full_ts", 0) or 0)
            except (TypeError, ValueError):
                last_full = 0
            full = (bt - last_full) >= FULL_REPORT_EVERY_S
        last = self.state.get("last_sent", {})
        if not isinstance(last, dict):
            last = {}
        body_entries, same_keys, new_fps = split_changed(items, last, full)
        try:
            if self.backend_kind != BACKEND_SZK:
                # Foreign backend: no ack/same/bn — backend builds own
                # payload from items (telemetry + attribute,
                # or SenML array).
                await self.backend.send_telemetry(body_entries, bt, session=self.session)
            else:
                # bn optimization: common prefix (e.g. sensor.)
                from .protocol import common_prefix

                keys = [x["n"] for x in body_entries]
                bn = common_prefix(keys)
                if bn and len(bn) * len(keys) < 12:
                    bn = ""
                await self.api.report(self.session, ack, bt, body_entries, bn=bn,
                                      same=same_keys)
        except api_mod.ApiAuthError as err:
            raise self._fail_auth() from err
        except api_mod.ApiRateLimitError as err:
            raise self._fail_rate(err.retry_after, interval, str(err)) from err
        except api_mod.ApiError as err:
            raise self._fail(f"report-hiba: {err}", self._backoff(interval)) from err
        # Fingerprints and full flags take effect only after SUCCESSFUL
        # send — else next cycle would wrongly see unsent values as
        # unchanged. Empty collect (e.g. no HA state at boot yet) must NOT
        # overwrite old fingerprints — else next cycle resends all needlessly.
        if items:
            self.state["last_sent"] = new_fps
        if full and items:
            # Empty cycle must not consume "full report" status either:
            # first REAL (non-empty) send after restart must
            # be full — empty boot cycle does not count.
            self.state["last_full_ts"] = bt
            self._force_full = False

        next_s = interval
        if not items and not self._first_send_done:
            # First send after boot has 0 items: next cycle
            # comes sooner (max 120 s), so real data goes up
            # ASAP. Already-shorter polling not stretched.
            # Later cycles run on original schedule (cycle-start
            # recalc restores interval).
            next_s = min(interval, 120)
            self.update_interval = timedelta(seconds=next_s)
        self._first_send_done = True

        # No OTA outside SZKP (updates via HA manager).
        if self.backend_kind == BACKEND_SZK:
            await self._maybe_ota_check()
        self._ok()
        self.last_report_ok = datetime.now().astimezone().isoformat(timespec="seconds")
        await self._save()
        _LOGGER.debug(
            "SzenzorKozpont ciklus OK: pull %d parancs, %d ack, riport %d tétel",
            len(cmds), len(ack), len(body_entries),
        )
        return {
            "last_report_ok": self.last_report_ok,
            "next_poll_s": next_s,
            "clock_drift": self.clock_drift,
            "sent": len(body_entries),
            "same": len(same_keys),
            "pending_cmds": len(cmds) - len(ack),
        }

    def _sysinfo(self) -> dict:
        interval, scope = self._interval()
        day_s, night_s = self._poll_cfg()
        return {
            "poll": {
                "day_s": day_s,
                "night_s": night_s,
                "scope": scope,
                "interval_s": interval,
                "override": dict(self.state.get("poll_override", {})),
            },
            "client_type": "ha",
            "fw_v": VERSION,
            "variant": "",
            "ha_v": _ha_version(self.hass),
            "cpu_arch": "unknown",
            "storage_free_mb": -1,
            "uptime_s": -1,
            "net_metered": False,
        }

    def _config_dir(self) -> str:
        """HA config dir for free-space measure (empty in test stub)."""
        try:
            cfg = getattr(self.hass, "config", None)
            p = cfg.path() if cfg is not None else None
            if p:
                return str(p)
        except Exception:
            pass
        return ""

    async def _async_sysinfo(self) -> dict:
        """Sysinfo packet filled with real host data (rare)."""
        body = self._sysinfo()
        try:
            host = await self._run(_host_info, self._config_dir())
        except Exception:
            host = {}
        if not isinstance(host, dict):
            host = {}
        if host.get("cpu"):
            body["cpu"] = host["cpu"]
        if host.get("runtime"):
            body["runtime"] = host["runtime"]
        if host.get("arch"):
            body["cpu_arch"] = host["arch"]
        if isinstance(host.get("storage_free_mb"), int):
            body["storage_free_mb"] = host["storage_free_mb"]
        try:
            body["uptime_s"] = max(0, int(time.time() - self._boot_ts))
        except Exception:
            pass
        return body
