"""SzenzorKozpont — pure (HA-independent) protocol logic.

All functions deterministic and I/O-free, so plain unittest
covers them. Network is in api.py, HA binding in coordinator.py.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import logging
import re
import unicodedata
from datetime import datetime, time as dtime

from .const import (
    CONTROL_DOMAINS,
    DEFAULT_DAY_FROM,
    DEFAULT_DAY_TO,
    DEFAULT_POLL_DAY_S,
    DEFAULT_POLL_NIGHT_S,
    MAX_ATTRS_PER_SENSOR,
    MAX_ATTR_BYTES,
    MAX_CONFIG_BYTES,
    MAX_CONFIG_ITEMS,
    MAX_REPORT_BYTES,
    SKIP_ATTRS,
)

_LOGGER = logging.getLogger(__name__)

_KEY_OK = re.compile(r"^[A-Za-z0-9_.\-]{1,50}$")
_WS = re.compile(r"\s+")

# accent → ascii (name rule allows only [a-zA-Z0-9_.-])
_ACCENTS = str.maketrans({
    "á": "a", "é": "e", "í": "i", "ó": "o", "ö": "o", "ő": "o",
    "ú": "u", "ü": "u", "ű": "u",
    "Á": "A", "É": "E", "Í": "I", "Ó": "O", "Ö": "O", "Ő": "O",
    "Ú": "U", "Ü": "U", "Ű": "U", "ß": "ss", "ä": "a", "Ä": "A",
})


def normalize_key(raw: str) -> str:
    """Valid sensor_key/group_key from arbitrary text."""
    s = raw.translate(_ACCENTS)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = _WS.sub("_", s.strip())
    s = re.sub(r"[^A-Za-z0-9_.\-]", "_", s)
    s = re.sub(r"_+", "_", s).strip("_.")
    return s[:50] or "szenzor"


def unique_key(base: str, taken: set[str]) -> str:
    """On collision number it: base, base_2, ... (max 50 chars)."""
    if base not in taken:
        return base
    i = 2
    while True:
        cand = f"{base[:47]}_{i}" if len(base) > 47 else f"{base}_{i}"
        if cand not in taken:
            return cand
        i += 1


def sensor_key_for(domain: str, object_id: str, taken: set[str]) -> str:
    return unique_key(normalize_key(f"{domain}.{object_id}"), taken)


def valid_key(s: str) -> bool:
    return bool(s) and _KEY_OK.match(s) is not None


def classify(domain: str) -> str:
    """telemetry or control by domain."""
    return "control" if domain in CONTROL_DOMAINS else "telemetry"


_BOOL_TRUE = frozenset({"on", "true", "yes", "home", "open", "locked", "1"})
_BOOL_FALSE = frozenset({"off", "false", "no", "away", "closed", "unlocked", "0"})
SKIP_STATES = frozenset({"unknown", "unavailable", ""})


def bool_of_state(state: object) -> bool | None:
    """HA state → True/False, or None if not binary-readable."""
    if not isinstance(state, str):
        return None
    low = state.strip().lower()
    if low in _BOOL_TRUE:
        return True
    if low in _BOOL_FALSE:
        return False
    return None


def encode_value(state: str) -> tuple[str, object] | None:
    """HA state → (v|vs|vb, value). None to skip.

    Order: number → bool words → string. 'none'/'null' text goes as string.
    NaN/Inf cannot go as number (would make invalid JSON) → string.
    """
    import math as _math

    s = state.strip()
    if s.lower() in SKIP_STATES:
        return None
    try:
        # "1"/"0" go as number, not bool
        if "." in s or "e" in s.lower():
            num = float(s)
        else:
            num = float(int(s, 10))
        if _math.isfinite(num):
            return ("v", num)
    except ValueError:
        pass
    low = s.lower()
    if low in _BOOL_TRUE:
        return ("vb", True)
    if low in _BOOL_FALSE:
        return ("vb", False)
    if len(s.encode("utf-8")) > 250:
        return None
    return ("vs", s)


def dtype_of(field: str) -> str:
    return {"v": "float", "vs": "string", "vb": "bool"}[field]


def entry_sig(item: dict) -> str:
    """Report-item fingerprint for change detection (stable JSON string).

    Field + value + params count (name not: it is dict key). Empty
    string if no readable value field — such item always sent.
    """
    for field in ("v", "vs", "vb"):
        if field in item:
            return json.dumps({"f": field, "v": item[field], "p": item.get("p")},
                              sort_keys=True, separators=(",", ":"),
                              ensure_ascii=False, default=str)
    return ""


def split_changed(items: list[dict], last: dict, full: bool) -> tuple[list[dict], list[str], dict]:
    """Split items for change-based report.

    items: built report items [{"n":..., field:..., ["p":...]}].
    last: previously sent fingerprints {key: sig}.
    full: full report (first/heartbeat/manual/change_only off).
    Returns: (items to send, unchanged keys for `same` list,
    new fingerprint map). Map holds only currently seen keys
    — vanished sensor's dropped (resent on reappear).
    """
    send: list[dict] = []
    same: list[str] = []
    new_fps: dict = {}
    for it in items:
        if not isinstance(it, dict) or not isinstance(it.get("n"), str):
            continue
        sig = entry_sig(it)
        if full:
            send.append(it)
            if sig:
                new_fps[it["n"]] = sig
            continue
        if sig and last.get(it["n"]) == sig:
            same.append(it["n"])
            new_fps[it["n"]] = sig
        else:
            send.append(it)
            if sig:
                new_fps[it["n"]] = sig
    return send, same, new_fps


def common_prefix(keys: list[str]) -> str:
    """Longest common prefix up to last '.' (for bn)."""
    if len(keys) < 2:
        return ""
    pref = keys[0]
    for k in keys[1:]:
        i = 0
        while i < len(pref) and i < len(k) and pref[i] == k[i]:
            i += 1
        pref = pref[:i]
        if not pref:
            return ""
    cut = pref.rfind(".")
    return pref[: cut + 1] if cut > 0 else ""


def build_report(
    entries: list[tuple],
    bt: int,
    max_bytes: int = MAX_REPORT_BYTES,
) -> dict:
    """SenML-light report: {bt, [bn], e:[{n,t?,v|vs|vb,p?}]}.

    entries: (resolved key, field, value[, params-dict]). t always 0
    (single moment), so omitted. On oversize trims tail, and
    logs dropped keys (warning) — instead of silent data loss.
    """
    keys = [k for k, _, *_ in entries]
    bn = common_prefix(keys)
    # bn only pays off if body gets shorter
    if bn and len(bn) * len(keys) < 12:
        bn = ""
    e: list[dict] = []
    for t in entries:
        k, f, v = t[0], t[1], t[2]
        name = k[len(bn):] if bn and k.startswith(bn) else k
        item: dict = {"n": name, f: v}
        if len(t) > 3 and isinstance(t[3], dict) and t[3]:
            item["p"] = t[3]
        e.append(item)
    body: dict = {"bt": bt, "e": e}
    if bn:
        body["bn"] = bn
    blob = json.dumps(body, separators=(",", ":")).encode()
    if len(blob) > max_bytes and e:
        names = [x["n"] for x in e]
        while len(blob) > max_bytes and e:
            e.pop()
            body["e"] = e
            blob = json.dumps(body, separators=(",", ":")).encode()
        dropped = names[len(e):]
        _LOGGER.warning(
            "SzenzorKozpont riport csonkolva: %d tétel eldobva (%s)%s",
            len(dropped), ",".join(dropped[:10]),
            ", ..." if len(dropped) > 10 else "")
    return body


def filter_entries(
    keys: list[str],
    allowed: list[str],
    denied: list[str],
) -> tuple[list[str], bool]:
    """Filter + has unknown (config-needing) key.

    empty allowed = send all (except denied).
    """
    aset = set(allowed)
    dset = set(denied)
    out = [k for k in keys if k not in dset and (not aset or k in aset)]
    # Unknown = allowlist exists, and key in neither allow nor deny.
    # (Empty allowed = "send all" mode: nothing unknown there.)
    unknown = bool(aset) and any(k not in aset and k not in dset for k in keys)
    return out, unknown


# Only Structural (non-measurement) fields count from attrs
# for fingerprint: device/area/unique ID and device_class
# belong here. Everything else (rssi, median, mean, last_mean_of,
# last_packet_id, battery, current temp, hvac_action,
# preset_mode, available...) changes per cycle, so excluded.
_FP_ATTRS = frozenset({"device_class", "ha_device_id", "ha_area", "unique_id"})


def config_fingerprint(items: list[dict]) -> str:
    """Structural fingerprint of config (SHA-256; content-based, order-free).

    Includes: `n`, `label`, `u`, `kind`, `dtype`, `group_key`,
    `group_label`, `ctl` + only `_FP_ATTRS` subset from attrs.
    Excludes: every measurement/counter-derived attrs value — without
    that each submit would differ, and client would resend config
    every cycle. If keys/labels/or any structural field above
    changed, fingerprint changes too → send (even unrequested).
    """
    slim: list[dict] = []
    for it in items:
        d = {k: v for k, v in it.items() if k != "attrs"}
        attrs = it.get("attrs")
        if isinstance(attrs, dict):
            keep = {k: v for k, v in attrs.items() if k in _FP_ATTRS}
            if keep:
                d["attrs"] = keep
        slim.append(d)
    slim.sort(key=lambda d: str(d.get("n", "")))
    blob = json.dumps(slim, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def build_config_items(
    sensors: list[dict],
    max_items: int = MAX_CONFIG_ITEMS,
) -> list[list[dict]]:
    """Sensor-meta list chunking (part/parts is caller's job)."""
    chunks = [sensors[i:i + max_items] for i in range(0, max(1, len(sensors)), max_items)]
    return chunks


def encode_body(payload: dict, max_bytes: int) -> tuple[bytes, bool]:
    """JSON + gzip above 16 KB. Returns: (body, gzipped?)."""
    raw = json.dumps(payload, separators=(",", ":")).encode()
    if len(raw) > 16384:
        return gzip.compress(raw), True
    if len(raw) > max_bytes:
        raise ValueError("túl nagy test gzip nélkül is")
    return raw, False


def parse_window(hhmm: str, default: str) -> dtime:
    try:
        h, m = hhmm.split(":")
        return dtime(int(h), int(m))
    except (ValueError, AttributeError):
        h, m = default.split(":")
        return dtime(int(h), int(m))


def is_day(now: datetime, day_from: str = DEFAULT_DAY_FROM, day_to: str = DEFAULT_DAY_TO) -> bool:
    """06:00 <= now <= 21:00 → daytime (no overlap). Midnight wrap supported."""
    t = now.time()
    start = parse_window(day_from, DEFAULT_DAY_FROM)
    end = parse_window(day_to, DEFAULT_DAY_TO)
    if start <= end:
        return start <= t <= end
    return t >= start or t <= end


def poll_cfg(options, data) -> dict:
    """Entry day/night polling settings, normalized.

    Source: HA options UI (`entry.options`) or first install
    (`entry.data`); invalid/missing value → default, 60–86400 s.
    """

    def _pick(key: str, default: int) -> int:
        try:
            raw = (options or {}).get(key, (data or {}).get(key, default))
        except AttributeError:
            return default
        try:
            v = int(raw)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return default
        return v if 60 <= v <= 86400 else default

    return {
        "day_s": _pick("poll_day_s", DEFAULT_POLL_DAY_S),
        "night_s": _pick("poll_night_s", DEFAULT_POLL_NIGHT_S),
    }


def poll_cfg_changed(prev, cur: dict) -> bool:
    """Did HA options-UI polling values change at restart?

    `prev` = pair seen at last startup (`state["poll_cfg_last"]`).
    None (first start / wiped state) → False: no self-sent
    sysinfo, server asks for it (if never received).
    """
    if not isinstance(prev, dict) or not prev:
        return False
    for key in ("day_s", "night_s"):
        if key not in prev:
            continue  # no old sample for this key → no signal
        try:
            if int(prev[key]) != int(cur[key]):  # type: ignore[index]
                return True
        except (TypeError, ValueError, KeyError):
            return True
    return False


def poll_interval(
    now: datetime,
    day_s: int = DEFAULT_POLL_DAY_S,
    night_s: int = DEFAULT_POLL_NIGHT_S,
    day_from: str = DEFAULT_DAY_FROM,
    day_to: str = DEFAULT_DAY_TO,
    override: dict | None = None,
) -> tuple[int, str]:
    """(s, 'day'|'night'). Fail-safe default on invalid value."""
    if is_day(now, day_from, day_to):
        scope, val, default = "day", day_s, DEFAULT_POLL_DAY_S
    else:
        scope, val, default = "night", night_s, DEFAULT_POLL_NIGHT_S
    # Override comes in two shapes: new {"day": 600, "night": 1200}
    # (per-scope key, stored by set_poll) and old
    # {"scope": "day", "value_s": 600} — parse both, else
    # set_poll command acks but schedule never changes.
    if override:
        raw = override.get(scope)
        if raw is None and override.get("scope") == scope:
            raw = override.get("value_s")
        try:
            v = int(raw)  # type: ignore[arg-type]
            if 60 <= v <= 86400:
                return v, scope
        except (TypeError, ValueError):
            pass
    try:
        v = int(val)
        if 60 <= v <= 86400:
            return v, scope
    except (TypeError, ValueError):
        pass
    return default, scope


TRIGGER_MAX = 10


def trigger_transition(old: object, new: object) -> bool:
    """Binary transition happened (ON→OFF or OFF→ON)?
    Only the two meaningful directions fire trigger; unknown/unavailable
    or attr change (same value) does not."""
    if not isinstance(old, str) or not isinstance(new, str):
        return False
    o, n = old.strip().lower(), new.strip().lower()
    return (o, n) in (("on", "off"), ("off", "on"))


def normalize_triggers(items: object) -> list[dict]:
    """Clean trigger list: [{"n": key}, ...], max TRIGGER_MAX.
    Only valid sensor keys kept, order-preserving, no dups."""
    out: list[dict] = []
    if not isinstance(items, list):
        return out
    for t in items:
        if not isinstance(t, dict):
            continue
        n = t.get("n")
        if not isinstance(n, str) or not valid_key(n):
            continue
        if all(e["n"] != n for e in out):
            out.append({"n": n})
        if len(out) >= TRIGGER_MAX:
            break
    return out


def apply_triggers(current: object, arg: object) -> list[dict] | None:
    """Process set_triggers command. Returns new list, or None if
    arg meaningless (then no ack).
    full=True (or missing full) → REPLACE; full=False → extend (union)."""
    if not isinstance(arg, dict):
        return None
    items = normalize_triggers(arg.get("triggers"))
    if arg.get("full", True):
        return items
    cur = normalize_triggers(current)
    seen = {e["n"] for e in cur}
    for e in items:
        if e["n"] not in seen:
            cur.append(e)
            seen.add(e["n"])
        if len(cur) >= TRIGGER_MAX:
            break
    return cur[:TRIGGER_MAX]


def control_service(sensor_key: str) -> tuple[str, str] | None:
    """sensor_key → (domain, service base). Caller appends state."""
    domain = sensor_key.split(".")[0] if "." in sensor_key else ""
    if domain in ("switch", "light", "input_boolean"):
        return domain, domain  # switch.turn_on / light.toggle ...
    if domain in ("input_text", "input_number"):
        return domain, "set_value"
    if domain == "button":
        return "button", "press"
    if domain == "scene":
        return "scene", "turn_on"
    return None


def control_service_call(
    sensor_key: str, state: str, params: dict | None = None
) -> tuple[str, str, dict] | None:
    """(domain, service, data). state: on/off/toggle; params for on only."""
    base = control_service(sensor_key)
    if base is None:
        return None
    domain, svc = base
    if svc == "press":
        return domain, "press", {}
    if domain in ("input_text", "input_number"):
        if state == "":
            return None
        if domain == "input_number":
            try:
                num = float(state)
            except (TypeError, ValueError):
                return None
            return domain, "set_value", {"value": num}
        if len(state.encode("utf-8")) > 250:
            return None
        return domain, "set_value", {"value": state}
    if state not in ("on", "off", "toggle"):
        return None
    if state == "off":
        return domain, "turn_off", {}
    if state == "toggle":
        return domain, "toggle", {}
    data: dict = {}
    for k, v in (params or {}).items():
        if k == "brightness" and isinstance(v, (int, float)) and 0 <= v <= 255:
            data[k] = int(v)
        elif k in ("rgb_color", "rgbw_color", "rgbww_color") and isinstance(v, (list, tuple)):
            need = 3 if k == "rgb_color" else 4
            if len(v) == need and all(isinstance(c, (int, float)) and 0 <= c <= 255 for c in v):
                data[k] = [int(c) for c in v]
        elif k == "color_temp_kelvin" and isinstance(v, (int, float)) and 1000 <= v <= 10000:
            data[k] = int(v)
        elif k == "effect" and isinstance(v, str) and v and len(v) <= 32:
            data[k] = v
    return domain, "turn_on", data


LIGHT_PARAM_KEYS = ("brightness", "rgb_color", "rgbw_color", "rgbww_color", "color_temp_kelvin", "effect")

CLIMATE_PARAM_KEYS = ("temperature", "hvac_mode", "preset_mode")

_KNOWN_HVAC_MODES = frozenset({"off", "heat", "cool", "auto", "dry", "fan_only"})

_KNOWN_COLOR_MODES = frozenset({
    "brightness", "onoff", "color_temp", "hs", "xy",
    "rgb", "rgbw", "rgbww", "white",
})


def light_ctl(attrs: dict) -> dict | None:
    """Lamp capability descriptor for config (supported_color_modes/effect_list)."""
    modes = attrs.get("supported_color_modes")
    if not isinstance(modes, (list, tuple)):
        return None
    modes = [m for m in modes if isinstance(m, str) and m in _KNOWN_COLOR_MODES][:10]
    if not modes:
        return None
    ctl: dict = {
        "type": "light",
        "brightness": bool("brightness" in modes or "brightness" in attrs),
        "color_modes": modes,
    }
    eff = attrs.get("effect_list")
    if isinstance(eff, (list, tuple)):
        fx = [str(x)[:32] for x in eff if isinstance(x, str) and x][:10]
        if fx:
            ctl["effects"] = fx
    return ctl


def light_state_params(attrs: dict, last: dict | None = None) -> dict:
    """Current lamp state into p object (for report).

    Off lamp gives no `brightness`/color attrs — then last
    known value (`last`: p part of earlier on-state)
    fills missing keys, so server/app knows last
    state. Live attr always overrides memory; bad
    `last` values skipped.
    """
    p: dict = {}
    b = attrs.get("brightness")
    if isinstance(b, bool):
        pass
    elif isinstance(b, (int, float)) and 0 <= b <= 255:
        p["brightness"] = int(b)
    for k, ln in (("rgb_color", 3), ("rgbw_color", 4), ("rgbww_color", 4)):
        v = attrs.get(k)
        if (
            isinstance(v, (list, tuple))
            and len(v) == ln
            and all(isinstance(c, (int, float)) and 0 <= c <= 255 for c in v)
        ):
            p[k] = [int(c) for c in v]
    if isinstance(last, dict):
        for k, ln in (("brightness", 1), ("rgb_color", 3), ("rgbw_color", 4),
                      ("rgbww_color", 4)):
            if k in p:
                continue
            v = last.get(k)
            if k == "brightness":
                if isinstance(v, bool):
                    pass
                elif isinstance(v, (int, float)) and 0 <= v <= 255:
                    p[k] = int(v)
            elif (
                isinstance(v, (list, tuple))
                and len(v) == ln
                and all(isinstance(c, (int, float)) and 0 <= c <= 255 for c in v)
            ):
                p[k] = [int(c) for c in v]
    ct = attrs.get("color_temp_kelvin")
    if isinstance(ct, (int, float)) and 1000 <= ct <= 10000:
        p["color_temp_kelvin"] = int(ct)
    fx = attrs.get("effect")
    if isinstance(fx, str) and fx and fx != "None" and len(fx) <= 32:
        p["effect"] = fx
    return p


def climate_ctl(attrs: dict) -> dict | None:
    """Thermostat capability descriptor for config (min/max/step + modes)."""
    modes = attrs.get("hvac_modes")
    modes = [m for m in modes if isinstance(m, str) and m in _KNOWN_HVAC_MODES][:8] \
        if isinstance(modes, (list, tuple)) else []
    presets = attrs.get("preset_modes")
    presets = [str(p)[:32] for p in presets if isinstance(p, str) and p][:10] \
        if isinstance(presets, (list, tuple)) else []

    def _num(key: str) -> float | None:
        v = attrs.get(key)
        return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None

    tmin, tmax, step = _num("min_temp"), _num("max_temp"), _num("target_temp_step")
    if not modes and tmin is None and tmax is None:
        return None
    ctl: dict = {"type": "climate"}
    if tmin is not None and -50 <= tmin <= 100:
        ctl["temp_min"] = tmin
    if tmax is not None and -50 <= tmax <= 100:
        ctl["temp_max"] = tmax
    if (step is not None and 0 < step <= 5
            and "temp_min" in ctl and "temp_max" in ctl and ctl["temp_min"] < ctl["temp_max"]):
        ctl["temp_step"] = step
    if modes:
        ctl["hvac_modes"] = modes
    if presets:
        ctl["preset_modes"] = presets
    return ctl


def climate_state(state: str, attrs: dict) -> tuple[str | None, object, dict]:
    """Thermostat: value is TARGET temp (v), p has current + mode + action.

    `hvac_mode` describes set mode only (e.g. heat); `hvac_action`
    whether heating/cooling runs NOW (heating/cooling/idle) — latter is
    per-cycle live state.
    No target: hvac mode as string. Nothing readable: (None, None, {}).
    """
    def _num(key: str) -> float | None:
        v = attrs.get(key)
        return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None

    p: dict = {}
    cur = _num("current_temperature")
    if cur is not None and -50 <= cur <= 100:
        p["current"] = cur
    if isinstance(state, str) and state and state not in ("unknown", "unavailable"):
        p["hvac_mode"] = state[:32]
    act = attrs.get("hvac_action")
    if isinstance(act, str) and act and act not in ("unknown", "unavailable"):
        p["hvac_action"] = act[:32]
    target = _num("temperature")
    if target is not None and -50 <= target <= 100:
        return "v", target, p
    if "hvac_mode" in p:
        return "vs", p["hvac_mode"], ({k: v for k, v in p.items() if k != "hvac_mode"} or {})
    return None, None, {}


def climate_calls(
    sensor_key: str, state: str, params: dict | None
) -> list[tuple[str, dict]] | None:
    """Thermostat service-call list [(service, extra)] (domain=climate).

    Order: hvac mode → preset → temperature. state=off → turn off.
    toggle → turn on (no meaningful toggle).
    Empty state: thermostat-standalone, pure param command (server sends no
    ON/OFF — thermostat decides on target temp); with no param
    nothing to do → None.
    """
    if sensor_key.split(".")[0] != "climate":
        return None
    if state == "off":
        return [("turn_off", {})]
    if state and state not in ("on", "toggle"):
        return None
    params = params or {}
    calls: list[tuple[str, dict]] = []
    hvac = params.get("hvac_mode")
    if isinstance(hvac, str) and hvac and hvac in _KNOWN_HVAC_MODES:
        calls.append(("set_hvac_mode", {"hvac_mode": hvac}))
    preset = params.get("preset_mode")
    if isinstance(preset, str) and preset and len(preset) <= 32:
        calls.append(("set_preset_mode", {"preset_mode": preset}))
    temp = params.get("temperature")
    if isinstance(temp, bool):
        pass
    elif isinstance(temp, (int, float)):
        calls.append(("set_temperature", {"temperature": float(temp)}))
    if not calls:
        if not state:
            return None  # no state, no params: nothing to do
        calls.append(("turn_on", {}))
    return calls


def semver_gt(a: str, b: str) -> bool:
    def parts(s: str) -> list[int]:
        out = []
        for p in s.strip().split("."):
            try:
                out.append(int(p))
            except ValueError:
                break
        return out or [0]

    pa, pb = parts(a), parts(b)
    for x, y in zip(pa, pb):
        if x != y:
            return x > y
    return len(pa) > len(pb)


def valid_response(data: object) -> bool:
    """Valid report/pull response? Misleading empty ok:true is NOT."""
    return isinstance(data, dict) and data.get("ok") is True and isinstance(data.get("srv_ts"), int)


def clock_drift_srv(srv_ts: int, now: int) -> int:
    return srv_ts - now
