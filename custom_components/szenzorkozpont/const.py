"""SzenzorKozpont — constants and version."""
from __future__ import annotations

DOMAIN = "szenzorkozpont"
VERSION = "1.0.0"

DEFAULT_POLL_DAY_S = 600
DEFAULT_POLL_NIGHT_S = 1200
DEFAULT_DAY_FROM = "06:00"
DEFAULT_DAY_TO = "21:00"

DEFAULT_INCLUDED_DOMAINS = (
    "sensor",
    "binary_sensor",
    "input_number",
    "input_text",
    "input_boolean",
    "switch",
    "light",
    "climate",
)
CONTROL_DOMAINS = ("switch", "light", "input_boolean", "input_text", "input_number", "button", "scene", "climate")

API_TIMEOUT_S = 15
MAX_REPORT_BYTES = 12288
# Full report auto-interval (heartbeat): 2x daily.
FULL_REPORT_EVERY_S = 43200
MAX_CONFIG_ITEMS = 200
MAX_CONFIG_BYTES = 256 * 1024
MAX_ATTRS_PER_SENSOR = 10
MAX_ATTR_BYTES = 100
SKIP_ATTRS = frozenset({"options", "attribution", "icon", "entity_picture"})

KEY_RE = r"[A-Za-z0-9_.\-]{1,50}"
API_KEY_RE = r"^szk_[A-Za-z0-9]{8}\.[A-Za-z0-9\-_]{20,64}$"

BACKEND_SZK = "szenzorkozpont"
BACKEND_THINGSBOARD = "thingsboard"
BACKEND_CHOICES = (BACKEND_SZK, BACKEND_THINGSBOARD)

BACKEND_LABELS = {
    BACKEND_SZK: "SzenzorKozpont (saját felhő)",
    BACKEND_THINGSBOARD: "ThingsBoard",
}

STORE_VERSION = 1
STORE_KEY = "szenzorkozpont_state"
