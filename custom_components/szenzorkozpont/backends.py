"""SzenzorKozpont — foreign backends for one-way telemetry mirroring.

Send ONLY (pull/commands/config/sysinfo/quota/OTA belong to SZKP
native protocol only). Rest of cycle (HA scan, key building,
filtering, scheduling, trigger-immediate, offline buffer, retry/backoff)
is backend-independent and reused unchanged.

Supported targets (client setting: `backend`):
  - "thingsboard": POST {base}/api/v1/{token}/telemetry
      + optional POST .../attributes (for `p` params).
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from .api import ApiAuthError, ApiError, ApiRateLimitError, ApiServerError
from .const import API_TIMEOUT_S

_LOGGER = logging.getLogger(__name__)

BACKEND_SZK = "szenzorkozpont"
BACKEND_THINGSBOARD = "thingsboard"
BACKENDS = (BACKEND_SZK, BACKEND_THINGSBOARD)


def flatten_params(sensor_key: str, params: dict | None) -> dict[str, tuple[str, Any]]:
    """Flatten `p` param object: {sensor__param: (kind, value)}.

    kind: "v" (number) | "vs" (string) | "vb" (bool). Number lists
    go as comma-joined string (e.g. rgbw "255,0,0,0" —
    matches legacy text form of control commands); dicts and
    other types skipped.
    """
    out: dict[str, tuple[str, Any]] = {}
    if not isinstance(params, dict):
        return out
    for name, val in params.items():
        if not isinstance(name, str) or not name:
            continue
        key = f"{sensor_key}__{name}"
        if isinstance(val, bool):
            out[key] = ("vb", val)
        elif isinstance(val, (int, float)):
            out[key] = ("v", val)
        elif isinstance(val, str):
            out[key] = ("vs", val)
        elif isinstance(val, (list, tuple)) and val and all(
            isinstance(c, (int, float)) and not isinstance(c, bool) for c in val
        ):
            out[key] = ("vs", ",".join(str(c) for c in val))
    return out


def tb_telemetry(entries: list[dict], bt: int) -> list[dict]:
    """ThingsBoard timestamped array: [{"ts": ms, "values": {...}}].

    One report = one sample moment, so single item suffices
    (no mixed timestamps in client cycle). None values skipped
    (TB answers those with 500); empty values -> [] returned, and
    caller skips POST then (no empty report sent).
    """
    values = _tb_values(entries)
    if not values:
        return []
    return [{"ts": int(bt * 1000), "values": values}]


def _tb_values(entries: list[dict]) -> dict:
    """Flat {key: value} dict from entries (no None)."""
    values: dict[str, Any] = {}
    for en in entries:
        if not isinstance(en, dict):
            continue
        n = en.get("n")
        if not isinstance(n, str) or not n:
            continue
        for field in ("v", "vs", "vb"):
            if field in en and en[field] is not None:
                values[n] = en[field]
                break
    return values


# Max keys per single POST: demo.thingsboard.io drops 500 empty
# above 20 keys (measured: 20 OK, 22+ 500 — size-independent).
# Chunking is harmless on own TB servers too (few reqs/cycle).
TB_MAX_KEYS_PER_POST = 15

# Gap between POSTs within one send_telemetry call (s).
# demo.thingsboard.io drops ms bursts (4-5 reqs on
# keep-alive, back-to-back) with empty 500, while the
# same reqs spaced out all return 200 — measured.
TB_POST_GAP_S = 1.5


def _chunks(d: dict, n: int) -> list[dict]:
    items = list(d.items())
    return [dict(items[i:i + n]) for i in range(0, len(items), n)]


def tb_attributes(entries: list[dict]) -> dict | None:
    """`p` params as ThingsBoard client attributes (flat object).

    None if no entry has params (then no POST).
    """
    attrs: dict[str, Any] = {}
    for en in entries:
        if not isinstance(en, dict):
            continue
        n = en.get("n")
        p = en.get("p")
        if not isinstance(n, str) or not n:
            continue
        if not isinstance(p, dict) or not p:
            continue
        for key, (_, sval) in flatten_params(n, p).items():
            attrs[key] = sval
    return attrs or None


async def _http_post(
    session: Any,
    url: str,
    headers: dict,
    raw: bytes,
    verify_ssl: bool,
    retry_after_cap: int = 3600,
) -> None:
    """Minimal POST for foreign backends (timeout + 1 retry + error matrix).

    Errors use api.py exception types, so coordinator
    existing branches (auth/backoff) work unchanged.
    """
    import aiohttp

    last: Exception | None = None
    for attempt in (0, 1):
        try:
            timeout = aiohttp.ClientTimeout(total=API_TIMEOUT_S)
            async with session.post(
                url, data=raw, headers=headers,
                ssl=verify_ssl, timeout=timeout,
            ) as resp:
                snippet = ""
                if resp.status < 200 or resp.status >= 300:
                    snippet = await _error_snippet(resp)
                    if resp.status == 429 or (
                        resp.status >= 500 and _is_rate_text(snippet)
                    ):
                        # 429, or rate-limit text in 5xx body (TB
                        # demo server signals quota overrun this way) —
                        # both get proper wait, no spinning.
                        try:
                            retry = max(1, min(int(resp.headers.get("Retry-After", 60)),
                                               retry_after_cap))
                        except (TypeError, ValueError):
                            retry = 60
                        raise ApiRateLimitError(
                            f"rate limited ({retry} mp)"
                            + (f": {snippet}" if snippet else ""), retry)
                if resp.status == 429:
                    raise ApiRateLimitError("rate limited (60 mp)", 60)
                if resp.status == 401:
                    raise ApiAuthError("hibás token/kulcs (401)")
                if resp.status == 413:
                    raise ApiError("túl nagy test (413)")
                if resp.status >= 500:
                    raise ApiServerError(
                        f"szerverhiba HTTP {resp.status}"
                        + (f": {snippet}" if snippet else ""))
                if resp.status < 200 or resp.status >= 300:
                    raise ApiError(f"HTTP {resp.status}"
                                   + (f": {snippet}" if snippet else ""))
                return
        except (ApiAuthError, ApiRateLimitError, ApiServerError):
            raise  # final classification: no retry
        except (asyncio.TimeoutError, aiohttp.ClientError) as err:
            last = err
            _LOGGER.debug("SzenzorKozpont idegen backend hiba (%d. próba): %s",
                          attempt + 1, type(err).__name__)
            if attempt == 0:
                await asyncio.sleep(2)
    raise ApiError(f"elérhetetlen: {last}")


async def _error_snippet(resp: Any, limit: int = 200) -> str:
    """Error body truncated for log (e.g. TB rate-limit text).

    Capped read: large body safe.
    """
    try:
        raw = await resp.content.read(512)
    except Exception:
        return ""
    try:
        text = raw.decode("utf-8", "replace").strip()
    except Exception:
        return ""
    if len(text) > limit:
        text = text[:limit] + "…"
    return " ".join(text.split())


_RATE_WORDS = ("rate limit", "ratelimit", "too many", "throttl")


def _is_rate_text(text: str) -> bool:
    """Rate-limit-like response text? (TB demo: comes in 5xx body.)"""
    low = (text or "").lower()
    return any(w in low for w in _RATE_WORDS)


class TelemetryBackend:
    """Common base: one cycle = one telemetry send, no ack/commands."""

    # Save location for failed sends (debugging). Set by factory to
    # HA config dir; None means debug log only. Token/header
    # NEVER saved (URL masked, body token-free).
    debug_path: str | None = None

    async def send_telemetry(self, entries: list[dict], bt: int, *,
                             session: Any) -> None:
        raise NotImplementedError

    @property
    def masked(self) -> str:
        return "idegen backend"

    def _record_failure(self, url_masked: str, raw: bytes, err: str) -> None:
        """Save failed POST: URL (masked) + body + error message."""
        try:
            body_txt = raw.decode("utf-8", "replace")
        except Exception:
            body_txt = "<nem dekódolható>"
        _LOGGER.debug("SzenzorKozpont sikertelen POST %s test=%d body=%s",
                      url_masked, len(raw), body_txt)
        if not self.debug_path:
            return
        try:
            import time as _t

            payload = {"ts": int(_t.time()), "url": url_masked,
                       "error": err, "body": body_txt}
            with open(self.debug_path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=1)
            _LOGGER.warning("SzenzorKozpont sikertelen küldés mentve: %s",
                            self.debug_path)
        except Exception as exc:
            _LOGGER.debug("SzenzorKozpont debug-mentés hiba: %s",
                          type(exc).__name__)


def tb_base(url: str) -> str:
    """TB base-URL normalize: host only (scheme + host + port) kept.

    TB docs show full path (.../api/v1/TOKEN/telemetry),
    so if user pastes /api/v1 too, strip it —
    client appends /api/v1/{token}/... itself.
    """
    base = (url or "").strip().rstrip("/")
    if base.lower().endswith("/api/v1"):
        base = base[: -len("/api/v1")].rstrip("/")
    return base


class ThingsboardBackend(TelemetryBackend):
    """ThingsBoard device HTTP API (telemetry + attributes)."""

    def __init__(self, base_url: str, token: str, verify_ssl: bool = True,
                 chunking: bool = True, post_gap: float = TB_POST_GAP_S) -> None:
        self._base = tb_base(base_url)
        self._token = token
        self._verify = verify_ssl
        # Chunking can be disabled (options: tb_chunking) — on own
        # server with no key limit extra POSTs are wasteful.
        self._chunking = chunking
        self._gap = post_gap

    @property
    def masked(self) -> str:
        tok = self._token or ""
        return f"tb_{tok[:4]}…****" if len(tok) >= 4 else "tb_????…****"

    def _masked_url(self, url: str) -> str:
        tok = self._token or ""
        if not tok:
            return url
        return url.replace(tok, tok[:4] + "…****")

    async def _post(self, session: Any, url: str, raw: bytes) -> None:
        """One POST + save on failure (masked URL)."""
        try:
            await _http_post(session, url, {"Content-Type": "application/json"},
                             raw, self._verify)
        except ApiError as err:
            self._record_failure(self._masked_url(url), raw, str(err))
            raise

    async def send_telemetry(self, entries: list[dict], bt: int, *,
                             session: Any) -> None:
        values = _tb_values(entries)
        if self._chunking:
            tel_parts = _chunks(values, TB_MAX_KEYS_PER_POST)
        else:
            tel_parts = [values] if values else []
        jobs: list[tuple[str, bytes]] = []
        for part in tel_parts:
            raw = json.dumps([{"ts": int(bt * 1000), "values": part}],
                             separators=(",", ":")).encode()
            jobs.append(
                (f"{self._base}/api/v1/{self._token}/telemetry", raw))
        attrs = tb_attributes(entries)
        if attrs is not None:
            if self._chunking:
                att_parts = _chunks(attrs, TB_MAX_KEYS_PER_POST)
            else:
                att_parts = [attrs]
            for part in att_parts:
                araw = json.dumps(part, separators=(",", ":")).encode()
                jobs.append(
                    (f"{self._base}/api/v1/{self._token}/attributes", araw))
        first = True
        # Switch (tb_chunking) controls chunking AND inter-chunk
        # wait together: off means one batch, no gap
        # (own server, no limits).
        pace = self._gap if self._chunking else 0.0
        for url, raw in jobs:
            if not first and pace > 0:
                await asyncio.sleep(pace)
            first = False
            await self._post(session, url, raw)



