"""SzenzorKozpont — async HTTP client (aiohttp).

All calls: HTTPS-only, Bearer API key header, 15 s timeout,
1 retry with backoff, gzip for large bodies. API key never
logged (masked prefix only).
"""
from __future__ import annotations

import asyncio
import gzip
import json
import logging
from typing import Any

import aiohttp

from .const import API_TIMEOUT_S, MAX_CONFIG_BYTES
from .protocol import encode_body, valid_response

_LOGGER = logging.getLogger(__name__)


class ApiError(Exception):
    """Expected error (timeout, HTTP error, bad response)."""


class ApiAuthError(ApiError):
    """401: bad/revoked key — retry pointless."""


class ApiRateLimitError(ApiError):
    """429: with Retry-After seconds."""

    def __init__(self, msg: str, retry_after: int = 60) -> None:
        super().__init__(msg)
        self.retry_after = retry_after


class ApiServerError(ApiError):
    """5xx: server-side error — retry later."""


class ApiClient:
    def __init__(
        self,
        server_url: str,
        api_key: str,
        verify_ssl: bool = True,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        if not server_url.startswith("https://"):
            raise ValueError("csak https:// URL fogadható el")
        self._base = server_url.rstrip("/")
        self._key = api_key
        self._verify = verify_ssl
        self._session = session

    @property
    def masked(self) -> str:
        """Loggable key form: szk_abcd…****."""
        try:
            _, rest = self._key.split("_", 1)
            prefix = rest.split(".", 1)[0][:8]
        except (ValueError, IndexError):
            prefix = "????"
        return f"szk_{prefix}…****"

    def full_url(self, url: str) -> str:
        """Join relative server path (`/ota/...`) to absolute."""
        if url.startswith("https://"):
            return url
        return self._base + "/" + url.lstrip("/")

    def auth_headers(self) -> dict:
        return {"Authorization": f"Bearer {self._key}"}

    async def _post_raw(
        self,
        path: str,
        raw: bytes,
        gzipped: bool,
        session: aiohttp.ClientSession,
    ) -> dict:
        """Raw POST with full error matrix. All calls go through here."""
        headers = {
            "Authorization": f"Bearer {self._key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if gzipped:
            headers["Content-Encoding"] = "gzip"
        last: Exception | None = None
        for attempt in (0, 1):
            try:
                timeout = aiohttp.ClientTimeout(total=API_TIMEOUT_S)
                async with session.post(
                    self._base + path,
                    data=raw,
                    headers=headers,
                    ssl=self._verify,
                    timeout=timeout,
                ) as resp:
                    if resp.status == 429:
                        try:
                            retry = max(1, min(int(resp.headers.get("Retry-After", 60)), 3600))
                        except (TypeError, ValueError):
                            retry = 60
                        kind = "rate limited"
                        try:
                            data = await resp.json(content_type=None)
                            if isinstance(data, dict) and data.get("err") == "quota":
                                kind = "kvóta kimerült (napi pontkeret, UTC éjfélkor nullázódik)"
                        except (json.JSONDecodeError, aiohttp.ContentTypeError,
                                asyncio.TimeoutError, aiohttp.ClientError):
                            pass
                        raise ApiRateLimitError(f"{kind} ({retry} mp)", retry)
                    if resp.status == 401:
                        raise ApiAuthError("hibás API-kulcs (401)")
                    if resp.status == 413:
                        raise ApiError("túl nagy test (413)")
                    if resp.status >= 500:
                        raise ApiServerError(f"szerverhiba HTTP {resp.status}")
                    if resp.status != 200:
                        raise ApiError(f"HTTP {resp.status}")
                    try:
                        data = await resp.json(content_type=None)
                    except (json.JSONDecodeError, aiohttp.ContentTypeError) as err:
                        raise ApiError("érvénytelen JSON-válasz") from err
                    if not isinstance(data, dict):
                        raise ApiError("rossz válasz-alak")
                    return data
            except (ApiAuthError, ApiRateLimitError, ApiServerError):
                raise  # final classification: no retry
            except (asyncio.TimeoutError, aiohttp.ClientError) as err:
                last = err
                _LOGGER.debug("SzenzorKozpont %s hiba (%d. próba): %s", path, attempt + 1, type(err).__name__)
                if attempt == 0:
                    await asyncio.sleep(2)
        raise ApiError(f"elérhetetlen: {last}")

    async def _post(
        self,
        path: str,
        payload: dict,
        session: aiohttp.ClientSession,
        gzip_ok: bool = False,
        gzip_min: int = 16384,
    ) -> dict:
        raw = json.dumps(payload, separators=(",", ":")).encode()
        gzipped = False
        if gzip_ok and len(raw) > gzip_min:
            raw = gzip.compress(raw)
            gzipped = True
        if len(raw) > MAX_CONFIG_BYTES and gzip_ok or len(raw) > 2 * 1024 * 1024:
            raise ApiError("túl nagy test")
        return await self._post_raw(path, raw, gzipped, session)

    async def pull(self, session: aiohttp.ClientSession, cfg_rev: int) -> dict:
        data = await self._post("/api/v1/commands/pull", {"cfg": cfg_rev}, session)
        if not valid_response(data) or not isinstance(data.get("cmds"), list):
            raise ApiError("érvénytelen pull-válasz (auth?)")
        # cfg_rev may lag on unknown keys — but srv_ts required
        data.setdefault("cfg_rev", cfg_rev)
        return data

    async def report(
        self, session: aiohttp.ClientSession, ack: list[int], bt: int, entries: list[dict], bn: str = "",
        same: list[str] | None = None,
    ) -> dict:
        from .protocol import build_report

        tuples = []
        for en in entries:
            for field in ("v", "vs", "vb"):
                if field in en:
                    t = (en["n"], field, en[field])
                    if isinstance(en.get("p"), dict) and en["p"]:
                        t = (en["n"], field, en[field], en["p"])
                    tuples.append(t)
                    break
        body = build_report(tuples, bt)
        if bn:
            body["bn"] = bn
        if same:
            # Unchanged keys: server refreshes last_seen only,
            # no point storage. Always full key name (no bn).
            body["same"] = [k for k in same if isinstance(k, str) and k]
        # Report is the only frequent gzip-worthy body: server
        # accepts Content-Encoding: gzip on all endpoints.
        data = await self._post("/api/v1/report", {"ack": ack, **body}, session,
                                gzip_ok=True, gzip_min=1024)
        if not valid_response(data):
            raise ApiError("érvénytelen report-válasz (auth?)")
        return data

    async def config(self, session: aiohttp.ClientSession, chunks: list[list[dict]]) -> dict:
        """Chunked config upload. Returns last chunk response."""
        last: dict = {}
        total = len(chunks)
        for i, items in enumerate(chunks, start=1):
            payload: dict[str, Any] = {"items": items}
            if total > 1:
                payload["part"] = i
                payload["parts"] = total
            raw, gz = encode_body(payload, MAX_CONFIG_BYTES)
            try:
                last = await self._post_raw("/api/v1/config", raw, gz, session)
            except (asyncio.TimeoutError, aiohttp.ClientError) as err:
                raise ApiError(f"config-hiba: {type(err).__name__}") from err
            if not isinstance(last, dict) or last.get("ok") is not True:
                raise ApiError("érvénytelen config-válasz")
        return last

    async def sysinfo(self, session: aiohttp.ClientSession, info: dict) -> dict:
        return await self._post("/api/v1/sysinfo", info, session)

    async def triggers(self, session: aiohttp.ClientSession, payload: dict) -> dict:
        return await self._post("/api/v1/triggers", payload, session)

    async def ota_check(self, session: aiohttp.ClientSession, client_type: str, fw: str) -> dict:
        return await self._post(
            "/api/v1/ota/check", {"client_type": client_type, "fw_v": fw}, session
        )
