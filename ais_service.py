"""
Live AIS vessel feed — AISStream.io WebSocket consumer.

Subscribes to PositionReport + ShipStaticData messages inside bounding boxes
for the Strait of Hormuz and Bab el-Mandeb at server startup, keeps an
in-memory cache of the most recent sighting per MMSI, and exposes that cache
to the FastAPI layer via get_vessels(chokepoint_id).

Runs in a daemon thread with its own asyncio loop so it never blocks the
HTTP server. Reconnects on failure with exponential backoff.

No API key? The service no-ops gracefully — the frontend sees an empty
list and falls back to "AIS feed not connected" (same as before the
integration).
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import threading
import time
from typing import Dict, List, Optional

import config

logger = logging.getLogger(__name__)

# ─── Configuration ──────────────────────────────────────────────────────────

# Bounding boxes are [[lat_top_left, lon_top_left], [lat_bottom_right, lon_bottom_right]]
# per AISStream.io API. Kept tight enough that we only get vessels near the
# chokepoint, wide enough to catch approaches from both sides.
#
#   Hormuz: Persian Gulf entrance + Gulf of Oman approach
#   Bab el-Mandeb: South Red Sea + Gulf of Aden approach
BOUNDING_BOXES = {
    "hormuz": [[27.3, 55.0], [25.5, 57.5]],
    "bab":    [[13.6, 42.5], [11.5, 44.2]],
}

# How long we keep a vessel in the cache after its last message. Chokepoint
# transits are slow (~6 kt for tankers); 30 min is enough for a vessel to
# fully cross the strait.
VESSEL_TTL_SECONDS = 30 * 60

# Cap the number returned by get_vessels — panel can only show ~8 legibly.
MAX_RETURNED = 12

# ─── In-memory state ─────────────────────────────────────────────────────────

# {chokepoint_id: {mmsi: vessel_dict}}
_vessels: Dict[str, Dict[int, dict]] = {cp: {} for cp in BOUNDING_BOXES}
_lock = threading.Lock()

# Most recent connection state — surfaced at /api/vessels/status for the UI so
# the panel can explain silence rather than lying about it.
_status: dict = {
    "connected": False,
    "last_message_ts": None,
    "last_connected_ts": None,  # set once per successful handshake
    "last_error": None,
    "started": False,
    "retry_eta_ts": None,       # when we plan to try reconnecting next
}


# ─── Vessel type classification (AIS ShipType codes) ────────────────────────
# Per ITU-R M.1371. We collapse to labels the UI actually cares about.
def _classify_type(type_code: Optional[int]) -> str:
    if type_code is None:
        return "OTHER"
    t = int(type_code)
    if 80 <= t <= 89:
        return "TANKER"
    if 70 <= t <= 79:
        return "CARGO"
    if 60 <= t <= 69:
        return "PASSENGER"
    if 30 <= t <= 39:
        return "FISHING"
    if 40 <= t <= 49:
        return "HSC"        # high-speed craft
    if 50 <= t <= 59:
        return "SPECIAL"    # law-enforcement, pilot, tug, etc.
    if t == 0:
        return "OTHER"
    return "OTHER"


def _in_box(lat: float, lon: float, box) -> bool:
    (lat_n, lon_w), (lat_s, lon_e) = box
    return lat_s <= lat <= lat_n and lon_w <= lon <= lon_e


# ─── WebSocket consumer ──────────────────────────────────────────────────────

async def _handle_message(raw: str) -> None:
    try:
        data = json.loads(raw)
    except Exception:
        return

    msg_type = data.get("MessageType")
    meta = data.get("MetaData") or {}
    mmsi = meta.get("MMSI")
    if not mmsi:
        return
    lat = meta.get("latitude")
    lon = meta.get("longitude")
    name = (meta.get("ShipName") or "").strip()

    now = time.time()
    _status["last_message_ts"] = now

    if msg_type == "PositionReport" and lat is not None and lon is not None:
        pos = (data.get("Message") or {}).get("PositionReport") or {}
        sog = pos.get("Sog")  # knots
        cog = pos.get("Cog")  # degrees
        nav = pos.get("NavigationalStatus")

        for cp, box in BOUNDING_BOXES.items():
            if not _in_box(lat, lon, box):
                # If vessel was here and left, let it age out via TTL
                continue
            with _lock:
                cur = _vessels[cp].get(mmsi, {})
                cur.update({
                    "mmsi": mmsi,
                    "lat": round(float(lat), 4),
                    "lon": round(float(lon), 4),
                    "speed": round(float(sog), 1) if sog is not None else None,
                    "course": round(float(cog), 0) if cog is not None else None,
                    "nav": nav,
                    "name": cur.get("name") or name or f"MMSI {mmsi}",
                    "type": cur.get("type", "OTHER"),
                    "last_seen": now,
                })
                _vessels[cp][mmsi] = cur

    elif msg_type == "ShipStaticData":
        static = (data.get("Message") or {}).get("ShipStaticData") or {}
        type_code = static.get("Type")
        vt = _classify_type(type_code)
        static_name = (static.get("Name") or "").strip() or name
        with _lock:
            for cp in _vessels:
                if mmsi in _vessels[cp]:
                    _vessels[cp][mmsi]["type"] = vt
                    if static_name:
                        _vessels[cp][mmsi]["name"] = static_name


async def _consumer_loop() -> None:
    # Imported lazily so the module doesn't hard-fail if websockets is missing
    # (e.g., during static analysis of the repo before `pip install` runs).
    try:
        import websockets
    except ImportError:
        logger.warning("[ais] websockets package not installed — skipping AIS feed")
        _status["last_error"] = "websockets package not installed"
        return

    if not config.AISSTREAM_API_KEY:
        logger.warning("[ais] AISSTREAM_API_KEY not set — skipping AIS feed")
        _status["last_error"] = "AISSTREAM_API_KEY not configured"
        return

    url = "wss://stream.aisstream.io/v0/stream"
    sub = {
        "APIKey": config.AISSTREAM_API_KEY,
        "BoundingBoxes": list(BOUNDING_BOXES.values()),
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }

    # AISStream.io free tier enforces "one active connection per API key". On
    # Render free-tier deploys a previous worker's WebSocket can linger for
    # ~30s before their side drops it, so our new worker gets HTTP 429 on
    # handshake. Sleep briefly before the very first connect to let that
    # drain — cheap if unnecessary, essential if it's the actual cause.
    await asyncio.sleep(8)

    # Backoff strategy (tuned after seeing the feed stuck in 30-min retries
    # on Render):
    #   - Normal disconnect       → 30s (+jitter), double up to 5 min
    #   - HTTP 429 (rate limit)   → 60s floor, 5 min cap
    #
    # The old 5-min floor + 30-min cap assumed the zombie connection would
    # linger for minutes. In practice AISStream drops it in ~30-60s. Waiting
    # 30 min between attempts meant the feed could stay dark for an entire
    # demo even after the zombie cleared. 60s floor gives the provider time
    # to clean up without punishing us for their latency; 5-min cap means
    # steady-state retries are frequent enough that a user actually sees
    # recovery happen in real time.
    #
    # Reset backoff only after we receive at least one message — a 429 can
    # happen after a successful TCP connect if free-tier quota triggers on
    # subscription rather than handshake.
    backoff = 30
    while True:
        got_message = False
        try:
            async with websockets.connect(url, ping_interval=30, ping_timeout=20) as ws:
                await ws.send(json.dumps(sub))
                _status["connected"] = True
                _status["last_error"] = None
                _status["last_connected_ts"] = time.time()
                _status["retry_eta_ts"] = None
                logger.info("[ais] Connected, subscribed to Hormuz + Bab boxes")
                async for raw in ws:
                    if not got_message:
                        got_message = True
                        backoff = 30  # confirmed healthy — reset only now
                    await _handle_message(raw)
        except Exception as e:
            _status["connected"] = False
            _status["last_error"] = f"{type(e).__name__}: {e}"
            # Dump everything the websockets library can tell us about this
            # failure — response status, headers, body — so we can distinguish
            # API-key invalidity, quota exhaustion, malformed subscription,
            # and zombie-connection contention. websockets 13.x raises
            # InvalidStatus(response=Response(status_code, headers, body))
            # so we have to dig INTO .response, not just str() it.
            response_retry_after = None
            response_body_text = None
            response_status_code = None
            resp = getattr(e, "response", None)
            if resp is not None:
                try:
                    response_status_code = getattr(resp, "status_code", None)
                    headers = getattr(resp, "headers", None)
                    body = getattr(resp, "body", None)
                    if response_status_code is not None:
                        logger.warning(f"[ais] handshake response: HTTP {response_status_code}")
                    if headers is not None:
                        # Headers can be a Headers/dict/list — normalize to dict-of-str.
                        try:
                            hdr_items = headers.raw_items() if hasattr(headers, "raw_items") else (
                                headers.items() if hasattr(headers, "items") else list(headers)
                            )
                            hdr_dict = {str(k).lower(): str(v) for (k, v) in hdr_items}
                        except Exception:
                            hdr_dict = {"_raw": str(headers)[:400]}
                        logger.warning(f"[ais] handshake headers: {json.dumps(hdr_dict)[:600]}")
                        response_retry_after = hdr_dict.get("retry-after")
                    if body is not None:
                        try:
                            response_body_text = body.decode("utf-8", errors="replace") if isinstance(body, (bytes, bytearray)) else str(body)
                        except Exception:
                            response_body_text = repr(body)
                        if response_body_text:
                            logger.warning(f"[ais] handshake body: {response_body_text[:600]}")
                except Exception as diag_err:
                    logger.warning(f"[ais] could not introspect exception.response: {diag_err!r}")
            err_str = str(e).lower()
            # Parenthesize the `rate ... limit` branch so it doesn't bind as
            # `(... or rate) and limit` — Python's `and` has higher precedence
            # than `or`, but readability matters more than correctness-by-luck.
            is_rate_limited = (
                "429" in err_str
                or "too many" in err_str
                or ("rate" in err_str and "limit" in err_str)
            )
            if is_rate_limited:
                # If AISStream sends a Retry-After header, honor it (their
                # number is authoritative; ours is a guess). Otherwise use
                # exponential backoff with our floor/cap.
                hint = None
                if response_retry_after:
                    try:
                        hint = float(response_retry_after)
                    except ValueError:
                        hint = None
                if hint is not None:
                    wait = max(60, min(hint, 600))  # clamp to [60s, 10min]
                    backoff = min(wait * 2, 300)
                    logger.warning(
                        f"[ais] HTTP 429 rate-limited; provider Retry-After={hint}s, "
                        f"sleeping {wait}s. Likely cause: another connection holds this "
                        f"key's slot (zombie process / second consumer / recent redeploy)."
                    )
                else:
                    wait = max(backoff, 60)          # floor at 60s (zombie lifetime)
                    wait = min(wait, 300)            # cap at 5 min
                    backoff = min(wait * 2, 300)     # next attempt, still capped
                    logger.warning(
                        f"[ais] HTTP 429 rate-limited; retry in {wait}s. "
                        f"Likely cause: another connection holds this key's slot "
                        f"(zombie process / second consumer / recent redeploy). "
                        f"If this persists past 10 min of no deploys, rotate the API key."
                    )
            else:
                wait = backoff
                logger.warning(f"[ais] connection dropped ({e!r}); retry in {wait}s")
                backoff = min(backoff * 2, 300)  # cap at 5 min for normal errors
            # Jitter: ±25% so multiple Render workers (if any) don't resync
            jitter = 1.0 + random.uniform(-0.25, 0.25)
            actual_wait = wait * jitter
            _status["retry_eta_ts"] = time.time() + actual_wait
            await asyncio.sleep(actual_wait)


def _prune_stale() -> None:
    cutoff = time.time() - VESSEL_TTL_SECONDS
    with _lock:
        for cp in _vessels:
            stale = [m for m, v in _vessels[cp].items() if (v.get("last_seen") or 0) < cutoff]
            for m in stale:
                _vessels[cp].pop(m, None)


# ─── Public API ──────────────────────────────────────────────────────────────

def get_vessels(chokepoint_id: str) -> List[dict]:
    """Return the current vessel list for a chokepoint, sorted by speed desc.

    Real live AIS data only. If the WebSocket feed has produced no messages
    yet (rate-limited, reconnecting, or not configured), we return an empty
    list — the UI surfaces the feed state via /api/vessels-status and explains
    the silence honestly rather than synthesizing illustrative traffic.
    """
    if chokepoint_id not in _vessels:
        return []
    _prune_stale()
    with _lock:
        vessels = [dict(v) for v in _vessels[chokepoint_id].values()]
    # Prefer vessels with real names + known type, then sort by speed
    vessels.sort(key=lambda v: (
        not (v.get("name") and not v["name"].startswith("MMSI")),
        not (v.get("type") and v["type"] != "OTHER"),
        -(v.get("speed") or 0),
    ))
    return vessels[:MAX_RETURNED]


def get_status() -> dict:
    """Surface connection + message health for the UI."""
    _prune_stale()
    with _lock:
        counts = {cp: len(v) for cp, v in _vessels.items()}
    return {
        **_status,
        "counts": counts,
        "configured": bool(config.AISSTREAM_API_KEY),
    }


def start() -> None:
    """Start the WebSocket consumer in a daemon thread. Safe to call twice."""
    if _status["started"]:
        return
    _status["started"] = True

    def _run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(_consumer_loop())
        except Exception as e:
            logger.error(f"[ais] consumer crashed: {e!r}")
        finally:
            loop.close()

    t = threading.Thread(target=_run, name="ais-consumer", daemon=True)
    t.start()
    logger.info("[ais] consumer thread started")
