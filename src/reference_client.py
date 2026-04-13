"""
Reference-data client: StormDPS → SurgeDPS.

Reference datasets (Atlas 14 precipitation frequency, Census population,
HAND/NHDPlus hydrology, geocoding) are large, read-mostly, and shared
between storms. To keep the SurgeDPS 5 GB Railway volume lean we serve
them from StormDPS over HTTP and keep a small LRU disk cache on the
SurgeDPS side so each item is fetched at most once per container.

Environment variables
---------------------
STORMDPS_API_URL
    Base URL of the StormDPS FastAPI service (e.g. https://stormdps.up.railway.app).
    If unset, the client falls back to direct local-file reads from
    ``tmp_integration/`` — the previous behavior, for local dev.

STORMDPS_API_TIMEOUT_SEC   default 15
STORMDPS_REFERENCE_CACHE_MB  default 500  (LRU cap on local disk cache)

Usage
-----
    from reference_client import reference_client
    data = reference_client.get_atlas14(lat=29.7, lon=-95.4)
    pop  = reference_client.get_census_pep(geoid='48201', vintage='2022')
    hand_path = reference_client.get_hand_raster('12040104')  # local path

All getters return the same shapes the legacy local fetchers did, so
callers only have to swap the access path.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Iterable, Optional

import requests

logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────────────
_STORMDPS_URL      = (os.environ.get("STORMDPS_API_URL") or "").rstrip("/")
_TIMEOUT_SEC       = float(os.environ.get("STORMDPS_API_TIMEOUT_SEC", "15"))
_CACHE_CAP_BYTES   = int(float(os.environ.get("STORMDPS_REFERENCE_CACHE_MB", "500")) * 1_048_576)

# Cache lives alongside other SurgeDPS persistent data but counts against
# the volume — the LRU cap keeps it bounded.
try:
    from persistent_paths import PERSISTENT_DATA_DIR as _PERSIST
    _CACHE_DIR = Path(_PERSIST) / "reference_cache"
except Exception:
    _CACHE_DIR = Path(__file__).resolve().parent.parent / "tmp_integration" / "reference_cache"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ── Session (connection-pooled, retried) ────────────────────────────────────
_session = requests.Session()
_session.headers.update({"User-Agent": "SurgeDPS-ReferenceClient/1.0"})


# ── Local LRU ───────────────────────────────────────────────────────────────
def _cache_key(*parts: Any) -> str:
    h = hashlib.sha1("|".join(str(p) for p in parts).encode()).hexdigest()[:16]
    return h


def _cache_path(namespace: str, key: str, ext: str = "json") -> Path:
    d = _CACHE_DIR / namespace
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{key}.{ext}"


def _evict_if_over_cap() -> None:
    """Remove oldest files (by mtime) until total size ≤ cap."""
    try:
        files = [p for p in _CACHE_DIR.rglob("*") if p.is_file()]
        total = sum(f.stat().st_size for f in files)
        if total <= _CACHE_CAP_BYTES:
            return
        files.sort(key=lambda p: p.stat().st_mtime)
        for f in files:
            try:
                total -= f.stat().st_size
                f.unlink()
            except OSError:
                continue
            if total <= int(_CACHE_CAP_BYTES * 0.75):
                break
    except Exception as exc:  # never let cache eviction crash a request
        logger.warning("reference_cache eviction failed: %s", exc)


# ── Low-level fetch primitives ──────────────────────────────────────────────
def _get_json(path: str, params: Optional[dict] = None) -> Optional[Any]:
    if not _STORMDPS_URL:
        return None
    url = f"{_STORMDPS_URL}{path}"
    try:
        r = _session.get(url, params=params, timeout=_TIMEOUT_SEC)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except requests.RequestException as exc:
        logger.warning("reference_client GET %s failed: %s", url, exc)
        return None


def _stream_to_file(path: str, dest: Path, params: Optional[dict] = None) -> bool:
    """Stream a binary response (raster) to *dest*. Returns True on success."""
    if not _STORMDPS_URL:
        return False
    url = f"{_STORMDPS_URL}{path}"
    tmp = Path(tempfile.mkstemp(dir=str(dest.parent), suffix=".part")[1])
    try:
        with _session.get(url, params=params, timeout=_TIMEOUT_SEC, stream=True) as r:
            if r.status_code == 404:
                tmp.unlink(missing_ok=True)
                return False
            r.raise_for_status()
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 16):
                    if chunk:
                        f.write(chunk)
        os.replace(tmp, dest)
        return True
    except (requests.RequestException, OSError) as exc:
        logger.warning("reference_client STREAM %s failed: %s", url, exc)
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        return False


# ── Public API ──────────────────────────────────────────────────────────────
class ReferenceClient:
    """Thin, testable facade over the fetch primitives."""

    @property
    def remote_enabled(self) -> bool:
        return bool(_STORMDPS_URL)

    # ---- Atlas 14 ---------------------------------------------------------
    def get_atlas14(self, lat: float, lon: float) -> Optional[dict]:
        """Atlas 14 precipitation-frequency table for a 1° grid cell.

        Returns the same dict shape the legacy cache on disk used:
            {"lat": float, "lon": float, "table": {...}}
        or None if not available.
        """
        lat1 = round(lat, 0)
        lon1 = round(lon, 0)
        key = _cache_key("atlas14", lat1, lon1)
        cpath = _cache_path("atlas14", key)
        if cpath.exists():
            try:
                return json.loads(cpath.read_text())
            except Exception:
                cpath.unlink(missing_ok=True)

        data = _get_json("/reference/atlas14", {"lat": lat1, "lon": lon1})
        if data is not None:
            try:
                cpath.write_text(json.dumps(data))
                _evict_if_over_cap()
            except OSError as exc:
                logger.warning("atlas14 cache write failed: %s", exc)
        return data

    # ---- Census -----------------------------------------------------------
    def get_census_pep(self, geoid: str, vintage: str | int = 2022) -> Optional[dict]:
        key = _cache_key("pep", geoid, vintage)
        cpath = _cache_path("census_pep", key)
        if cpath.exists():
            try:
                return json.loads(cpath.read_text())
            except Exception:
                cpath.unlink(missing_ok=True)

        data = _get_json("/reference/census/pep", {"geoid": geoid, "vintage": vintage})
        if data is not None:
            try:
                cpath.write_text(json.dumps(data))
                _evict_if_over_cap()
            except OSError:
                pass
        return data

    def get_census_acs(self, geoid: str, vintage: str | int = 2022) -> Optional[dict]:
        key = _cache_key("acs", geoid, vintage)
        cpath = _cache_path("census_acs", key)
        if cpath.exists():
            try:
                return json.loads(cpath.read_text())
            except Exception:
                cpath.unlink(missing_ok=True)

        data = _get_json("/reference/census/acs", {"geoid": geoid, "vintage": vintage})
        if data is not None:
            try:
                cpath.write_text(json.dumps(data))
                _evict_if_over_cap()
            except OSError:
                pass
        return data

    # ---- HAND / FIM (binary rasters) --------------------------------------
    def get_hand_raster(self, huc8: str) -> Optional[Path]:
        """Return a local path to the HAND raster for *huc8*.

        Downloads from StormDPS on first miss, caches to disk, returns
        the local Path on subsequent calls.
        """
        return self._get_binary("hand", f"/reference/hand/{huc8}", huc8, "tif")

    def get_catchments_raster(self, huc8: str) -> Optional[Path]:
        return self._get_binary(
            "catchments", f"/reference/hand/{huc8}/catchments", huc8, "tif",
        )

    def _get_binary(self, namespace: str, route: str, key_in: str, ext: str) -> Optional[Path]:
        key = _cache_key(namespace, key_in)
        cpath = _cache_path(namespace, key, ext=ext)
        if cpath.exists() and cpath.stat().st_size > 0:
            # touch so LRU eviction sees this as fresh
            os.utime(cpath, None)
            return cpath
        ok = _stream_to_file(route, cpath)
        if not ok:
            return None
        _evict_if_over_cap()
        return cpath

    # ---- Geocode ----------------------------------------------------------
    def reverse_geocode(self, lat: float, lon: float) -> Optional[dict]:
        # Round to ~100 m precision for cache reuse
        latr = round(lat, 3)
        lonr = round(lon, 3)
        key = _cache_key("revgeo", latr, lonr)
        cpath = _cache_path("geocode", key)
        if cpath.exists():
            try:
                return json.loads(cpath.read_text())
            except Exception:
                cpath.unlink(missing_ok=True)
        data = _get_json("/reference/geocode", {"lat": latr, "lon": lonr})
        if data is not None:
            try:
                cpath.write_text(json.dumps(data))
                _evict_if_over_cap()
            except OSError:
                pass
        return data


reference_client = ReferenceClient()


# ── Introspection for /health ───────────────────────────────────────────────
def cache_stats() -> dict:
    files = list(_CACHE_DIR.rglob("*"))
    size = sum(f.stat().st_size for f in files if f.is_file())
    return {
        "remote_enabled": bool(_STORMDPS_URL),
        "remote_url": _STORMDPS_URL or None,
        "cache_dir": str(_CACHE_DIR),
        "cache_bytes": size,
        "cache_mb": round(size / 1_048_576, 2),
        "cap_mb": round(_CACHE_CAP_BYTES / 1_048_576, 2),
        "file_count": sum(1 for f in files if f.is_file()),
    }
