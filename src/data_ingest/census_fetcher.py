"""
Census Bureau Population Fetcher

Provides population context for storm-affected areas by:
  1. Converting landfall coordinates → county FIPS via the FCC Census Block API
  2. Querying the Census Population Estimates API for that county

This gives the UI a quick "~X people in [County], [State]" context line
so users understand the scale of exposure.

APIs used:
  - FCC Census Block: https://geo.fcc.gov/api/census/block/find
    (no API key required, returns county FIPS from lat/lon)
  - Census PEP: https://api.census.gov/data/{vintage}/pep/population
    (no API key required for low-volume; returns county population)
"""

from __future__ import annotations

import json
import logging
import os
from typing import Dict, Optional

import requests

logger = logging.getLogger(__name__)

# ── Persistent disk cache (survives restarts on Railway volume) ──────────
_BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
_PERSISTENT_DIR = os.environ.get('PERSISTENT_DATA_DIR', os.path.join(_BASE_DIR, 'tmp_integration'))
_CENSUS_CACHE_DIR = os.path.join(_PERSISTENT_DIR, 'census')
os.makedirs(_CENSUS_CACHE_DIR, exist_ok=True)

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "SurgeDPS/1.0 (surgedps.com)"})

# Most recent vintage year for PEP (Population Estimates Program)
# Census publishes vintage 2023 as of mid-2024; update when newer available
_PEP_VINTAGE = 2023

# State FIPS → abbreviation (US states + territories with hurricane exposure)
_STATE_ABBR: Dict[str, str] = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY", "72": "PR", "78": "VI",
}


def _cache_path(key: str) -> str:
    """Return filesystem path for a cache key."""
    import hashlib
    h = hashlib.md5(key.encode()).hexdigest()
    return os.path.join(_CENSUS_CACHE_DIR, f'{h}.json')


def _read_cache(key: str) -> Optional[dict]:
    cp = _cache_path(key)
    if os.path.exists(cp):
        with open(cp) as f:
            return json.load(f)
    return None


def _write_cache(key: str, data: dict):
    cp = _cache_path(key)
    with open(cp, 'w') as f:
        json.dump(data, f)


def get_county_fips(lat: float, lon: float) -> Optional[Dict]:
    """
    Use the FCC Census Block API to get county info from coordinates.

    Returns:
        {"state_fips": "12", "county_fips": "033", "county_name": "Bay County",
         "state_code": "FL"} or None on failure.
    """
    cache_key = f"fcc:{lat:.4f},{lon:.4f}"
    cached = _read_cache(cache_key)
    if cached:
        return cached

    url = "https://geo.fcc.gov/api/census/block/find"
    try:
        resp = _SESSION.get(url, params={
            "latitude": f"{lat:.5f}",
            "longitude": f"{lon:.5f}",
            "censusYear": "2020",
            "format": "json",
        }, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("[Census] FCC geocode failed: %s", exc)
        return None

    county = data.get("County", {})
    state = data.get("State", {})

    if not county.get("FIPS"):
        logger.info("[Census] No county FIPS returned for (%.4f, %.4f)", lat, lon)
        return None

    # County FIPS is 5 digits: state (2) + county (3)
    full_fips = county["FIPS"]
    state_fips = full_fips[:2]
    county_fips = full_fips[2:]

    result = {
        "state_fips": state_fips,
        "county_fips": county_fips,
        "county_name": county.get("name", ""),
        "state_code": _STATE_ABBR.get(state_fips, state.get("code", "")),
        "state_name": state.get("name", ""),
        "full_fips": full_fips,
    }
    _write_cache(cache_key, result)
    return result


def get_county_population(state_fips: str, county_fips: str) -> Optional[Dict]:
    """
    Query Census Population Estimates for a specific county.

    Returns:
        {"population": 174705, "name": "Bay County, Florida", "vintage": 2023}
        or None on failure.
    """
    cache_key = f"pop:{state_fips}:{county_fips}"
    cached = _read_cache(cache_key)
    if cached:
        return cached

    # Try vintage years from newest to oldest
    for vintage in (_PEP_VINTAGE, _PEP_VINTAGE - 1, _PEP_VINTAGE - 2):
        url = f"https://api.census.gov/data/{vintage}/pep/population"
        try:
            resp = _SESSION.get(url, params={
                "get": f"POP_{vintage},NAME",
                "for": f"county:{county_fips}",
                "in": f"state:{state_fips}",
            }, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                # Response is [[header...], [values...]]
                if len(data) >= 2:
                    headers = data[0]
                    values = data[1]
                    pop_idx = next((i for i, h in enumerate(headers) if h.startswith("POP")), None)
                    name_idx = next((i for i, h in enumerate(headers) if h == "NAME"), None)

                    population = int(values[pop_idx]) if pop_idx is not None else None
                    name = values[name_idx] if name_idx is not None else None

                    if population is not None:
                        result = {
                            "population": population,
                            "name": name,
                            "vintage": vintage,
                        }
                        _write_cache(cache_key, result)
                        return result
        except Exception as exc:
            logger.info("[Census] PEP vintage %d failed: %s", vintage, exc)
            continue

    # Fallback: try ACS 5-year estimates (more reliable, slightly older)
    try:
        url = "https://api.census.gov/data/2022/acs/acs5"
        resp = _SESSION.get(url, params={
            "get": "B01001_001E,NAME",
            "for": f"county:{county_fips}",
            "in": f"state:{state_fips}",
        }, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if len(data) >= 2:
                headers = data[0]
                values = data[1]
                pop_idx = next((i for i, h in enumerate(headers) if h == "B01001_001E"), None)
                name_idx = next((i for i, h in enumerate(headers) if h == "NAME"), None)
                if pop_idx is not None:
                    result = {
                        "population": int(values[pop_idx]),
                        "name": values[name_idx] if name_idx is not None else None,
                        "vintage": 2022,
                        "source": "ACS5",
                    }
                    _write_cache(cache_key, result)
                    return result
    except Exception as exc:
        logger.info("[Census] ACS fallback failed: %s", exc)

    return None


def get_median_home_value(state_fips: str, county_fips: str) -> Optional[Dict]:
    """
    Query Census ACS 5-year estimates for median home value in a county.

    Uses ACS variable B25077_001E (Median Value for Owner-Occupied Housing Units).
    This is the best freely available proxy for per-county replacement costs
    when NSI val_struct is unavailable (OSM/MSFT buildings).

    Returns:
        {"median_home_value": 215000, "name": "Bay County, Florida",
         "vintage": 2022, "cost_per_sqft_est": 135.0}
        or None on failure.
    """
    cache_key = f"acs_home_val:{state_fips}:{county_fips}"
    cached = _read_cache(cache_key)
    if cached:
        return cached

    # Try ACS 5-year vintages from newest to oldest
    for vintage in (2022, 2021, 2020):
        url = f"https://api.census.gov/data/{vintage}/acs/acs5"
        try:
            resp = _SESSION.get(url, params={
                "get": "B25077_001E,B25035_001E,NAME",
                "for": f"county:{county_fips}",
                "in": f"state:{state_fips}",
            }, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if len(data) >= 2:
                    headers = data[0]
                    values = data[1]
                    val_idx = next((i for i, h in enumerate(headers) if h == "B25077_001E"), None)
                    yr_idx = next((i for i, h in enumerate(headers) if h == "B25035_001E"), None)
                    name_idx = next((i for i, h in enumerate(headers) if h == "NAME"), None)

                    if val_idx is not None and values[val_idx] is not None:
                        median_val = int(values[val_idx])
                        if median_val <= 0:
                            continue
                        median_yr = int(values[yr_idx]) if yr_idx is not None and values[yr_idx] else None
                        # Estimate $/sqft assuming median single-family ~1,600 sqft
                        # (national Census median). Rough but better than flat $150.
                        cost_per_sqft = median_val / 1600.0
                        result = {
                            "median_home_value": median_val,
                            "median_year_built": median_yr,
                            "name": values[name_idx] if name_idx is not None else None,
                            "vintage": vintage,
                            "cost_per_sqft_est": round(cost_per_sqft, 1),
                        }
                        _write_cache(cache_key, result)
                        return result
        except Exception as exc:
            logger.info("[Census] ACS home value vintage %d failed: %s", vintage, exc)
            continue

    return None


def get_county_home_value(lat: float, lon: float) -> Optional[Dict]:
    """
    High-level: get median home value context for a coordinate.

    Returns:
        {"county_name": "Bay County", "state_code": "FL",
         "median_home_value": 215000, "cost_per_sqft_est": 135.0}
        or None.
    """
    county = get_county_fips(lat, lon)
    if not county:
        return None

    val = get_median_home_value(county["state_fips"], county["county_fips"])
    if not val:
        return None

    return {
        "county_name": county["county_name"],
        "state_code": county["state_code"],
        "full_fips": county["full_fips"],
        "median_home_value": val["median_home_value"],
        "cost_per_sqft_est": val["cost_per_sqft_est"],
        "vintage": val["vintage"],
    }


def get_population_context(lat: float, lon: float) -> Optional[Dict]:
    """
    High-level function: get population context for a coordinate.

    Returns a dict suitable for embedding in the storm activation response:
    {
        "county_name": "Bay County",
        "state_code": "FL",
        "population": 174705,
        "pop_label": "~175K residents",
        "vintage": 2023
    }
    Or None if unable to determine.
    """
    county = get_county_fips(lat, lon)
    if not county:
        return None

    pop = get_county_population(county["state_fips"], county["county_fips"])
    if not pop:
        return {
            "county_name": county["county_name"],
            "state_code": county["state_code"],
            "population": None,
            "pop_label": None,
            "vintage": None,
        }

    population = pop["population"]

    # Human-readable label
    if population >= 1_000_000:
        label = f"~{population / 1_000_000:.1f}M residents"
    elif population >= 10_000:
        label = f"~{round(population / 1000)}K residents"
    elif population >= 1000:
        label = f"~{population / 1000:.1f}K residents"
    else:
        label = f"~{population:,} residents"

    return {
        "county_name": county["county_name"],
        "state_code": county["state_code"],
        "state_name": county.get("state_name", ""),
        "population": population,
        "pop_label": label,
        "vintage": pop.get("vintage"),
    }
