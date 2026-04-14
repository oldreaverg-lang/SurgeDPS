#!/usr/bin/env python3
"""
Rebuild ui/src/assets/cities-coastal.json from Census sources, including
Census Designated Places (CDPs) at a low population threshold.

Data sources:
  1. TIGER 2023 Gazetteer Places file   — gives GEOID, NAME, lat/lon centroid
  2. Decennial 2020 P1 population API   — gives total population per place
  3. Local counties-coastal.json        — polygons used to filter to coastal
                                          counties via point-in-polygon

Why this exists
---------------
The previous cities-coastal.json was built from an incorporated-places-only
list with a ~2,500 minimum population, which dropped small-but-storm-critical
communities like Mexico Beach FL (pop ~1,072, ground zero for Hurricane
Michael), St. George Island FL, the Outer Banks villages, etc.  This
rebuild pulls in CDPs and drops the floor to MIN_POP (default 100) so those
places get their own bubble and jurisdiction row instead of being lumped
into a generic "Unincorporated" bucket.

Schema (one row per place, same as before):
    { name, state, county_geoid, lat, lon, pop }
"""
from __future__ import annotations
import json
import re
import sys
import urllib.request
import zipfile
from pathlib import Path

import requests
import shapely.geometry as sg
from shapely.strtree import STRtree

REPO = Path(__file__).resolve().parent.parent
COUNTIES_PATH = REPO / "ui/src/assets/counties-coastal.json"
OUT_PATH      = REPO / "ui/src/assets/cities-coastal.json"

GAZ_URL       = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2023_Gazetteer/2023_Gaz_place_national.zip"
GAZ_TMP       = REPO / ".cache/2023_Gaz_place_national.txt"

MIN_POP       = 100     # include any place with >= 100 people
CENSUS_API    = "https://api.census.gov/data/2020/dec/pl"

# Suffix stripping: "Mexico Beach city" → "Mexico Beach", "Hatteras CDP" → "Hatteras"
_SUFFIX_RE = re.compile(
    r"\s+(city|town|village|borough|CDP|municipality|comunidad|zona urbana|"
    r"urban(-type)?|consolidated government|metro government|metropolitan government)$",
    re.IGNORECASE,
)


def clean_name(raw: str) -> str:
    return _SUFFIX_RE.sub("", raw).strip()


def download_gazetteer() -> Path:
    """Fetch and unzip the 2023 Gazetteer Places file (cached)."""
    GAZ_TMP.parent.mkdir(exist_ok=True)
    if GAZ_TMP.exists() and GAZ_TMP.stat().st_size > 1_000_000:
        return GAZ_TMP
    print(f"→ downloading {GAZ_URL}", file=sys.stderr)
    zpath = GAZ_TMP.with_suffix(".zip")
    urllib.request.urlretrieve(GAZ_URL, zpath)
    with zipfile.ZipFile(zpath) as z:
        inner = next(n for n in z.namelist() if n.endswith(".txt"))
        with z.open(inner) as src, open(GAZ_TMP, "wb") as dst:
            dst.write(src.read())
    zpath.unlink()
    return GAZ_TMP


def load_counties():
    """Load coastal county polygons, return list of (geoid, name, state, geom)
    plus a prepared STRtree spatial index."""
    with open(COUNTIES_PATH) as f:
        data = json.load(f)
    feats = data["features"] if isinstance(data, dict) else data
    rows, geoms = [], []
    for ft in feats:
        p = ft["properties"]
        geom = sg.shape(ft["geometry"])
        rows.append({
            "geoid": p["GEOID"],
            "state_fips": p["STATE"],
            "name": p["NAME"],
            "geom": geom,
        })
        geoms.append(geom)
    tree = STRtree(geoms)
    # index_by_id lets us map a tree hit back to its row
    idx_by_id = {id(g): i for i, g in enumerate(geoms)}
    return rows, tree, idx_by_id


def fetch_populations(state_fips_list: list[str]) -> dict[str, int]:
    """Fetch Decennial 2020 P1_001N for every place in each coastal state.
    Returns a dict keyed by 7-digit place GEOID (state+place)."""
    pops: dict[str, int] = {}
    for st in sorted(set(state_fips_list)):
        url = f"{CENSUS_API}?get=NAME,P1_001N&for=place:*&in=state:{st}"
        print(f"→ census pop  state {st}", file=sys.stderr)
        # urllib's default TLS handshake fails intermittently against
        # api.census.gov from some sandboxes; requests handles it cleanly.
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        header, *rows = data
        name_i = header.index("NAME")
        pop_i  = header.index("P1_001N")
        state_i = header.index("state")
        place_i = header.index("place")
        for row in rows:
            geoid = row[state_i].zfill(2) + row[place_i].zfill(5)
            try:
                pops[geoid] = int(row[pop_i])
            except (TypeError, ValueError):
                pass
    return pops


def parse_gazetteer(path: Path, coastal_state_fips: set[str]):
    """Yield (place_geoid, state_usps, state_fips, name, lat, lon) for every
    Place whose 2-letter USPS state matches a coastal state."""
    # Build FIPS → USPS map via the gazetteer USPS column itself.
    usps_to_fips = {
        "AL":"01","AK":"02","AZ":"04","AR":"05","CA":"06","CO":"08","CT":"09",
        "DE":"10","DC":"11","FL":"12","GA":"13","HI":"15","ID":"16","IL":"17",
        "IN":"18","IA":"19","KS":"20","KY":"21","LA":"22","ME":"23","MD":"24",
        "MA":"25","MI":"26","MN":"27","MS":"28","MO":"29","MT":"30","NE":"31",
        "NV":"32","NH":"33","NJ":"34","NM":"35","NY":"36","NC":"37","ND":"38",
        "OH":"39","OK":"40","OR":"41","PA":"42","RI":"44","SC":"45","SD":"46",
        "TN":"47","TX":"48","UT":"49","VT":"50","VA":"51","WA":"53","WV":"54",
        "WI":"55","WY":"56","PR":"72",
    }
    with open(path, encoding="utf-8") as f:
        header = f.readline().rstrip().split("\t")
        iUSPS  = header.index("USPS")
        iGEOID = header.index("GEOID")
        iNAME  = header.index("NAME")
        iLAT   = header.index("INTPTLAT")
        # Header column is "INTPTLONG                ..." (trailing spaces)
        iLON   = next(i for i, h in enumerate(header) if h.strip() == "INTPTLONG")
        for line in f:
            parts = line.rstrip("\n").split("\t")
            usps = parts[iUSPS].strip()
            fips = usps_to_fips.get(usps)
            if not fips or fips not in coastal_state_fips:
                continue
            try:
                lat = float(parts[iLAT].strip())
                lon = float(parts[iLON].strip())
            except ValueError:
                continue
            yield (parts[iGEOID].strip(), usps, fips,
                   clean_name(parts[iNAME].strip()), lat, lon)


def main() -> None:
    counties, tree, idx_by_id = load_counties()
    coastal_fips = {c["state_fips"] for c in counties}

    pops = fetch_populations(sorted(coastal_fips))
    print(f"✓ {len(pops):,} place populations fetched", file=sys.stderr)

    gaz_path = download_gazetteer()

    kept: list[dict] = []
    dropped_no_county = dropped_low_pop = dropped_no_pop = 0
    for geoid, usps, fips, name, lat, lon in parse_gazetteer(gaz_path, coastal_fips):
        pop = pops.get(geoid)
        if pop is None:
            dropped_no_pop += 1
            continue
        if pop < MIN_POP:
            dropped_low_pop += 1
            continue

        pt = sg.Point(lon, lat)
        hit_geoid = None
        # shapely 2 STRtree.query returns a numpy array of ints
        for idx in tree.query(pt):
            row = counties[int(idx)]
            if row["geom"].contains(pt) or row["geom"].touches(pt):
                hit_geoid = row["geoid"]
                break
        if hit_geoid is None:
            dropped_no_county += 1
            continue

        kept.append({
            "name": name,
            "state": usps,
            "county_geoid": hit_geoid,
            "lat": round(lat, 5),
            "lon": round(lon, 5),
            "pop": pop,
        })

    # Largest first for nicer rendering order.
    kept.sort(key=lambda r: (-r["pop"], r["state"], r["name"]))

    print(f"✓ wrote {len(kept):,} places to {OUT_PATH}", file=sys.stderr)
    print(f"   dropped: {dropped_no_county:,} outside coastal counties, "
          f"{dropped_low_pop:,} below MIN_POP={MIN_POP}, "
          f"{dropped_no_pop:,} missing pop", file=sys.stderr)

    with open(OUT_PATH, "w") as f:
        json.dump(kept, f, separators=(",", ":"))


if __name__ == "__main__":
    main()
