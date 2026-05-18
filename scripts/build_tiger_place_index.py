"""
Build a fast point-in-polygon index of Census TIGER/Line Place boundaries.

Run LOCALLY from a developer machine — output is committed to the repo and
bundled into the Docker image. Includes both incorporated places and Census
Designated Places (CDPs). Used by src/data_ingest/place_lookup.py at runtime
to stamp each building with its real municipal/CDP boundary instead of
nearest-centroid Voronoi.

Source: https://www2.census.gov/geo/tiger/TIGER2024/PLACE/

Usage:
    python scripts/build_tiger_place_index.py --states 22
    python scripts/build_tiger_place_index.py --states 22 48 12 13 22 28 01 \\
        37 45 51 24 34 36 09 25 44 23

State FIPS codes (coastal states the SurgeDPS catalog covers):
    01 AL   09 CT   10 DE   12 FL   13 GA   22 LA   23 ME   24 MD
    25 MA   28 MS   33 NH   34 NJ   36 NY   37 NC   44 RI   45 SC
    48 TX   51 VA
"""

from __future__ import annotations

import argparse
import io
import os
import pickle
import sys
import urllib.request
import zipfile

import shapefile  # pyshp
from shapely.geometry import shape, mapping, Polygon, MultiPolygon

_TIGER_BASE = "https://www2.census.gov/geo/tiger/TIGER2024/PLACE"
_TIGER_URL_TEMPLATE = "{base}/tl_2024_{fips}_place.zip"

# Output location — data/places/places_<state_fips>.pkl. The runtime module
# loads any pickles it finds in this directory.
_BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_OUTPUT_DIR = os.path.join(_BASE_DIR, "data", "places")

# Simplification tolerance — 0.001° ≈ 100 m near 30°N. Plenty of resolution
# for "is this building inside the city limit?" at consumer-map zoom levels,
# while shrinking the pickled output from ~50 MB to ~5 MB per state.
_SIMPLIFY_TOL = 0.001

# FIPS → human-readable name (informational only; the stamp records FIPS
# alongside the place's own geoid for downstream lookups).
_STATE_NAMES = {
    "01": "AL", "09": "CT", "10": "DE", "12": "FL", "13": "GA",
    "22": "LA", "23": "ME", "24": "MD", "25": "MA", "28": "MS",
    "33": "NH", "34": "NJ", "36": "NY", "37": "NC", "44": "RI",
    "45": "SC", "48": "TX", "51": "VA",
}


def fetch_tiger_zip(state_fips: str) -> bytes:
    url = _TIGER_URL_TEMPLATE.format(base=_TIGER_BASE, fips=state_fips)
    print(f"  Downloading {url} ...")
    with urllib.request.urlopen(url, timeout=120) as resp:
        return resp.read()


def parse_shapefile_records(zip_bytes: bytes):
    """Open the shapefile from in-memory ZIP and yield (record_dict, geometry_dict)."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        # pyshp reads .shp + .dbf + .shx by file-like handles
        shp_name = next(n for n in z.namelist() if n.endswith(".shp"))
        dbf_name = next(n for n in z.namelist() if n.endswith(".dbf"))
        shx_name = next(n for n in z.namelist() if n.endswith(".shx"))
        with (
            z.open(shp_name) as shp,
            z.open(dbf_name) as dbf,
            z.open(shx_name) as shx,
        ):
            sf = shapefile.Reader(shp=shp, dbf=dbf, shx=shx)
            fields = [f[0] for f in sf.fields if f[0] != "DeletionFlag"]
            for sr in sf.iterShapeRecords():
                rec = dict(zip(fields, sr.record))
                geom = sr.shape.__geo_interface__
                yield rec, geom


def build_state_index(state_fips: str) -> list[dict]:
    state_abbr = _STATE_NAMES.get(state_fips, "??")
    print(f"\n[{state_fips} {state_abbr}]")
    zip_bytes = fetch_tiger_zip(state_fips)
    print(f"  Got {len(zip_bytes):,} bytes")

    records = []
    bytes_before = 0
    bytes_after = 0
    for rec, geom in parse_shapefile_records(zip_bytes):
        geoid = rec.get("GEOID")
        name = rec.get("NAME")
        classfp = rec.get("CLASSFP", "")   # G6 = incorporated, U1/U2/U6 = CDP
        if not geoid or not name:
            continue
        try:
            poly = shape(geom)
        except Exception as e:
            print(f"  WARN: bad geometry for {name} ({geoid}): {e}")
            continue
        if poly.is_empty:
            continue
        # Simplify — preserve_topology keeps the polygon valid as a city
        # boundary even on tight inlets.
        bytes_before += len(str(mapping(poly)))
        poly_s = poly.simplify(_SIMPLIFY_TOL, preserve_topology=True)
        if poly_s.is_empty:
            poly_s = poly  # fall back if simplification collapsed it
        bytes_after += len(str(mapping(poly_s)))

        records.append({
            "geoid": str(geoid),
            "name": str(name),
            "state": state_abbr,
            "classfp": str(classfp),
            "geometry": mapping(poly_s),
        })

    print(f"  {len(records)} places (simplified {bytes_before/1e6:.1f} → {bytes_after/1e6:.1f} MB)")
    return records


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--states", nargs="+", required=True,
                        help="State FIPS codes to process (e.g. 22 48 12)")
    parser.add_argument("--out", default=_OUTPUT_DIR,
                        help="Output directory (default: data/places)")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    total_places = 0
    for fips in args.states:
        try:
            records = build_state_index(fips)
        except Exception as e:
            print(f"  FAILED: {e}")
            return 1
        out_path = os.path.join(args.out, f"places_{fips}.pkl")
        with open(out_path, "wb") as f:
            pickle.dump({"state_fips": fips,
                         "state_abbr": _STATE_NAMES.get(fips, "??"),
                         "records": records}, f, protocol=4)
        print(f"  Wrote {out_path} ({os.path.getsize(out_path)/1024:.0f} KB, {len(records)} places)")
        total_places += len(records)

    print(f"\nDone. {total_places} places across {len(args.states)} state(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
