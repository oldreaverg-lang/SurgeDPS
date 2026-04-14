"""
Time-to-access reachability model (E6).

Pulls arterial / collector / highway OSM roads in the storm bounding
box via the Overpass API, samples the current compound-flood mosaic
along each segment, and runs a Dijkstra search from the nearest road
node to a staging origin (the storm's landfall point) to every hotspot
centroid.

The cost function treats inundated segments as impassable above a
configurable depth threshold and penalizes shallow flooding as slow
(assessment vehicles, not amphibious). The resulting "hours until
accessible" is derived from the storm's forward-speed clock plus the
projected drawdown of the deepest inundation on the shortest path.

Design goals:
- Pure-function module: no process state. Called once per /api/time_to_access
  request. Results are cached by the API layer.
- Degrade gracefully: if Overpass is unreachable, or the compound
  mosaic doesn't exist yet, return None and let the caller fall back
  to the heuristic.
- Cap work aggressively: 1° bbox max, residential/service roads
  filtered out, and the graph stays in-memory (no PostGIS).

Typical wall-clock cost: ~5s for a first call over a medium storm
bbox; Overpass responses are cached to CACHE_DIR/<storm>/osm_roads.json.
"""

from __future__ import annotations

import json
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Iterable

import requests

try:  # Optional deps — caller handles ImportError
    import networkx as nx
    import rasterio
    from rasterio.sample import sample_gen
except Exception:  # pragma: no cover
    nx = None  # type: ignore
    rasterio = None  # type: ignore
    sample_gen = None  # type: ignore


# Arterial / collector / highway classes. Residential and service are
# excluded — an assessment team doesn't care if a private driveway is
# passable. "primary" and "secondary" roads are what actually matter
# for getting trucks to a neighborhood.
_OSM_HIGHWAY_KEEP = (
    "motorway", "trunk", "primary", "secondary",
    "motorway_link", "trunk_link", "primary_link", "secondary_link",
    "tertiary", "tertiary_link",
)

_OVERPASS_URL = "https://overpass-api.de/api/interpreter"
# Two-hour cache on OSM responses. Roads change slowly; during an
# active storm we don't need fresh extracts.
_OSM_CACHE_SECONDS = 7200

# Depth cutoffs. Above _IMPASSABLE_FT we treat the segment as severed;
# between _SLOW_FT and _IMPASSABLE_FT we apply a slowdown multiplier.
_IMPASSABLE_FT = 2.0
_SLOW_FT = 0.5
_SLOW_MULT = 4.0

# Average ground speeds (mph) per OSM highway class, used to convert
# segment length → travel time. These are intentionally conservative
# for post-storm conditions — the real limiter is depth, not speed
# limits.
_SPEED_MPH = {
    "motorway": 55, "motorway_link": 30,
    "trunk": 50, "trunk_link": 30,
    "primary": 40, "primary_link": 25,
    "secondary": 35, "secondary_link": 25,
    "tertiary": 30, "tertiary_link": 20,
}

# Minutes of overhead per hotspot (dismount, assess access, turn
# around) baked into every non-null ETA.
_ACCESS_OVERHEAD_HR = 0.75


@dataclass
class RoadsBundle:
    """In-memory graph + node coordinates for one storm bbox."""
    graph: Any  # nx.MultiDiGraph
    nodes: dict[int, tuple[float, float]]  # node_id → (lon, lat)


def _osm_cache_path(cache_dir: str) -> str:
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, "osm_roads.json")


def _overpass_query(bbox: tuple[float, float, float, float]) -> str:
    """Build an Overpass QL query for the highway classes we want."""
    south, west, north, east = bbox[1], bbox[0], bbox[3], bbox[2]
    classes = "|".join(_OSM_HIGHWAY_KEEP)
    return (
        f"[out:json][timeout:60];"
        f"(way[\"highway\"~\"^({classes})$\"]"
        f"({south},{west},{north},{east}););"
        f"out geom;"
    )


def fetch_osm_roads(
    bbox: tuple[float, float, float, float],
    cache_dir: str,
    overpass_url: str = _OVERPASS_URL,
) -> list[dict] | None:
    """
    Fetch arterial/highway OSM ways for the bbox (lon_min, lat_min,
    lon_max, lat_max). Returns a list of ways with `geometry` and `tags`
    as returned by Overpass. Cached for _OSM_CACHE_SECONDS seconds.

    Returns None if the fetch fails and no cache is available.
    """
    cache_file = _osm_cache_path(cache_dir)
    if os.path.exists(cache_file):
        try:
            age = time.time() - os.path.getmtime(cache_file)
            if age < _OSM_CACHE_SECONDS:
                with open(cache_file) as f:
                    cached = json.load(f)
                if cached.get("bbox") == list(bbox):
                    return cached.get("elements") or []
        except Exception:
            pass

    try:
        resp = requests.post(
            overpass_url,
            data={"data": _overpass_query(bbox)},
            timeout=90,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        # Fall back to stale cache rather than failing outright.
        if os.path.exists(cache_file):
            try:
                with open(cache_file) as f:
                    cached = json.load(f)
                return cached.get("elements") or []
            except Exception:
                return None
        return None

    elements = payload.get("elements") or []
    try:
        with open(cache_file, "w") as f:
            json.dump({"bbox": list(bbox), "elements": elements}, f)
    except Exception:
        pass
    return elements


def _haversine_miles(a: tuple[float, float], b: tuple[float, float]) -> float:
    """Great-circle distance in miles between two (lon, lat) points."""
    lon1, lat1 = a
    lon2, lat2 = b
    R = 3958.8  # Earth radius, miles
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    sa = (math.sin(dlat / 2) ** 2
          + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
          * math.sin(dlon / 2) ** 2)
    return 2 * R * math.asin(min(1.0, math.sqrt(sa)))


def _segment_depths_ft(
    pts: Iterable[tuple[float, float]],
    compound_tif_path: str | None,
) -> list[float]:
    """Sample the compound-depth raster (ft) at each (lon, lat) point."""
    if not compound_tif_path or rasterio is None or sample_gen is None:
        return []
    try:
        with rasterio.open(compound_tif_path) as src:
            band_units = 1
            # compound.tif from process_cell is in feet already; double
            # check via tags if the writer starts emitting metres.
            try:
                tags = src.tags(1)
                if (tags.get("units") or "").lower() in ("m", "meter", "metre"):
                    band_units = 3.28084
            except Exception:
                pass
            out: list[float] = []
            for val in sample_gen(src, list(pts), indexes=[1]):
                v = val[0] if val else 0.0
                try:
                    v = float(v)
                except Exception:
                    v = 0.0
                if math.isnan(v):
                    v = 0.0
                out.append(max(0.0, v * band_units))
            return out
    except Exception:
        return []


def build_graph(
    ways: list[dict],
    compound_tif_path: str | None,
) -> RoadsBundle | None:
    """
    Build a directed graph from Overpass ways, weighted by estimated
    travel time in hours with inundation penalties applied.

    Nodes are OSM node ids; edges have (weight_hr, length_mi,
    max_depth_ft, severed).
    """
    if nx is None:
        return None

    G = nx.MultiDiGraph()
    node_coords: dict[int, tuple[float, float]] = {}

    for w in ways:
        geom = w.get("geometry") or []
        nds = w.get("nodes") or []
        tags = w.get("tags") or {}
        hw = tags.get("highway") or "tertiary"
        speed = _SPEED_MPH.get(hw, 25)
        oneway = tags.get("oneway") in ("yes", "true", "1")

        # Remap geometry to ids; Overpass gives us parallel nodes+geometry
        # arrays when we use `out geom;` with `way`.
        if len(geom) < 2:
            continue

        for nid, pt in zip(nds, geom):
            node_coords.setdefault(nid, (pt["lon"], pt["lat"]))

        # Sample depths along the whole way in one raster call for
        # efficiency, then slice by segment.
        sample_pts = [(g["lon"], g["lat"]) for g in geom]
        depths = _segment_depths_ft(sample_pts, compound_tif_path)

        for i in range(len(geom) - 1):
            a_id = nds[i]
            b_id = nds[i + 1]
            a = (geom[i]["lon"], geom[i]["lat"])
            b = (geom[i + 1]["lon"], geom[i + 1]["lat"])
            miles = _haversine_miles(a, b)
            if miles <= 0:
                continue

            # Max depth bounding this segment (take the deeper of
            # the two endpoint samples as a proxy for the segment).
            if depths:
                d = max(depths[i], depths[i + 1])
            else:
                d = 0.0

            # Base travel time at posted speed.
            hours = miles / max(speed, 5)
            severed = d >= _IMPASSABLE_FT
            if severed:
                # Represent severed links as very high but finite cost
                # so Dijkstra still returns the best (worst) path if
                # nothing else exists.
                hours = hours * 1000.0
            elif d >= _SLOW_FT:
                hours *= _SLOW_MULT

            attrs = dict(weight=hours, miles=miles, depth_ft=d, severed=severed)
            G.add_edge(a_id, b_id, **attrs)
            if not oneway:
                G.add_edge(b_id, a_id, **attrs)

    if not node_coords:
        return None
    return RoadsBundle(graph=G, nodes=node_coords)


def _nearest_node(
    bundle: RoadsBundle, target: tuple[float, float], max_deg: float = 0.1
) -> int | None:
    """Find the OSM node closest to (lon, lat). O(N); N is small enough."""
    tlon, tlat = target
    best = None
    best_d2 = max_deg * max_deg
    for nid, (lon, lat) in bundle.nodes.items():
        d2 = (lon - tlon) ** 2 + (lat - tlat) ** 2
        if d2 < best_d2:
            best_d2 = d2
            best = nid
    return best


def access_estimates(
    *,
    landfall: tuple[float, float],
    hotspots: list[tuple[int, tuple[float, float]]],
    compound_tif_path: str | None,
    cache_dir: str,
    bbox_pad_deg: float = 1.0,
) -> list[dict] | None:
    """
    Return one estimate per hotspot: {rank, eta_hours, limiting_factor,
    confidence, max_depth_ft, miles}.

    Returns None if the OSM fetch fails and there's no cache to fall back
    on. Callers should use the heuristic path instead.

    `hotspots` is a list of (rank, (lon, lat)) pairs. `landfall` is the
    (lon, lat) staging origin.
    """
    if nx is None:
        return None

    # Bbox covers landfall and every hotspot with padding.
    lons = [landfall[0]] + [p[0] for _, p in hotspots]
    lats = [landfall[1]] + [p[1] for _, p in hotspots]
    bbox = (
        min(lons) - bbox_pad_deg, min(lats) - bbox_pad_deg,
        max(lons) + bbox_pad_deg, max(lats) + bbox_pad_deg,
    )

    ways = fetch_osm_roads(bbox, cache_dir)
    if not ways:
        return None

    bundle = build_graph(ways, compound_tif_path)
    if bundle is None:
        return None

    origin = _nearest_node(bundle, landfall)
    if origin is None:
        return None

    # Single-source shortest paths. For ~10k-node graphs this is fine
    # within the /api call budget.
    try:
        dist, pred = nx.single_source_dijkstra(
            bundle.graph, origin, weight="weight"
        )
    except Exception:
        return None

    confidence = "medium" if compound_tif_path else "low"
    out: list[dict] = []
    for rank, (lon, lat) in hotspots:
        tgt = _nearest_node(bundle, (lon, lat))
        if tgt is None or tgt not in dist:
            out.append({
                "hotspot_rank": rank,
                "eta_hours": None,
                "limiting_factor": "unknown",
                "confidence": "low",
                "max_depth_ft": None,
                "miles": None,
                "notes": "No path in OSM graph within 1° of landfall.",
            })
            continue

        hours_raw = dist[tgt] + _ACCESS_OVERHEAD_HR
        # Reconstruct path to compute max depth / total miles / any
        # severed link along the route.
        path_nodes = _reconstruct_path(pred, origin, tgt)
        max_depth = 0.0
        total_miles = 0.0
        severed_any = False
        for a, b in zip(path_nodes[:-1], path_nodes[1:]):
            edges = bundle.graph.get_edge_data(a, b) or {}
            best = None
            for e in edges.values():
                if best is None or e["weight"] < best["weight"]:
                    best = e
            if best is None:
                continue
            max_depth = max(max_depth, best["depth_ft"])
            total_miles += best["miles"]
            if best["severed"]:
                severed_any = True

        if severed_any:
            eta_hours = None
            limiting = "surge"
            confidence_r = "high"
        elif max_depth >= _SLOW_FT:
            eta_hours = round(hours_raw, 1)
            limiting = "surge"
            confidence_r = confidence
        else:
            eta_hours = round(hours_raw, 1)
            limiting = "road_closure"
            confidence_r = confidence

        out.append({
            "hotspot_rank": rank,
            "eta_hours": eta_hours,
            "limiting_factor": limiting,
            "confidence": confidence_r,
            "max_depth_ft": round(max_depth, 2),
            "miles": round(total_miles, 1),
            "notes": "OSM × compound-depth reachability.",
        })
    return out


def _reconstruct_path(pred: dict, src: int, tgt: int) -> list[int]:
    """Turn a networkx predecessor map into a node list src→tgt."""
    rev = [tgt]
    cur = tgt
    visited = {tgt}
    while cur != src:
        nxt_list = pred.get(cur)
        if not nxt_list:
            break
        nxt = nxt_list[0] if isinstance(nxt_list, list) else nxt_list
        if nxt in visited:  # cycle guard
            break
        rev.append(nxt)
        visited.add(nxt)
        cur = nxt
    rev.reverse()
    return rev
