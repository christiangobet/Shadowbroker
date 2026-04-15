#!/usr/bin/env python3
"""
Layer 2 — hazard exposure enrichment for datacenters_geocoded.json.

Populates (never overwrites existing non-null values unless --force):
  jrc_flood_100yr_m     — JRC GloFAS flood depth at 100-yr return period (m)
  usgs_pga_10pct_50yr   — USGS Design Maps PGA (g); US mainland + territories only
  ibtracs_track_density — IBTrACS tropical cyclone tracks/decade within 200 km
  wildfire_days_50km    — NASA FIRMS fire detections/yr within 50 km (7-day × 52)
  heat_extreme_days     — skipped: requires ERA5 CDS credentials
  hazard_eq             — updated 0–100 seismic score from usgs_pga_10pct_50yr
  hazard_flood          — updated 0–100 flood score from jrc_flood_100yr_m
  hazard_cyclone        — updated 0–100 cyclone score from ibtracs_track_density
  hazard_fire           — updated 0–100 fire score from wildfire_days_50km

Data sources (all free, no API key required):
  1. NOAA IBTrACS CSV   → ibtracs_track_density  (global; ~75 MB, cached locally)
  2. USGS Design Maps   → usgs_pga_10pct_50yr    (US sites only, ~1 600 DCs)
  3. NASA FIRMS CSV     → wildfire_days_50km     (global 7-day CSV, no key)
  4. JRC GeoServer WMS  → jrc_flood_100yr_m      (batched by 0.5° grid cell)

Usage:
    cd backend
    ../backend/venv/bin/python3 -m scripts.enrich_l2_hazard
    ../backend/venv/bin/python3 -m scripts.enrich_l2_hazard --force   # overwrite existing
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional

import httpx
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).parent.parent.parent
DC_PATH = _REPO_ROOT / "backend" / "data" / "datacenters_geocoded.json"
CACHE_DIR = _REPO_ROOT / "backend" / "data"
IBTRACS_CACHE = CACHE_DIR / "ibtracs_cache.csv"

_EARTH_RADIUS_KM = 6371.0

# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def _interp_score(value: float, breakpoints: list[tuple[float, int]]) -> int:
    """
    Piecewise-linear interpolation between (raw_value, score) breakpoints.
    Returns int 0–100.
    """
    if value <= breakpoints[0][0]:
        return breakpoints[0][1]
    if value >= breakpoints[-1][0]:
        return breakpoints[-1][1]
    for i in range(len(breakpoints) - 1):
        x0, y0 = breakpoints[i]
        x1, y1 = breakpoints[i + 1]
        if x0 <= value <= x1:
            t = (value - x0) / (x1 - x0)
            return round(y0 + t * (y1 - y0))
    return breakpoints[-1][1]


# Empirical score breakpoints — tuned to match INFORM 2023 score ranges
_PGA_BREAKPOINTS    = [(0.0, 0), (0.04, 10), (0.1, 30), (0.2, 55), (0.4, 80), (0.8, 100)]
_FLOOD_BREAKPOINTS  = [(0.0, 0), (0.1, 15), (0.3, 35), (1.0, 65), (2.0, 85), (4.0, 100)]
_CYCLONE_BREAKPOINTS = [(0, 0), (1, 15), (3, 35), (8, 65), (15, 85), (25, 100)]
_FIRE_BREAKPOINTS   = [(0, 0), (3, 15), (10, 40), (20, 65), (40, 85), (70, 100)]


# ---------------------------------------------------------------------------
# Spatial helpers
# ---------------------------------------------------------------------------

def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlng / 2) ** 2)
    return 2 * _EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def _rad_array(lats: list[float], lngs: list[float]) -> np.ndarray:
    return np.radians(np.column_stack([lats, lngs]))


# ---------------------------------------------------------------------------
# Step 1: IBTrACS — cyclone track density
# ---------------------------------------------------------------------------

_IBTRACS_URL = (
    "https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs"
    "/v04r01/access/csv/ibtracs.ALL.list.v04r01.csv"
)
_IBTRACS_RADIUS_KM = 200.0
_IBTRACS_YEARS = 30  # compute density over last 30 years → divide by 3 for per-decade


def _download_ibtracs(timeout: int = 300) -> bool:
    """Download IBTrACS ALL CSV to cache if not already present. Returns True on success."""
    if IBTRACS_CACHE.exists():
        size_mb = IBTRACS_CACHE.stat().st_size / 1_048_576
        logger.info(f"  IBTrACS cache found ({size_mb:.1f} MB): {IBTRACS_CACHE}")
        return True
    logger.info(f"  Downloading IBTrACS CSV (~75 MB) → {IBTRACS_CACHE} ...")
    try:
        with httpx.stream("GET", _IBTRACS_URL, timeout=timeout,
                          follow_redirects=True,
                          headers={"User-Agent": "ShadowBroker-enrichment/1.0"}) as r:
            r.raise_for_status()
            with open(IBTRACS_CACHE, "wb") as f:
                for chunk in r.iter_bytes(chunk_size=65536):
                    f.write(chunk)
        logger.info(f"  IBTrACS download complete ({IBTRACS_CACHE.stat().st_size / 1_048_576:.1f} MB)")
        return True
    except Exception as e:
        logger.warning(f"  IBTrACS download failed: {e}")
        if IBTRACS_CACHE.exists():
            IBTRACS_CACHE.unlink()
        return False


def build_ibtracs_index(current_year: int) -> Optional[tuple[cKDTree, list[dict]]]:
    """
    Parse IBTrACS CSV and return a cKDTree of tropical track points
    (filtered to last _IBTRACS_YEARS years) plus list of {lat, lng, sid} records.
    Returns None if parse fails.
    """
    logger.info("  Parsing IBTrACS CSV (this may take 20–30 s) ...")
    try:
        # Row 0 = column headers; row 1 = units row (skip it)
        df = pd.read_csv(
            IBTRACS_CACHE,
            skiprows=[1],
            usecols=["SID", "SEASON", "ISO_TIME", "LAT", "LON", "NATURE"],
            dtype={"SID": str, "SEASON": str, "NATURE": str},
            na_values=["", " "],
            low_memory=False,
        )
    except Exception as e:
        logger.warning(f"  IBTrACS parse error: {e}")
        return None

    # Keep only tropical systems (NATURE TS = tropical storm/hurricane/typhoon)
    df = df[df["NATURE"].str.strip().isin(["TS", "TY", "SS"])].copy()

    # Season filter: last _IBTRACS_YEARS years
    min_season = current_year - _IBTRACS_YEARS
    df["SEASON"] = pd.to_numeric(df["SEASON"], errors="coerce")
    df = df[df["SEASON"] >= min_season]

    # Parse coordinates
    df["LAT"] = pd.to_numeric(df["LAT"], errors="coerce")
    df["LON"] = pd.to_numeric(df["LON"], errors="coerce")
    df = df.dropna(subset=["LAT", "LON"])
    df = df[(df["LAT"].between(-90, 90)) & (df["LON"].between(-180, 180))]

    records = [{"lat": row.LAT, "lng": row.LON, "sid": row.SID}
               for row in df.itertuples(index=False)]
    if not records:
        logger.warning("  IBTrACS: no valid tropical track points found")
        return None

    tree = cKDTree(_rad_array([r["lat"] for r in records],
                               [r["lng"] for r in records]))
    logger.info(f"  IBTrACS: {len(records):,} track points from {df['SID'].nunique():,} storms "
                f"({min_season}–{current_year})")
    return tree, records


def compute_ibtracs_density(
    dc: dict,
    tree: cKDTree,
    records: list[dict],
) -> float:
    """
    Count unique storm tracks (SIDs) passing within _IBTRACS_RADIUS_KM of dc.
    Returns tracks/decade (divide total by _IBTRACS_YEARS/10).
    """
    radius_rad = _IBTRACS_RADIUS_KM / _EARTH_RADIUS_KM
    pt = np.radians([dc["lat"], dc["lng"]])
    idxs = tree.query_ball_point(pt, r=radius_rad)
    if not idxs:
        return 0.0
    unique_sids = {records[i]["sid"] for i in idxs}
    # Filter: verify haversine (cKDTree uses chord approximation)
    confirmed = sum(
        1 for sid in unique_sids
        # One pass to confirm ≥1 point of each storm is truly within radius
        if any(_haversine_km(dc["lat"], dc["lng"],
                             records[i]["lat"], records[i]["lng"]) <= _IBTRACS_RADIUS_KM
               for i in idxs if records[i]["sid"] == sid)
    )
    return round(confirmed / (_IBTRACS_YEARS / 10), 2)


# ---------------------------------------------------------------------------
# Step 2: USGS Design Maps — PGA for US sites
# ---------------------------------------------------------------------------

_USGS_DESIGN_MAPS_URL = "https://earthquake.usgs.gov/ws/designmaps/asce7-16.json"

# Bounding boxes that the USGS NSHM covers
_US_REGIONS: list[tuple[float, float, float, float]] = [
    (24.0, -125.0, 50.5, -65.0),   # Contiguous US
    (51.0, -180.0, 72.0, -130.0),  # Alaska
    (18.0, -160.5, 22.5, -154.5),  # Hawaii
    (17.5, -68.0, 18.6, -65.0),    # Puerto Rico / USVI
    (13.0, 144.5, 21.0, 146.0),    # Guam / CNMI
]


def _is_us_site(lat: float, lng: float) -> bool:
    return any(
        lat_min <= lat <= lat_max and lng_min <= lng <= lng_max
        for (lat_min, lng_min, lat_max, lng_max) in _US_REGIONS
    )


def fetch_usgs_pga(lat: float, lng: float, client: httpx.Client) -> Optional[float]:
    """
    Query USGS Design Maps web service and return PGA (g) for rock site (Site Class C).
    Returns None if the site is outside the NSHM coverage area or the request fails.
    """
    try:
        resp = client.get(
            _USGS_DESIGN_MAPS_URL,
            params={
                "latitude": round(lat, 4),
                "longitude": round(lng, 4),
                "siteClass": "C",
                "riskCategory": "III",
                "title": "hazard",
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json().get("response", {}).get("data", {})
        pga = data.get("pga")
        return round(float(pga), 4) if pga is not None else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Step 3: NASA FIRMS — wildfire density (7-day CSV, no key)
# ---------------------------------------------------------------------------

_FIRMS_7D_URL = (
    "https://firms.modaps.eosdis.nasa.gov/data/active_fire"
    "/modis-c6.1/csv/MODIS_C6_1_Global_7d.csv"
)
_FIRE_RADIUS_KM = 50.0
_FIRE_ANNUAL_SCALE = 365.0 / 7.0   # scale 7-day count → annual estimate


def fetch_firms_data(timeout: int = 120) -> Optional[pd.DataFrame]:
    """Download FIRMS 7-day global active fire CSV. Returns DataFrame or None."""
    logger.info("  Downloading NASA FIRMS 7-day global CSV ...")
    try:
        resp = httpx.get(
            _FIRMS_7D_URL,
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": "ShadowBroker-enrichment/1.0"},
        )
        resp.raise_for_status()
        from io import StringIO
        df = pd.read_csv(StringIO(resp.text))
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        df["acq_date"] = pd.to_datetime(df["acq_date"], errors="coerce")
        df = df.dropna(subset=["latitude", "longitude"])
        logger.info(f"  FIRMS: {len(df):,} fire detections in last 7 days")
        return df
    except Exception as e:
        logger.warning(f"  FIRMS download failed: {e}")
        return None


def build_firms_index(df: pd.DataFrame) -> tuple[cKDTree, pd.DataFrame]:
    """Build cKDTree over FIRMS detections."""
    tree = cKDTree(_rad_array(df["latitude"].tolist(), df["longitude"].tolist()))
    return tree, df


def compute_fire_days(
    dc: dict,
    tree: cKDTree,
    df: pd.DataFrame,
) -> float:
    """
    Count unique fire-detection dates within _FIRE_RADIUS_KM of dc (7-day window),
    scaled to an annual estimate.
    """
    radius_rad = _FIRE_RADIUS_KM / _EARTH_RADIUS_KM
    pt = np.radians([dc["lat"], dc["lng"]])
    idxs = tree.query_ball_point(pt, r=radius_rad)
    if not idxs:
        return 0.0
    subset = df.iloc[idxs]
    unique_days = subset["acq_date"].nunique()
    return round(unique_days * _FIRE_ANNUAL_SCALE, 1)


# ---------------------------------------------------------------------------
# Step 4: JRC GeoServer WMS — flood depth at 100-yr RP (batched by 0.5° grid)
# ---------------------------------------------------------------------------

_JRC_WMS_BASE = "https://geoserver.jrc.ec.europa.eu/geoserver/FLOODS/wms"
_JRC_LAYER = "FLOODS:floodMapGL_rp100y"
_JRC_RATE_S = 0.3   # polite rate limit (calls per second)

# Possible property-name keys returned by the JRC GetFeatureInfo JSON response
_JRC_VALUE_KEYS = ("GRAY_INDEX", "value", "band1", "BANDS", "Gray", "gray_index")


def _cell_key(lat: float, lng: float) -> tuple[float, float]:
    """Round to nearest 0.5° grid cell centre."""
    return (round(lat * 2) / 2, round(lng * 2) / 2)


def query_jrc_flood_cell(
    cell_lat: float,
    cell_lng: float,
    client: httpx.Client,
) -> Optional[float]:
    """
    GetFeatureInfo for the 0.5°×0.5° cell centred on (cell_lat, cell_lng).
    Returns flood depth in metres, or None if no data / service unavailable.
    """
    delta = 0.25
    bbox = f"{cell_lng - delta},{cell_lat - delta},{cell_lng + delta},{cell_lat + delta}"
    params = {
        "SERVICE": "WMS",
        "VERSION": "1.1.1",
        "REQUEST": "GetFeatureInfo",
        "LAYERS": _JRC_LAYER,
        "QUERY_LAYERS": _JRC_LAYER,
        "STYLES": "",
        "BBOX": bbox,
        "WIDTH": "10",
        "HEIGHT": "10",
        "FORMAT": "image/jpeg",
        "INFO_FORMAT": "application/json",
        "X": "5",
        "Y": "5",
        "SRS": "EPSG:4326",
    }
    try:
        resp = client.get(_JRC_WMS_BASE, params=params, timeout=12)
        if resp.status_code != 200:
            return None
        data = resp.json()
        features = data.get("features", [])
        if not features:
            return None
        props = features[0].get("properties", {})
        for key in _JRC_VALUE_KEYS:
            if key in props and props[key] is not None:
                try:
                    val = float(props[key])
                    return round(val, 2) if val > 0 else 0.0
                except (ValueError, TypeError):
                    continue
        return None
    except Exception:
        return None


def fetch_jrc_flood_batch(
    dcs: list[dict],
    force: bool = False,
) -> dict[tuple[float, float], Optional[float]]:
    """
    Query JRC flood depth for all unique 0.5° grid cells occupied by dcs.
    Returns {cell_key: depth_m_or_None}.
    """
    # Build map: cell → [dc indices]
    cells_needed: set[tuple[float, float]] = set()
    for dc in dcs:
        if dc.get("jrc_flood_100yr_m") is None or force:
            cells_needed.add(_cell_key(dc["lat"], dc["lng"]))

    if not cells_needed:
        logger.info("  JRC flood: all values already filled — skipping")
        return {}

    logger.info(f"  JRC flood: querying {len(cells_needed)} grid cells via WMS GetFeatureInfo...")
    results: dict[tuple[float, float], Optional[float]] = {}
    cells_list = sorted(cells_needed)
    success = 0
    skip = 0

    # Test the service with the first cell before committing to a full run
    with httpx.Client(headers={"User-Agent": "ShadowBroker-enrichment/1.0"}) as client:
        test_lat, test_lng = cells_list[0]
        test_val = query_jrc_flood_cell(test_lat, test_lng, client)
        if test_val is None:
            # Check if the service is reachable at all
            try:
                probe = client.get(_JRC_WMS_BASE,
                                   params={"SERVICE": "WMS", "REQUEST": "GetCapabilities"},
                                   timeout=15)
                if probe.status_code != 200:
                    logger.warning(f"  JRC WMS not reachable (HTTP {probe.status_code}) — skipping flood step")
                    return {}
                # Service is up but this cell has no flood data — proceed
            except Exception as e:
                logger.warning(f"  JRC WMS unreachable: {e} — skipping flood step")
                return {}

        results[cells_list[0]] = test_val
        if test_val is not None:
            success += 1
        else:
            skip += 1

        for i, (clat, clng) in enumerate(cells_list[1:], start=1):
            time.sleep(_JRC_RATE_S)
            val = query_jrc_flood_cell(clat, clng, client)
            results[(clat, clng)] = val
            if val is not None:
                success += 1
            else:
                skip += 1
            if i % 100 == 0:
                logger.info(f"    JRC progress: {i+1}/{len(cells_list)} cells "
                            f"({success} with data, {skip} no-data)")

    logger.info(f"  JRC flood: {success}/{len(cells_list)} cells with flood data")
    return results


# ---------------------------------------------------------------------------
# Main enrichment
# ---------------------------------------------------------------------------

def enrich(dc_path: Path = DC_PATH, force: bool = False) -> tuple[list[dict], dict]:
    import datetime
    current_year = datetime.date.today().year

    logger.info(f"Loading {dc_path} ...")
    dcs = json.loads(dc_path.read_text(encoding="utf-8"))
    logger.info(f"Loaded {len(dcs)} data center records")
    valid_dcs = [d for d in dcs if d.get("lat") is not None and d.get("lng") is not None]

    # ── Step 1: IBTrACS — cyclone track density ──────────────────────────────
    logger.info("Step 1: IBTrACS cyclone track density ...")
    ibtracs_ok = _download_ibtracs()
    ibtracs_index = None
    if ibtracs_ok:
        ibtracs_index = build_ibtracs_index(current_year)

    ibtracs_updated = 0
    if ibtracs_index:
        tree_ibt, records_ibt = ibtracs_index
        for dc in valid_dcs:
            if dc.get("ibtracs_track_density") is not None and not force:
                continue
            density = compute_ibtracs_density(dc, tree_ibt, records_ibt)
            dc["ibtracs_track_density"] = density
            dc["hazard_cyclone"] = _interp_score(density, _CYCLONE_BREAKPOINTS)
            ibtracs_updated += 1
        logger.info(f"  ibtracs_track_density set/updated: {ibtracs_updated}")
    else:
        logger.warning("  Skipping cyclone density — IBTrACS data unavailable")

    # ── Step 2: USGS Design Maps — PGA (US sites only) ───────────────────────
    logger.info("Step 2: USGS Design Maps PGA (US sites only) ...")
    us_dcs = [dc for dc in valid_dcs
              if _is_us_site(dc["lat"], dc["lng"])
              and (dc.get("usgs_pga_10pct_50yr") is None or force)]
    logger.info(f"  US sites to query: {len(us_dcs)}")

    pga_updated = 0
    if us_dcs:
        with httpx.Client(
            headers={"User-Agent": "ShadowBroker-enrichment/1.0"},
            follow_redirects=True,
        ) as client:
            for i, dc in enumerate(us_dcs):
                pga = fetch_usgs_pga(dc["lat"], dc["lng"], client)
                if pga is not None:
                    dc["usgs_pga_10pct_50yr"] = pga
                    dc["hazard_eq"] = _interp_score(pga, _PGA_BREAKPOINTS)
                    pga_updated += 1
                if i % 100 == 0 and i > 0:
                    logger.info(f"    USGS progress: {i}/{len(us_dcs)} sites "
                                f"({pga_updated} PGA values retrieved)")
                time.sleep(0.05)  # ~20 req/s — polite rate limit
        logger.info(f"  usgs_pga_10pct_50yr set/updated: {pga_updated}")
    else:
        logger.info("  No US sites need updating — skipping USGS step")

    # ── Step 3: NASA FIRMS — wildfire density ────────────────────────────────
    logger.info("Step 3: NASA FIRMS wildfire density ...")
    firms_df = fetch_firms_data()

    fire_updated = 0
    if firms_df is not None and len(firms_df) > 0:
        tree_fire, firms_indexed = build_firms_index(firms_df)
        for dc in valid_dcs:
            if dc.get("wildfire_days_50km") is not None and not force:
                continue
            fire_days = compute_fire_days(dc, tree_fire, firms_indexed)
            dc["wildfire_days_50km"] = fire_days
            dc["hazard_fire"] = _interp_score(fire_days, _FIRE_BREAKPOINTS)
            fire_updated += 1
        logger.info(f"  wildfire_days_50km set/updated: {fire_updated}")
    else:
        logger.warning("  Skipping wildfire density — FIRMS data unavailable")

    # ── Step 4: JRC WMS — flood depth at 100-yr RP ───────────────────────────
    logger.info("Step 4: JRC GeoServer flood depth (100-yr RP) ...")
    cell_results = fetch_jrc_flood_batch(valid_dcs, force=force)

    flood_updated = 0
    if cell_results:
        for dc in valid_dcs:
            if dc.get("jrc_flood_100yr_m") is not None and not force:
                continue
            cell = _cell_key(dc["lat"], dc["lng"])
            depth = cell_results.get(cell)
            if depth is not None:
                dc["jrc_flood_100yr_m"] = depth
                dc["hazard_flood"] = _interp_score(depth, _FLOOD_BREAKPOINTS)
                flood_updated += 1
        logger.info(f"  jrc_flood_100yr_m set/updated: {flood_updated}")
    else:
        logger.info("  JRC flood step produced no results")

    # ── Step 5: Coverage summary ─────────────────────────────────────────────
    total = len(valid_dcs)
    coverage_fields = [
        "jrc_flood_100yr_m", "usgs_pga_10pct_50yr",
        "ibtracs_track_density", "wildfire_days_50km",
        "hazard_eq", "hazard_flood", "hazard_cyclone", "hazard_fire",
    ]
    coverage: dict[str, float] = {}
    for field in coverage_fields:
        filled = sum(1 for d in valid_dcs if d.get(field) is not None)
        pct = filled / total * 100 if total else 0
        coverage[field] = round(pct, 1)
        logger.info(f"  {field}: {filled}/{total} ({pct:.1f}% coverage)")

    # ── Atomic write ─────────────────────────────────────────────────────────
    logger.info(f"Writing enriched data to {dc_path} ...")
    tmp_path = dc_path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(dcs, ensure_ascii=False, indent=None), encoding="utf-8")
    os.replace(tmp_path, dc_path)
    logger.info("Done.")
    return dcs, coverage


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    import time as _time

    parser = argparse.ArgumentParser(description="Layer 2 hazard exposure enrichment")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing non-null L2 fields")
    args = parser.parse_args()

    try:
        from services.sync_meta import write_result, set_running
        set_running("enrich_l2_hazard")
    except Exception:
        write_result = set_running = None  # type: ignore

    t0 = _time.time()
    log_lines: list[str] = []
    _orig_info = logger.info

    def _tee(msg: str, *a, **kw):
        log_lines.append(str(msg))
        _orig_info(msg, *a, **kw)
    logger.info = _tee  # type: ignore

    try:
        _, coverage = enrich(force=args.force)
        if write_result:
            write_result(
                "enrich_l2_hazard",
                status="success",
                duration_s=_time.time() - t0,
                coverage=coverage,
                log_tail="\n".join(log_lines[-60:]),
            )
    except Exception as exc:
        if write_result:
            write_result(
                "enrich_l2_hazard",
                status="error",
                duration_s=_time.time() - t0,
                log_tail="\n".join(log_lines[-60:]),
                error=str(exc),
            )
        raise
