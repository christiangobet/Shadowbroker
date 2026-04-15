#!/usr/bin/env python3
"""
Layer 3 — dependency enrichment for datacenters_geocoded.json.

This stage annotates data centers with nearest shared grid/node and network
dependency metadata. It prefers local cache files when available and keeps all
online lookups optional so the pipeline remains usable offline.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
from pathlib import Path
from typing import Any


logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).parent.parent.parent
DC_PATH = _REPO_ROOT / "backend" / "data" / "datacenters_geocoded.json"
SUBSTATION_CACHE_PATH = _REPO_ROOT / "backend" / "data" / "osm_substations_cache.json"
IXP_CACHE_PATH = _REPO_ROOT / "backend" / "data" / "peeringdb_ix_cache.json"
_EARTH_RADIUS_KM = 6371.0


def _deg_to_rad(value: float) -> float:
    return value * math.pi / 180.0


def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    dlat = _deg_to_rad(lat2 - lat1)
    dlng = _deg_to_rad(lng2 - lng1)
    a = math.sin(dlat / 2) ** 2 + math.cos(_deg_to_rad(lat1)) * math.cos(_deg_to_rad(lat2)) * math.sin(dlng / 2) ** 2
    return 2 * _EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def _load_cache(path: Path) -> list[dict[str, Any]]:
    try:
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, list) else payload.get("items", [])
    except Exception as exc:
        logger.warning("Failed reading cache %s: %s", path, exc)
    return []


def _nearest_point(lat: float, lng: float, points: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, float | None]:
    nearest = None
    nearest_km = None
    for point in points:
        p_lat = point.get("lat")
        p_lng = point.get("lng")
        if p_lat is None or p_lng is None:
            continue
        dist = _haversine_km(lat, lng, float(p_lat), float(p_lng))
        if nearest_km is None or dist < nearest_km:
            nearest = point
            nearest_km = dist
    return nearest, nearest_km


def annotate_dependency_fields(
    datacenters: list[dict[str, Any]],
    *,
    substations: list[dict[str, Any]] | None = None,
    ixps: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    substations = substations or []
    ixps = ixps or []
    for dc in datacenters:
        lat = dc.get("lat")
        lng = dc.get("lng")
        if lat is None or lng is None:
            continue
        lat_f = float(lat)
        lng_f = float(lng)

        nearest_substation, dist_km = _nearest_point(lat_f, lng_f, substations)
        if nearest_substation:
            dc["substation_osm_id"] = str(nearest_substation.get("id") or nearest_substation.get("osm_id") or "")
            dc["substation_dist_km"] = round(dist_km or 0.0, 1)
            dc["substation_cluster_id"] = dc["substation_osm_id"]
            dc["substation_lat"] = nearest_substation.get("lat")
            dc["substation_lng"] = nearest_substation.get("lng")

        nearby_ixps: list[dict[str, Any]] = []
        nearest_ixp = None
        nearest_ixp_dist = None
        for ixp in ixps:
            ixp_lat = ixp.get("lat")
            ixp_lng = ixp.get("lng")
            if ixp_lat is None or ixp_lng is None:
                continue
            dist = _haversine_km(lat_f, lng_f, float(ixp_lat), float(ixp_lng))
            if dist <= 50:
                nearby_ixps.append(ixp)
            if nearest_ixp_dist is None or dist < nearest_ixp_dist:
                nearest_ixp = ixp
                nearest_ixp_dist = dist

        dc["ixp_ids"] = [str(ixp.get("id")) for ixp in nearby_ixps if ixp.get("id") is not None]
        dc["ixp_count_50km"] = len(dc["ixp_ids"])
        dc["nearest_ixp_km"] = round(nearest_ixp_dist, 1) if nearest_ixp_dist is not None else None
        dc["fibre_path_count"] = max(1, len(dc["ixp_ids"])) if dc["ixp_ids"] else 0
        if nearest_ixp:
            dc["nearest_ixp_id"] = str(nearest_ixp.get("id")) if nearest_ixp.get("id") is not None else None
            dc["nearest_ixp_name"] = nearest_ixp.get("name")
            dc["nearest_ixp_lat"] = nearest_ixp.get("lat")
            dc["nearest_ixp_lng"] = nearest_ixp.get("lng")
            dc["asn"] = str(nearest_ixp.get("asn")) if nearest_ixp.get("asn") not in (None, "") else dc.get("asn")
        dc["water_stress_idx"] = dc.get("water_stress_idx", 0.0)

    cluster_counts: dict[str, int] = {}
    for dc in datacenters:
        cluster_id = dc.get("substation_cluster_id")
        if cluster_id:
            cluster_counts[str(cluster_id)] = cluster_counts.get(str(cluster_id), 0) + 1

    for dc in datacenters:
        cluster_id = dc.get("substation_cluster_id")
        if cluster_id:
            dc["substation_shared_count"] = cluster_counts.get(str(cluster_id), 1)
        else:
            dc["substation_shared_count"] = dc.get("substation_shared_count", 0)

    return datacenters


def enrich(force: bool = False, dc_path: Path = DC_PATH) -> tuple[list[dict[str, Any]], dict[str, float]]:
    dcs = json.loads(dc_path.read_text(encoding="utf-8"))
    substations = _load_cache(SUBSTATION_CACHE_PATH)
    ixps = _load_cache(IXP_CACHE_PATH)
    valid_dcs = [dc for dc in dcs if dc.get("lat") is not None and dc.get("lng") is not None]

    if not valid_dcs:
        return dcs, {}

    annotate_dependency_fields(valid_dcs, substations=substations, ixps=ixps)

    total = len(valid_dcs)
    coverage = {
        field: round(sum(1 for dc in valid_dcs if dc.get(field) not in (None, "", [], {})) / total * 100, 1)
        for field in (
            "substation_osm_id",
            "substation_cluster_id",
            "substation_shared_count",
            "ixp_count_50km",
            "fibre_path_count",
            "water_stress_idx",
            "asn",
        )
    }

    tmp_path = dc_path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(dcs, ensure_ascii=False, indent=None), encoding="utf-8")
    os.replace(tmp_path, dc_path)
    logger.info("L3 dependency enrichment complete")
    return dcs, coverage


if __name__ == "__main__":
    import time as _time

    parser = argparse.ArgumentParser(description="Layer 3 dependency enrichment")
    parser.add_argument("--force", action="store_true", help="Overwrite existing non-null L3 fields")
    args = parser.parse_args()

    try:
        from services.sync_meta import write_result, set_running

        set_running("enrich_l3_dependencies")
    except Exception:
        write_result = set_running = None  # type: ignore

    t0 = _time.time()
    try:
        _, coverage = enrich(force=args.force)
        if write_result:
            write_result(
                "enrich_l3_dependencies",
                status="success",
                duration_s=_time.time() - t0,
                coverage=coverage,
                log_tail="Layer 3 dependency enrichment complete",
            )
    except Exception as exc:
        if write_result:
            write_result(
                "enrich_l3_dependencies",
                status="error",
                duration_s=_time.time() - t0,
                error=str(exc),
            )
        raise
