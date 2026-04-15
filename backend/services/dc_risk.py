"""Helpers for data-center risk overlays and portfolio summaries."""

from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
from typing import Any


_REPO_ROOT = Path(__file__).parent.parent
_IBTRACS_CACHE = _REPO_ROOT / "data" / "ibtracs_cache.csv"
_CYCLONE_TRACK_CACHE = _REPO_ROOT / "data" / "dc_cyclone_tracks.geojson"


def _point_feature(lng: float, lat: float, properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "Feature",
        "properties": properties,
        "geometry": {"type": "Point", "coordinates": [lng, lat]},
    }


def _line_feature(coords: list[list[float]], properties: dict[str, Any]) -> dict[str, Any] | None:
    if len(coords) < 2:
        return None
    return {
        "type": "Feature",
        "properties": properties,
        "geometry": {"type": "LineString", "coordinates": coords},
    }


def build_dc_flood_zone_features(datacenters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    features = []
    for index, dc in enumerate(datacenters):
        zone = dc.get("fema_flood_zone")
        flood_depth = dc.get("jrc_flood_100yr_m")
        hazard_flood = float(dc.get("hazard_flood") or 0)
        if zone in (None, "", "UNMAPPED") and not flood_depth and hazard_flood <= 0:
            continue
        lat = dc.get("lat")
        lng = dc.get("lng")
        if lat is None or lng is None:
            continue
        flood_source = "raw"
        if zone in (None, "", "UNMAPPED") and not flood_depth:
            flood_source = "legacy_hazard_score"
        features.append(
            _point_feature(
                lng,
                lat,
                {
                    "id": f"dc-flood-{index}",
                    "datacenter": dc.get("name", "Unknown"),
                    "fema_flood_zone": zone,
                    "jrc_flood_100yr_m": flood_depth,
                    "hazard_flood": hazard_flood,
                    "risk_score": dc.get("risk_score", 0),
                    "flood_source": flood_source,
                },
            )
        )
    return features


def build_dc_power_dependency_features(datacenters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    features: list[dict[str, Any]] = []
    for index, dc in enumerate(datacenters):
        lat = dc.get("lat")
        lng = dc.get("lng")
        sub_lat = dc.get("substation_lat")
        sub_lng = dc.get("substation_lng")
        if None in (lat, lng, sub_lat, sub_lng):
            continue
        feature = _line_feature(
            [[lng, lat], [sub_lng, sub_lat]],
            {
                "id": f"dc-power-{index}",
                "datacenter": dc.get("name", "Unknown"),
                "substation_osm_id": dc.get("substation_osm_id"),
                "substation_shared_count": dc.get("substation_shared_count", 0),
                "substation_dist_km": dc.get("substation_dist_km"),
            },
        )
        if feature:
            features.append(feature)
    return features


def build_dc_network_dependency_features(datacenters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    features: list[dict[str, Any]] = []
    for index, dc in enumerate(datacenters):
        lat = dc.get("lat")
        lng = dc.get("lng")
        ixp_lat = dc.get("nearest_ixp_lat")
        ixp_lng = dc.get("nearest_ixp_lng")
        if None in (lat, lng, ixp_lat, ixp_lng):
            continue
        feature = _line_feature(
            [[lng, lat], [ixp_lng, ixp_lat]],
            {
                "id": f"dc-network-{index}",
                "datacenter": dc.get("name", "Unknown"),
                "ixp_id": dc.get("nearest_ixp_id"),
                "ixp_name": dc.get("nearest_ixp_name"),
                "ixp_count_50km": dc.get("ixp_count_50km", 0),
                "asn": dc.get("asn"),
            },
        )
        if feature:
            features.append(feature)
    return features


def build_dc_accumulation_cluster_features(datacenters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    features: list[dict[str, Any]] = []
    clusters: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for dc in datacenters:
        cluster_id = dc.get("substation_cluster_id") or dc.get("substation_osm_id")
        if not cluster_id:
            continue
        clusters[str(cluster_id)].append(dc)

    for cluster_id, cluster_members in clusters.items():
        flagged = [dc for dc in cluster_members if dc.get("accumulation_flag")]
        if not flagged and len(cluster_members) < 2:
            continue
        coords = [(dc.get("lat"), dc.get("lng")) for dc in cluster_members]
        coords = [(lat, lng) for lat, lng in coords if lat is not None and lng is not None]
        if not coords:
            continue
        avg_lat = sum(lat for lat, _ in coords) / len(coords)
        avg_lng = sum(lng for _, lng in coords) / len(coords)
        max_risk = max(float(dc.get("risk_score") or 0) for dc in cluster_members)
        features.append(
            _point_feature(
                avg_lng,
                avg_lat,
                {
                    "id": f"dc-cluster-{cluster_id}",
                    "cluster_id": cluster_id,
                    "member_count": len(cluster_members),
                    "flagged_count": len(flagged),
                    "max_risk_score": round(max_risk, 1),
                    "datacenters": [dc.get("name", "Unknown") for dc in cluster_members],
                },
            )
        )
    return features


def _coerce_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_dc_cyclone_track_features(datacenters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not _IBTRACS_CACHE.exists():
        return []

    cache_inputs = [
        _IBTRACS_CACHE.stat().st_mtime,
        max((dc.get("ibtracs_track_density") or 0) for dc in datacenters) if datacenters else 0,
    ]
    if _CYCLONE_TRACK_CACHE.exists() and _CYCLONE_TRACK_CACHE.stat().st_mtime >= max(cache_inputs):
        import json

        try:
            data = json.loads(_CYCLONE_TRACK_CACHE.read_text(encoding="utf-8"))
            return list(data.get("features", []))
        except Exception:
            pass

    active_sites = [
        dc
        for dc in datacenters
        if (dc.get("ibtracs_track_density") or 0) > 0
        and dc.get("lat") is not None
        and dc.get("lng") is not None
    ]
    if not active_sites:
        return []

    lat_min = min(float(dc["lat"]) for dc in active_sites) - 3.0
    lat_max = max(float(dc["lat"]) for dc in active_sites) + 3.0
    lng_min = min(float(dc["lng"]) for dc in active_sites) - 3.0
    lng_max = max(float(dc["lng"]) for dc in active_sites) + 3.0

    tracks: dict[str, list[list[float]]] = defaultdict(list)
    with _IBTRACS_CACHE.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(row for idx, row in enumerate(handle) if idx != 1)
        for row in reader:
            lat = _coerce_float(row.get("LAT"))
            lng = _coerce_float(row.get("LON"))
            if lat is None or lng is None:
                continue
            if not (lat_min <= lat <= lat_max and lng_min <= lng <= lng_max):
                continue
            sid = str(row.get("SID") or "").strip()
            if not sid:
                continue
            tracks[sid].append([lng, lat])

    features: list[dict[str, Any]] = []
    for sid, coords in list(tracks.items())[:150]:
        feature = _line_feature(
            coords,
            {
                "id": f"ibtracs-{sid}",
                "storm_id": sid,
                "point_count": len(coords),
            },
        )
        if feature:
            features.append(feature)

    import json

    _CYCLONE_TRACK_CACHE.write_text(
        json.dumps({"type": "FeatureCollection", "features": features}, ensure_ascii=False),
        encoding="utf-8",
    )
    return features


def build_dc_risk_overlays(datacenters: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    return {
        "dc_flood_zones": build_dc_flood_zone_features(datacenters),
        "dc_power_dependencies": build_dc_power_dependency_features(datacenters),
        "dc_network_dependencies": build_dc_network_dependency_features(datacenters),
        "dc_accumulation_clusters": build_dc_accumulation_cluster_features(datacenters),
        "dc_cyclone_tracks": build_dc_cyclone_track_features(datacenters),
    }


def compute_datacenter_risk_summary(datacenters: list[dict[str, Any]]) -> dict[str, Any]:
    scores = [float(dc.get("risk_score") or 0) for dc in datacenters]
    total = len(datacenters)
    high_risk_count = sum(1 for score in scores if score >= 70)
    flagged_count = sum(1 for dc in datacenters if bool(dc.get("accumulation_flag")))
    flood_zone_count = sum(1 for dc in datacenters if dc.get("fema_flood_zone") not in (None, "", "UNMAPPED"))
    avg_score = round(sum(scores) / total, 1) if total else 0.0

    return {
        "total_datacenters": total,
        "high_risk_count": high_risk_count,
        "accumulation_flagged_count": flagged_count,
        "flood_zone_count": flood_zone_count,
        "average_risk_score": avg_score,
        "max_risk_score": round(max(scores), 1) if scores else 0.0,
    }
