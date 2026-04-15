#!/usr/bin/env python3
"""
Layer 1 — physical asset enrichment for datacenters_geocoded.json.

Populates (never overwrites existing non-null values unless --force):
  operator_type  — "hyperscaler" | "colocation" | "enterprise"
  tier_rating    — 1 | 2 | 3 | 4  (Uptime Institute)
  mw_capacity    — float (IT load in MW; low coverage — only where OSM tags exist)
  year_built     — int
  cooling_type   — "air" | "liquid" | "hybrid"
  floor_level    — "above" | "basement" | "mixed"

Data sources (all free, no API key required):
  1. Company name heuristics         → operator_type        (pure lookup, no network)
  2. OSM Overpass API                → tier_rating, year_built, cooling_type, floor_level, mw_capacity
     Query is tag-selective: only fetches DCs that actually carry enrichment tags,
     keeping the result set small and avoiding gateway timeouts.
  3. Uptime Institute public JSON    → tier_rating (authoritative; ~841 certified facilities)
     Endpoint: /index.php?option=com_tierachievement&task=map.retrieveMappingInformation
     Returns every Tier-certified facility with lat/lng. Spatial matched at ≤1 km.

Usage:
    cd backend
    ../backend/venv/bin/python3 -m scripts.enrich_l1_physical
    ../backend/venv/bin/python3 -m scripts.enrich_l1_physical --force   # overwrite existing

Reads and overwrites:  backend/data/datacenters_geocoded.json
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import time
from pathlib import Path
from typing import Optional

import httpx
import numpy as np
from scipy.spatial import cKDTree

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).parent.parent.parent
DC_PATH = _REPO_ROOT / "backend" / "data" / "datacenters_geocoded.json"

_EARTH_RADIUS_KM = 6371.0

# ---------------------------------------------------------------------------
# Operator type classification — company name heuristics
# ---------------------------------------------------------------------------

# Companies that own and operate their own infrastructure at scale
_HYPERSCALER_TOKENS = {
    "amazon", "aws", "google", "alphabet", "microsoft", "azure",
    "meta", "facebook", "apple", "alibaba", "aliyun", "tencent",
    "baidu", "oracle", "salesforce", "ibm", "huawei", "bytedance",
    "cloudflare", "fastly", "akamai",
}

# Third-party colocation and hosting providers
_COLO_TOKENS = {
    "equinix", "digital realty", "cyrusone", "iron mountain", "ntt",
    "colt", "verizon", "lumen", "centurylink", "zayo", "switch",
    "qts", "coresite", "sabey", "cologix", "aligned", "databank",
    "flexential", "internap", "navisite", "peak 10", "iomart",
    "global switch", "interxion", "datacenter", "data center",
    "colocation", "colo", "telehouse", "teraco", "ark data",
    "pulsant", "virtus", "kao data", "scala", "ascenty", "odata",
    "serverius", "leasewebhosting", "leaseweb", "hetzner", "ovh",
    "ionos", "1&1", "rackspace", "iomart", "netwise", "comxo",
    "host", "hosting", "telecity", "volico", "navisite", "bluebridge",
    "zone4", "datalink", "colocrossing", "colocity", "c7", "vaultus",
    "datagryd", "volico", "navisite", "green house", "mindshift",
    "expedient", "fibernet", "involta", "latisys", "xo communications",
    "tw telecom", "zayo", "cogent", "level 3", "windstream",
    "tier point", "tierpoint", "flexential", "landmark", "flexential",
}


def classify_operator_type(company: str) -> Optional[str]:
    """Classify operator type from company name string."""
    if not company:
        return None
    lower = company.lower()
    # Check hyperscaler first — more specific
    for token in _HYPERSCALER_TOKENS:
        if token in lower:
            return "hyperscaler"
    for token in _COLO_TOKENS:
        if token in lower:
            return "colocation"
    # Default: assume enterprise-owned if company name is specific
    # (non-empty, doesn't look like a generic name)
    if len(company.strip()) > 2:
        return "enterprise"
    return None


# ---------------------------------------------------------------------------
# OSM Overpass — fetch global data center nodes/ways
# ---------------------------------------------------------------------------

# Tag-selective query: only fetch DCs that already carry at least one enrichment tag.
# This keeps the result set to ~hundreds rather than tens-of-thousands, avoiding
# gateway timeouts from heavy global scans.
_OVERPASS_QUERY = """
[out:json][timeout:120];
(
  node["datacenter:tier"];
  way["datacenter:tier"];
  node["cooling"]["amenity"="data_center"];
  way["cooling"]["amenity"="data_center"];
  node["start_date"]["amenity"="data_center"];
  way["start_date"]["amenity"="data_center"];
  node["generator:output:electricity"]["amenity"="data_center"];
  way["generator:output:electricity"]["amenity"="data_center"];
  node["power:output"]["amenity"="data_center"];
  way["power:output"]["amenity"="data_center"];
);
out center tags;
"""
# Public Overpass mirrors tried in order
_OVERPASS_MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
]


def _parse_tier(raw: str) -> Optional[int]:
    """Parse Uptime Institute tier from OSM tag value."""
    if not raw:
        return None
    upper = raw.upper()
    # Try digit first: "3", "Tier 3", "tier-4", "TIER1"
    m = re.search(r'(?<!\d)([1-4])(?!\d)', upper)
    if m:
        return int(m.group(1))
    # Then Roman numeral as a whole word: "III", "Tier III", "tier-III"
    roman = {"IV": 4, "III": 3, "II": 2, "I": 1}
    for numeral, value in roman.items():
        if re.search(r'\b' + numeral + r'\b', upper):
            return value
    return None


def _parse_year(raw: str) -> Optional[int]:
    """Parse year from OSM start_date tag (handles 'YYYY', 'YYYY-MM', 'YYYY-MM-DD')."""
    if not raw:
        return None
    m = re.match(r'^(\d{4})', raw.strip())
    if m:
        y = int(m.group(1))
        if 1950 <= y <= 2030:
            return y
    return None


def _parse_cooling(raw: str) -> Optional[str]:
    """Normalise OSM cooling tag to schema literals."""
    if not raw:
        return None
    lower = raw.lower()
    if any(t in lower for t in ("liquid", "water", "immersion", "direct")):
        return "liquid"
    if any(t in lower for t in ("hybrid", "mixed", "combination")):
        return "hybrid"
    if any(t in lower for t in ("air", "free", "adiabatic", "evaporat")):
        return "air"
    return None


def _parse_floor_level(tags: dict) -> Optional[str]:
    """Infer above/basement from OSM location/level tags."""
    location = tags.get("location", "").lower()
    if "underground" in location or "basement" in location:
        return "basement"
    level = tags.get("level", tags.get("building:levels", "")).lower()
    if "underground" in level or level.startswith("-"):
        return "basement"
    # Explicit above-ground marker
    if location in ("surface", "overground", "ground"):
        return "above"
    return None


def _parse_mw_capacity(tags: dict) -> Optional[float]:
    """
    Extract IT load (MW) from OSM power/generator tags.
    OSM uses values like "10 MW", "10000 kW", "5 GW".
    """
    for key in ("generator:output:electricity", "power:output", "capacity:electrical"):
        raw = tags.get(key, "")
        if not raw:
            continue
        m = re.search(r'([\d.]+)\s*(GW|MW|kW|W)', raw, re.IGNORECASE)
        if m:
            val = float(m.group(1))
            unit = m.group(2).upper()
            if unit == "GW":
                return round(val * 1000, 1)
            if unit == "MW":
                return round(val, 1)
            if unit == "KW":
                return round(val / 1000, 3)
            if unit == "W":
                return round(val / 1_000_000, 6)
    return None


def fetch_osm_datacenters(timeout: int = 140) -> list[dict]:
    """
    Query Overpass for data center nodes/ways that carry enrichment tags.
    Uses tag-selective query (not global DC scan) to stay well under timeout.
    Falls back through multiple public mirrors.
    Returns list of dicts with lat, lng, and parsed L1 fields.
    """
    # NOTE: OSM coverage for datacenter:tier, cooling, start_date is currently sparse —
    # the query may legitimately return 0 elements. Coverage improves as OSM contributors
    # tag facilities. The pipeline is wired up correctly for future enrichment.
    logger.info("Querying Overpass API (tag-selective query across mirrors)...")
    elements = None  # None = request failed; [] = succeeded with no results
    for mirror in _OVERPASS_MIRRORS:
        try:
            resp = httpx.post(
                mirror,
                data={"data": _OVERPASS_QUERY},
                timeout=timeout,
                headers={"User-Agent": "ShadowBroker-enrichment/1.0"},
            )
            resp.raise_for_status()
            elements = resp.json().get("elements", [])
            logger.info(f"  {mirror} → {len(elements)} elements (0 = no matching OSM tags yet)")
            break
        except Exception as e:
            logger.warning(f"  {mirror} failed: {e}")
            time.sleep(2)

    if elements is None:
        logger.error("All Overpass mirrors failed — skipping OSM enrichment")
        return []
    if not elements:
        logger.info("  No OSM elements with enrichment tags — skipping spatial match")
        return []

    results = []
    for el in elements:
        if el["type"] == "way":
            center = el.get("center", {})
            lat = center.get("lat")
            lng = center.get("lon")
        else:
            lat = el.get("lat")
            lng = el.get("lon")
        if lat is None or lng is None:
            continue

        tags = el.get("tags", {})
        tier_raw = tags.get("datacenter:tier", tags.get("tier", ""))
        results.append({
            "lat": lat,
            "lng": lng,
            "osm_tier": _parse_tier(tier_raw),
            "osm_year_built": _parse_year(tags.get("start_date", "")),
            "osm_cooling_type": _parse_cooling(
                tags.get("cooling", tags.get("cooling_system", ""))
            ),
            "osm_floor_level": _parse_floor_level(tags),
            "osm_mw_capacity": _parse_mw_capacity(tags),
        })

    logger.info(f"Parsed {len(results)} OSM locations with coordinates")
    return results


# ---------------------------------------------------------------------------
# Spatial merge helpers
# ---------------------------------------------------------------------------

def _to_radians_array(records: list[dict]) -> np.ndarray:
    return np.radians([[r["lat"], r["lng"]] for r in records])


def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlng / 2) ** 2)
    return 2 * _EARTH_RADIUS_KM * math.asin(math.sqrt(a))


# ---------------------------------------------------------------------------
# Uptime Institute — authoritative Tier certifications
# ---------------------------------------------------------------------------

_UPTIME_ENDPOINT = (
    "https://uptimeinstitute.com"
    "/index.php?option=com_tierachievement&task=map.retrieveMappingInformation"
)


def _tier_from_label(label: str) -> Optional[int]:
    """Extract tier integer from Uptime Institute typeLabel / name strings."""
    m = re.search(r'Tier\s+(IV|III|II|I|[1-4])\b', label, re.I)
    if not m:
        return None
    token = m.group(1).upper()
    roman = {"IV": 4, "III": 3, "II": 2, "I": 1}
    return roman.get(token) or (int(token) if token.isdigit() else None)


def fetch_uptime_tier_certifications(timeout: int = 30) -> list[dict]:
    """
    Fetch all Tier-certified facilities from Uptime Institute's public map API.
    Returns list of dicts: lat, lng, tier_rating (int), dc_name, client_name.
    Only returns records with type=='Tier' and a parseable tier level.
    """
    logger.info("Fetching Uptime Institute Tier certifications...")
    try:
        resp = httpx.get(
            _UPTIME_ENDPOINT,
            timeout=timeout,
            headers={"User-Agent": "ShadowBroker-enrichment/1.0"},
            follow_redirects=True,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning(f"Uptime Institute fetch failed: {e}")
        return []

    results = []
    # 'dupes' are facilities that share a lat/lng with others — same structure, still useful
    all_entries = data.get("datacenters", []) + data.get("dupes", [])
    for entry in all_entries:
        try:
            lat = float(entry["lat"])
            lng = float(entry["lng"])
        except (KeyError, ValueError, TypeError):
            continue
        if not (-90 <= lat <= 90 and -180 <= lng <= 180):
            continue

        # Find highest Tier cert for this facility (a facility can hold multiple)
        best_tier: Optional[int] = None
        for cert in entry.get("certs", []):
            if cert.get("type") != "Tier":
                continue
            tier = _tier_from_label(cert.get("typeLabel", "") + " " + cert.get("name", ""))
            if tier and (best_tier is None or tier > best_tier):
                best_tier = tier

        if best_tier is None:
            continue

        results.append({
            "lat": lat,
            "lng": lng,
            "tier_rating": best_tier,
            "dc_name": entry.get("datacenterName", ""),
            "client_name": entry.get("clientName", ""),
        })

    logger.info(f"Uptime Institute: {len(results)} Tier-certified facilities")
    return results


def spatial_match(
    target: dict,
    tree: cKDTree,
    source_records: list[dict],
    max_dist_km: float,
) -> Optional[dict]:
    """Return nearest source record within max_dist_km of target, or None."""
    pt = np.radians([target["lat"], target["lng"]])
    dist_rad, idx = tree.query(pt, k=1)
    matched = source_records[int(idx)]
    dist_km = _haversine_km(target["lat"], target["lng"], matched["lat"], matched["lng"])
    if dist_km <= max_dist_km:
        return matched
    return None


# ---------------------------------------------------------------------------
# Main enrichment
# ---------------------------------------------------------------------------

def enrich(dc_path: Path = DC_PATH, force: bool = False) -> list[dict]:
    logger.info(f"Loading {dc_path}...")
    dcs = json.loads(dc_path.read_text(encoding="utf-8"))
    logger.info(f"Loaded {len(dcs)} data center records")

    valid_dcs = [d for d in dcs if d.get("lat") is not None and d.get("lng") is not None]

    # ── Step 1: operator_type from company name (no network needed) ─────────
    logger.info("Step 1: classifying operator_type from company names...")
    op_updated = 0
    for dc in valid_dcs:
        if dc.get("operator_type") and not force:
            continue
        ot = classify_operator_type(dc.get("company", "") or dc.get("name", ""))
        if ot:
            dc["operator_type"] = ot
            op_updated += 1
    logger.info(f"  operator_type set/updated: {op_updated}")

    # ── Step 2: OSM — tier_rating, year_built, cooling_type, floor_level, mw_capacity ──
    logger.info("Step 2: fetching OSM data centers for physical attributes...")
    osm_records = fetch_osm_datacenters()

    osm_updated = {
        "tier_rating": 0, "year_built": 0,
        "cooling_type": 0, "floor_level": 0, "mw_capacity": 0,
    }
    if osm_records:
        osm_tree = cKDTree(_to_radians_array(osm_records))
        for dc in valid_dcs:
            match = spatial_match(dc, osm_tree, osm_records, max_dist_km=0.5)
            if not match:
                continue
            for schema_field, osm_field in (
                ("tier_rating",  "osm_tier"),
                ("year_built",   "osm_year_built"),
                ("cooling_type", "osm_cooling_type"),
                ("floor_level",  "osm_floor_level"),
                ("mw_capacity",  "osm_mw_capacity"),
            ):
                if (dc.get(schema_field) is None or force) and match.get(osm_field) is not None:
                    dc[schema_field] = match[osm_field]
                    osm_updated[schema_field] += 1
        logger.info(f"  OSM fields updated: {osm_updated}")

    # ── Step 3: Uptime Institute — authoritative tier_rating ─────────────────
    logger.info("Step 3: fetching Uptime Institute Tier certifications...")
    uptime_records = fetch_uptime_tier_certifications()

    tier_updated = 0
    if uptime_records:
        uptime_tree = cKDTree(_to_radians_array(uptime_records))
        for dc in valid_dcs:
            if dc.get("tier_rating") is not None and not force:
                continue
            match = spatial_match(dc, uptime_tree, uptime_records, max_dist_km=1.0)
            if match:
                dc["tier_rating"] = match["tier_rating"]
                tier_updated += 1
        logger.info(f"  tier_rating set/updated from Uptime Institute: {tier_updated}")
    else:
        logger.warning("  No Uptime Institute records — skipping tier enrichment")

    # ── Coverage summary ─────────────────────────────────────────────────────
    total = len(valid_dcs)
    for field in ("operator_type", "tier_rating", "mw_capacity", "year_built",
                  "cooling_type", "floor_level"):
        filled = sum(1 for d in valid_dcs if d.get(field) is not None)
        pct = filled / total * 100 if total else 0
        logger.info(f"  {field}: {filled}/{total} ({pct:.1f}% coverage)")

    # ── Atomic write ─────────────────────────────────────────────────────────
    logger.info(f"Writing enriched data to {dc_path}...")
    tmp_path = dc_path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(dcs, ensure_ascii=False, indent=None), encoding="utf-8")
    os.replace(tmp_path, dc_path)
    logger.info("Done.")
    return dcs, {
        field: round(sum(1 for d in valid_dcs if d.get(field) is not None) / total * 100, 1)
        for field in ("operator_type", "tier_rating", "mw_capacity",
                      "year_built", "cooling_type", "floor_level")
    }


if __name__ == "__main__":
    import sys
    import time as _time
    parser = argparse.ArgumentParser(description="Layer 1 physical asset enrichment")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing non-null L1 fields")
    args = parser.parse_args()

    try:
        from services.sync_meta import write_result, set_running
        set_running("enrich_l1_physical")
    except Exception:
        write_result = set_running = None  # type: ignore

    t0 = _time.time()
    log_lines: list[str] = []
    _orig_info = logger.info

    def _tee(msg: str, *a, **kw):
        log_lines.append(msg)
        _orig_info(msg, *a, **kw)
    logger.info = _tee  # type: ignore

    try:
        _, coverage = enrich(force=args.force)
        if write_result:
            write_result(
                "enrich_l1_physical",
                status="success",
                duration_s=_time.time() - t0,
                coverage=coverage,
                log_tail="\n".join(log_lines[-40:]),
            )
    except Exception as exc:
        if write_result:
            write_result(
                "enrich_l1_physical",
                status="error",
                duration_s=_time.time() - t0,
                log_tail="\n".join(log_lines[-40:]),
                error=str(exc),
            )
        raise
