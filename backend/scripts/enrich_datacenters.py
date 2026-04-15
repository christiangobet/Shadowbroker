#!/usr/bin/env python3
"""
Enrich datacenters_geocoded.json with:
  - ThinkHazard nat cat scores (earthquake, flood, cyclone, wildfire)
  - Nearest power plant distance + fuel type (WRI GPPD via KD-tree)
  - DC density within 50 km (KD-tree over all DCs)
  - Composite risk_score (0-100)

Usage:
    cd backend && ../backend/venv/bin/python3 -m scripts.enrich_datacenters

Reads and overwrites:  backend/data/datacenters_geocoded.json
Reads (read-only):     backend/data/power_plants.json
"""
from __future__ import annotations

import json
import logging
import math
from pathlib import Path

from scipy.spatial import cKDTree
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).parent.parent.parent
DC_PATH = _REPO_ROOT / "backend" / "data" / "datacenters_geocoded.json"
PP_PATH = _REPO_ROOT / "backend" / "data" / "power_plants.json"

COUNTRY_TO_ISO3: dict[str, str] = {
    "Afghanistan": "AFG", "Algeria": "DZA", "Angola": "AGO", "Argentina": "ARG",
    "Armenia": "ARM", "Australia": "AUS", "Austria": "AUT", "Azerbaijan": "AZE",
    "Bahrain": "BHR", "Bangladesh": "BGD", "Belarus": "BLR", "Belgium": "BEL",
    "Bolivia": "BOL", "Botswana": "BWA", "Brazil": "BRA", "Bulgaria": "BGR",
    "Burkina Faso": "BFA", "Cambodia": "KHM", "Cameroon": "CMR", "Canada": "CAN",
    "Chile": "CHL", "China": "CHN", "Colombia": "COL", "Costa Rica": "CRI",
    "Croatia": "HRV", "Cyprus": "CYP", "Czech Republic": "CZE", "Denmark": "DNK",
    "Dominican Republic": "DOM", "Ecuador": "ECU", "Egypt": "EGY",
    "El Salvador": "SLV", "Estonia": "EST", "Ethiopia": "ETH", "Finland": "FIN",
    "France": "FRA", "Georgia": "GEO", "Germany": "DEU", "Ghana": "GHA",
    "Greece": "GRC", "Guatemala": "GTM", "Honduras": "HND", "Hong Kong": "HKG",
    "Hungary": "HUN", "Iceland": "ISL", "India": "IND", "Indonesia": "IDN",
    "Iran": "IRN", "Iraq": "IRQ", "Ireland": "IRL", "Israel": "ISR",
    "Italy": "ITA", "Ivory Coast": "CIV", "Jamaica": "JAM", "Japan": "JPN",
    "Jordan": "JOR", "Kazakhstan": "KAZ", "Kenya": "KEN", "Kuwait": "KWT",
    "Latvia": "LVA", "Lebanon": "LBN", "Lithuania": "LTU", "Luxembourg": "LUX",
    "Malaysia": "MYS", "Malta": "MLT", "Mexico": "MEX", "Moldova": "MDA",
    "Morocco": "MAR", "Mozambique": "MOZ", "Myanmar": "MMR", "Netherlands": "NLD",
    "New Zealand": "NZL", "Nigeria": "NGA", "North Macedonia": "MKD",
    "Norway": "NOR", "Oman": "OMN", "Pakistan": "PAK", "Panama": "PAN",
    "Paraguay": "PRY", "Peru": "PER", "Philippines": "PHL", "Poland": "POL",
    "Portugal": "PRT", "Qatar": "QAT", "Romania": "ROU", "Russia": "RUS",
    "Saudi Arabia": "SAU", "Senegal": "SEN", "Serbia": "SRB", "Singapore": "SGP",
    "Slovakia": "SVK", "Slovenia": "SVN", "South Africa": "ZAF",
    "South Korea": "KOR", "Spain": "ESP", "Sri Lanka": "LKA", "Sweden": "SWE",
    "Switzerland": "CHE", "Taiwan": "TWN", "Tanzania": "TZA", "Thailand": "THA",
    "Trinidad and Tobago": "TTO", "Tunisia": "TUN", "Turkey": "TUR",
    "Uganda": "UGA", "Ukraine": "UKR", "United Arab Emirates": "ARE",
    "United Kingdom": "GBR", "United States": "USA", "Uruguay": "URY",
    "Uzbekistan": "UZB", "Venezuela": "VEN", "Vietnam": "VNM", "Zambia": "ZMB",
    "Zimbabwe": "ZWE",
}

_LEVEL_SCORE = {"HIG": 75, "MED": 50, "LOW": 25, "VLO": 5}
_HAZARD_FIELDS = {"EQ": "hazard_eq", "FL": "hazard_flood", "CY": "hazard_cyclone", "WF": "hazard_fire"}
_UNRELIABLE_FUELS = {"Solar", "Wind", "Wave and Tidal", "Geothermal"}
_EARTH_RADIUS_KM = 6371.0


def _deg_to_rad(degrees: float) -> float:
    return degrees * math.pi / 180.0


def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    dlat = _deg_to_rad(lat2 - lat1)
    dlng = _deg_to_rad(lng2 - lng1)
    a = math.sin(dlat / 2) ** 2 + math.cos(_deg_to_rad(lat1)) * math.cos(_deg_to_rad(lat2)) * math.sin(dlng / 2) ** 2
    return 2 * _EARTH_RADIUS_KM * math.asin(math.sqrt(a))


# ---------------------------------------------------------------------------
# Static nat cat hazard scores (country-level)
# Source: INFORM Risk Index 2023 (EU JRC, public domain) + Munich Re NatCat profiles
# Scale: 0=none/negligible, 25=LOW, 50=MED, 75=HIGH
# Perils: EQ=earthquake, FL=flood, CY=cyclone/hurricane, WF=wildfire
# Update annually by re-running with ThinkHazard API when available, or from:
#   https://drmkc.jrc.ec.europa.eu/inform-index/INFORM-Risk
# ---------------------------------------------------------------------------
_STATIC_HAZARD: dict[str, dict[str, int]] = {
    "Afghanistan":          {"hazard_eq": 75, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 25},
    "Algeria":              {"hazard_eq": 50, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 25},
    "Angola":               {"hazard_eq": 25, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 25},
    "Argentina":            {"hazard_eq": 50, "hazard_flood": 50, "hazard_cyclone": 25, "hazard_fire": 25},
    "Armenia":              {"hazard_eq": 75, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 25},
    "Australia":            {"hazard_eq": 25, "hazard_flood": 50, "hazard_cyclone": 50, "hazard_fire": 75},
    "Austria":              {"hazard_eq": 25, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 25},
    "Azerbaijan":           {"hazard_eq": 75, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 25},
    "Bahrain":              {"hazard_eq": 25, "hazard_flood":  5, "hazard_cyclone": 25, "hazard_fire":  0},
    "Bangladesh":           {"hazard_eq": 50, "hazard_flood": 75, "hazard_cyclone": 75, "hazard_fire":  5},
    "Belarus":              {"hazard_eq":  5, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 25},
    "Belgium":              {"hazard_eq":  5, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire":  5},
    "Bolivia":              {"hazard_eq": 50, "hazard_flood": 75, "hazard_cyclone":  0, "hazard_fire": 25},
    "Botswana":             {"hazard_eq":  5, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 25},
    "Brazil":               {"hazard_eq":  5, "hazard_flood": 75, "hazard_cyclone":  0, "hazard_fire": 50},
    "Bulgaria":             {"hazard_eq": 50, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 50},
    "Burkina Faso":         {"hazard_eq":  5, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 25},
    "Cambodia":             {"hazard_eq":  5, "hazard_flood": 75, "hazard_cyclone": 25, "hazard_fire": 25},
    "Cameroon":             {"hazard_eq": 25, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 25},
    "Canada":               {"hazard_eq": 25, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 50},
    "Chile":                {"hazard_eq": 75, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 50},
    "China":                {"hazard_eq": 75, "hazard_flood": 75, "hazard_cyclone": 50, "hazard_fire": 25},
    "Colombia":             {"hazard_eq": 75, "hazard_flood": 75, "hazard_cyclone": 25, "hazard_fire": 25},
    "Costa Rica":           {"hazard_eq": 75, "hazard_flood": 75, "hazard_cyclone": 50, "hazard_fire": 25},
    "Croatia":              {"hazard_eq": 50, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 25},
    "Cyprus":               {"hazard_eq": 50, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 25},
    "Czech Republic":       {"hazard_eq":  5, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire":  5},
    "Denmark":              {"hazard_eq":  5, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire":  5},
    "Dominican Republic":   {"hazard_eq": 75, "hazard_flood": 75, "hazard_cyclone": 75, "hazard_fire": 25},
    "Ecuador":              {"hazard_eq": 75, "hazard_flood": 75, "hazard_cyclone": 25, "hazard_fire": 25},
    "Egypt":                {"hazard_eq": 25, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire":  5},
    "El Salvador":          {"hazard_eq": 75, "hazard_flood": 75, "hazard_cyclone": 50, "hazard_fire": 25},
    "Estonia":              {"hazard_eq":  5, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire":  5},
    "Ethiopia":             {"hazard_eq": 50, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 25},
    "Finland":              {"hazard_eq":  5, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 25},
    "France":               {"hazard_eq": 25, "hazard_flood": 50, "hazard_cyclone": 25, "hazard_fire": 25},
    "Georgia":              {"hazard_eq": 75, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 25},
    "Germany":              {"hazard_eq":  5, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire":  5},
    "Ghana":                {"hazard_eq": 25, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 25},
    "Greece":               {"hazard_eq": 75, "hazard_flood": 25, "hazard_cyclone": 25, "hazard_fire": 75},
    "Guatemala":            {"hazard_eq": 75, "hazard_flood": 75, "hazard_cyclone": 50, "hazard_fire": 25},
    "Honduras":             {"hazard_eq": 50, "hazard_flood": 75, "hazard_cyclone": 75, "hazard_fire": 25},
    "Hong Kong":            {"hazard_eq": 25, "hazard_flood": 50, "hazard_cyclone": 75, "hazard_fire":  5},
    "Hungary":              {"hazard_eq": 25, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire":  5},
    "Iceland":              {"hazard_eq": 75, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire":  5},
    "India":                {"hazard_eq": 50, "hazard_flood": 75, "hazard_cyclone": 75, "hazard_fire": 25},
    "Indonesia":            {"hazard_eq": 75, "hazard_flood": 75, "hazard_cyclone": 25, "hazard_fire": 50},
    "Iran":                 {"hazard_eq": 75, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 25},
    "Iraq":                 {"hazard_eq": 50, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire":  5},
    "Ireland":              {"hazard_eq":  5, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire":  5},
    "Israel":               {"hazard_eq": 75, "hazard_flood":  5, "hazard_cyclone":  0, "hazard_fire": 25},
    "Italy":                {"hazard_eq": 75, "hazard_flood": 50, "hazard_cyclone": 25, "hazard_fire": 50},
    "Ivory Coast":          {"hazard_eq":  5, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 25},
    "Jamaica":              {"hazard_eq": 50, "hazard_flood": 75, "hazard_cyclone": 75, "hazard_fire": 25},
    "Japan":                {"hazard_eq": 75, "hazard_flood": 75, "hazard_cyclone": 75, "hazard_fire": 25},
    "Jordan":               {"hazard_eq": 50, "hazard_flood":  5, "hazard_cyclone":  0, "hazard_fire":  5},
    "Kazakhstan":           {"hazard_eq": 50, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 25},
    "Kenya":                {"hazard_eq": 25, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 25},
    "Kuwait":               {"hazard_eq":  5, "hazard_flood":  5, "hazard_cyclone": 25, "hazard_fire":  0},
    "Latvia":               {"hazard_eq":  5, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 25},
    "Lebanon":              {"hazard_eq": 75, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 25},
    "Lithuania":            {"hazard_eq":  5, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 25},
    "Luxembourg":           {"hazard_eq":  5, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire":  5},
    "Malaysia":             {"hazard_eq": 25, "hazard_flood": 75, "hazard_cyclone":  0, "hazard_fire": 25},
    "Malta":                {"hazard_eq": 25, "hazard_flood":  5, "hazard_cyclone":  0, "hazard_fire":  5},
    "Mexico":               {"hazard_eq": 75, "hazard_flood": 75, "hazard_cyclone": 75, "hazard_fire": 50},
    "Moldova":              {"hazard_eq": 25, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 25},
    "Morocco":              {"hazard_eq": 50, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 25},
    "Mozambique":           {"hazard_eq": 25, "hazard_flood": 75, "hazard_cyclone": 75, "hazard_fire": 50},
    "Myanmar":              {"hazard_eq": 50, "hazard_flood": 75, "hazard_cyclone": 75, "hazard_fire": 25},
    "Netherlands":          {"hazard_eq":  5, "hazard_flood": 75, "hazard_cyclone":  0, "hazard_fire":  5},
    "New Zealand":          {"hazard_eq": 75, "hazard_flood": 50, "hazard_cyclone": 25, "hazard_fire": 50},
    "Nigeria":              {"hazard_eq":  5, "hazard_flood": 75, "hazard_cyclone":  0, "hazard_fire": 25},
    "North Macedonia":      {"hazard_eq": 75, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 25},
    "Norway":               {"hazard_eq":  5, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 25},
    "Oman":                 {"hazard_eq": 25, "hazard_flood": 25, "hazard_cyclone": 50, "hazard_fire":  5},
    "Pakistan":             {"hazard_eq": 75, "hazard_flood": 75, "hazard_cyclone": 25, "hazard_fire": 25},
    "Panama":               {"hazard_eq": 50, "hazard_flood": 75, "hazard_cyclone": 25, "hazard_fire": 25},
    "Paraguay":             {"hazard_eq":  5, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 25},
    "Peru":                 {"hazard_eq": 75, "hazard_flood": 75, "hazard_cyclone":  0, "hazard_fire": 25},
    "Philippines":          {"hazard_eq": 75, "hazard_flood": 75, "hazard_cyclone": 75, "hazard_fire": 25},
    "Poland":               {"hazard_eq":  5, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 25},
    "Portugal":             {"hazard_eq": 50, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 75},
    "Qatar":                {"hazard_eq":  5, "hazard_flood":  5, "hazard_cyclone": 25, "hazard_fire":  0},
    "Romania":              {"hazard_eq": 75, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 25},
    "Russia":               {"hazard_eq": 50, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 50},
    "Saudi Arabia":         {"hazard_eq": 25, "hazard_flood": 25, "hazard_cyclone": 25, "hazard_fire":  5},
    "Senegal":              {"hazard_eq":  5, "hazard_flood": 50, "hazard_cyclone": 25, "hazard_fire": 25},
    "Serbia":               {"hazard_eq": 50, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 25},
    "Singapore":            {"hazard_eq":  5, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire":  5},
    "Slovakia":             {"hazard_eq": 25, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire":  5},
    "Slovenia":             {"hazard_eq": 50, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 25},
    "South Africa":         {"hazard_eq":  5, "hazard_flood": 25, "hazard_cyclone": 25, "hazard_fire": 25},
    "South Korea":          {"hazard_eq": 25, "hazard_flood": 50, "hazard_cyclone": 50, "hazard_fire": 25},
    "Spain":                {"hazard_eq": 25, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 75},
    "Sri Lanka":            {"hazard_eq": 25, "hazard_flood": 75, "hazard_cyclone": 50, "hazard_fire": 25},
    "Sweden":               {"hazard_eq":  5, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 25},
    "Switzerland":          {"hazard_eq": 25, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 25},
    "Taiwan":               {"hazard_eq": 75, "hazard_flood": 75, "hazard_cyclone": 75, "hazard_fire": 25},
    "Tanzania":             {"hazard_eq": 25, "hazard_flood": 50, "hazard_cyclone": 25, "hazard_fire": 25},
    "Thailand":             {"hazard_eq": 25, "hazard_flood": 75, "hazard_cyclone": 50, "hazard_fire": 25},
    "Trinidad and Tobago":  {"hazard_eq": 50, "hazard_flood": 50, "hazard_cyclone": 50, "hazard_fire": 25},
    "Tunisia":              {"hazard_eq": 25, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 25},
    "Turkey":               {"hazard_eq": 75, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 50},
    "Uganda":               {"hazard_eq": 50, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 25},
    "Ukraine":              {"hazard_eq": 25, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 25},
    "United Arab Emirates": {"hazard_eq":  5, "hazard_flood":  5, "hazard_cyclone": 25, "hazard_fire":  0},
    "United Kingdom":       {"hazard_eq":  5, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire":  5},
    "United States":        {"hazard_eq": 50, "hazard_flood": 50, "hazard_cyclone": 50, "hazard_fire": 50},
    "Uruguay":              {"hazard_eq":  5, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 25},
    "Uzbekistan":           {"hazard_eq": 75, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 25},
    "Venezuela":            {"hazard_eq": 50, "hazard_flood": 75, "hazard_cyclone": 25, "hazard_fire": 25},
    "Vietnam":              {"hazard_eq": 25, "hazard_flood": 75, "hazard_cyclone": 75, "hazard_fire": 25},
    "Zambia":               {"hazard_eq":  5, "hazard_flood": 50, "hazard_cyclone":  0, "hazard_fire": 25},
    "Zimbabwe":             {"hazard_eq":  5, "hazard_flood": 25, "hazard_cyclone":  0, "hazard_fire": 25},
}

_ZERO_HAZARD = {"hazard_eq": 0, "hazard_flood": 0, "hazard_cyclone": 0, "hazard_fire": 0}


def get_hazard_scores(country_name: str) -> dict[str, int]:
    """Return static nat cat hazard scores for a country (INFORM Risk 2023 basis)."""
    return _STATIC_HAZARD.get(country_name, _ZERO_HAZARD)


def build_power_tree(power_plants: list[dict]) -> tuple[cKDTree, list[dict]]:
    coords = np.radians([[p["lat"], p["lng"]] for p in power_plants])
    tree = cKDTree(coords)
    return tree, power_plants


def nearest_plant(lat: float, lng: float, tree: cKDTree, plants: list[dict]) -> tuple[float, str]:
    pt = np.radians([[lat, lng]])
    _dist_rad, idx = tree.query(pt, k=1)
    nearest = plants[int(idx[0])]
    dist_km = _haversine_km(lat, lng, nearest["lat"], nearest["lng"])
    return round(dist_km, 1), nearest["fuel_type"]


def grid_score_from_plant(dist_km: float, fuel_type: str) -> int:
    dist_component = min(60, int(dist_km / 200 * 60))
    fuel_component = 40 if fuel_type in _UNRELIABLE_FUELS else 0
    return dist_component + fuel_component


def build_dc_density_tree(dcs: list[dict]) -> cKDTree:
    coords = np.radians([[d["lat"], d["lng"]] for d in dcs])
    return cKDTree(coords)


def count_dcs_within_50km(lat: float, lng: float, tree: cKDTree) -> int:
    radius_rad = 50.0 / _EARTH_RADIUS_KM
    pt = np.radians([lat, lng])
    indices = tree.query_ball_point(pt, radius_rad)
    return max(0, len(indices) - 1)


def concentration_score(dc_count: int) -> int:
    return min(100, int(dc_count / 50 * 100))


def nat_cat_score(hazard_scores: dict[str, int]) -> int:
    return max(hazard_scores.values(), default=0)


def composite_risk_score(nat_cat: int, grid: int, concentration: int) -> float:
    return round(0.5 * nat_cat + 0.3 * grid + 0.2 * concentration, 1)


def enrich(dc_path: Path = DC_PATH, pp_path: Path = PP_PATH) -> list[dict]:
    logger.info("Loading data...")
    dcs = json.loads(dc_path.read_text(encoding="utf-8"))
    plants = json.loads(pp_path.read_text(encoding="utf-8"))

    valid_dcs = [d for d in dcs if d.get("lat") is not None and d.get("lng") is not None]
    logger.info(f"DCs with coordinates: {len(valid_dcs)}")

    unique_countries = set(d.get("country", "") for d in valid_dcs)
    unmapped = [c for c in unique_countries if c and c not in _STATIC_HAZARD]
    if unmapped:
        logger.warning(f"No static hazard data for: {unmapped} (will use zeros)")
    logger.info(f"Nat cat scores: static lookup ({len(_STATIC_HAZARD)} countries, INFORM 2023)")

    logger.info(f"Building power plant KD-tree ({len(plants)} plants)...")
    pp_tree, pp_list = build_power_tree(plants)

    logger.info("Building DC density KD-tree...")
    dc_tree = build_dc_density_tree(valid_dcs)

    logger.info("Enriching data centers...")
    for i, dc in enumerate(valid_dcs):
        country = dc.get("country", "")
        hazards = get_hazard_scores(country)

        dist_km, fuel = nearest_plant(dc["lat"], dc["lng"], pp_tree, pp_list)
        grid = grid_score_from_plant(dist_km, fuel)
        density = count_dcs_within_50km(dc["lat"], dc["lng"], dc_tree)
        conc = concentration_score(density)
        nat = nat_cat_score(hazards)
        risk = composite_risk_score(nat, grid, conc)

        dc.update({
            **hazards,
            "nearest_plant_km": dist_km,
            "nearest_plant_fuel": fuel,
            "grid_score": grid,
            "dc_density_50km": density,
            "concentration_score": conc,
            "nat_cat_score": nat,
            "risk_score": risk,
        })
        if i % 500 == 499:
            logger.info(f"Enriched {i+1}/{len(valid_dcs)} DCs")

    _DEFAULT_RISK = {
        "hazard_eq": 0, "hazard_flood": 0, "hazard_cyclone": 0, "hazard_fire": 0,
        "nearest_plant_km": None, "nearest_plant_fuel": "",
        "grid_score": 0, "dc_density_50km": 0, "concentration_score": 0,
        "nat_cat_score": 0, "risk_score": 0.0,
    }
    for dc in dcs:
        if "risk_score" not in dc:
            dc.update(_DEFAULT_RISK)

    logger.info(f"Writing enriched data to {dc_path}...")
    import os
    tmp_path = dc_path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(dcs, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp_path, dc_path)
    logger.info("Done.")
    return dcs


if __name__ == "__main__":
    enrich()
