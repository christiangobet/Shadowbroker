"""Contract tests for infrastructure.fetch_datacenters()."""

import json

from services.fetchers import _store
from services.fetchers.infrastructure import fetch_datacenters


def test_fetch_datacenters_preserves_layer_2_to_5_fields(tmp_path, monkeypatch):
    sample_path = tmp_path / "datacenters_geocoded.json"
    sample_path.write_text(
        json.dumps(
            [
                {
                    "name": "DC Alpha",
                    "company": "Example Cloud",
                    "lat": 40.0,
                    "lng": -75.0,
                    "jrc_flood_100yr_m": 1.25,
                    "usgs_pga_10pct_50yr": 0.21,
                    "ibtracs_track_density": 3.4,
                    "wildfire_days_50km": 14.0,
                    "heat_extreme_days": 8.0,
                    "hazard_eq": 55,
                    "hazard_flood": 65,
                    "hazard_cyclone": 35,
                    "hazard_fire": 25,
                    "substation_osm_id": "sub-east",
                    "substation_dist_km": 1.4,
                    "substation_cluster_id": "sub-east",
                    "substation_shared_count": 3,
                    "ixp_ids": ["1001", "1002"],
                    "nearest_ixp_km": 8.2,
                    "ixp_count_50km": 2,
                    "fibre_path_count": 4,
                    "water_stress_idx": 2.5,
                    "asn": "AS64500",
                    "betweenness_centrality": 0.83,
                    "systemic_importance_score": 76.0,
                    "accumulation_flag": True,
                    "risk_score": 72.4,
                }
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("services.fetchers.infrastructure._DC_GEOCODED_PATH", sample_path)
    monkeypatch.setitem(_store.active_layers, "datacenters", True)
    monkeypatch.setitem(_store.active_layers, "hyperscalers", False)
    monkeypatch.setitem(_store.latest_data, "datacenters", [])

    fetch_datacenters()

    [dc] = _store.latest_data["datacenters"]
    assert dc["jrc_flood_100yr_m"] == 1.25
    assert dc["heat_extreme_days"] == 8.0
    assert dc["substation_osm_id"] == "sub-east"
    assert dc["substation_shared_count"] == 3
    assert dc["ixp_ids"] == ["1001", "1002"]
    assert dc["asn"] == "AS64500"
    assert dc["betweenness_centrality"] == 0.83
    assert dc["systemic_importance_score"] == 76.0
    assert dc["accumulation_flag"] is True
    assert dc["risk_score"] == 72.4
