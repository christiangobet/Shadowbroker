"""Tests for the staged data-center risk enrichment pipeline."""

from services.dc_risk import build_dc_flood_zone_features
from scripts.enrich_l3_dependencies import annotate_dependency_fields
from scripts.enrich_l4_topology import annotate_topology_scores
from scripts.enrich_l5_risk import annotate_composite_risk


def test_l3_assigns_shared_substations_ixps_and_asn():
    datacenters = [
        {"name": "DC A", "lat": 40.0, "lng": -75.0},
        {"name": "DC B", "lat": 40.02, "lng": -75.01},
        {"name": "DC C", "lat": 34.05, "lng": -118.25},
    ]
    substations = [
        {"id": "sub-east", "lat": 40.01, "lng": -75.0},
        {"id": "sub-west", "lat": 34.04, "lng": -118.24},
    ]
    ixps = [
        {"id": 1001, "name": "IX East", "lat": 40.015, "lng": -75.005, "asn": "AS64500"},
        {"id": 2002, "name": "IX West", "lat": 34.06, "lng": -118.245, "asn": "AS64510"},
    ]

    enriched = annotate_dependency_fields(datacenters, substations=substations, ixps=ixps)

    assert enriched[0]["substation_osm_id"] == "sub-east"
    assert enriched[1]["substation_osm_id"] == "sub-east"
    assert enriched[0]["substation_shared_count"] == 2
    assert enriched[1]["substation_shared_count"] == 2
    assert enriched[0]["ixp_count_50km"] == 1
    assert enriched[0]["ixp_ids"] == ["1001"]
    assert enriched[0]["asn"] == "AS64500"
    assert enriched[2]["substation_osm_id"] == "sub-west"
    assert enriched[2]["ixp_ids"] == ["2002"]


def test_l4_derives_accumulation_flag_and_systemic_score():
    datacenters = [
        {
            "name": "Cluster A1",
            "substation_cluster_id": "sub-east",
            "substation_shared_count": 3,
            "ixp_ids": ["1001", "1002"],
            "ixp_count_50km": 2,
            "asn": "AS64500",
        },
        {
            "name": "Cluster A2",
            "substation_cluster_id": "sub-east",
            "substation_shared_count": 3,
            "ixp_ids": ["1001"],
            "ixp_count_50km": 1,
            "asn": "AS64500",
        },
        {
            "name": "Cluster A3",
            "substation_cluster_id": "sub-east",
            "substation_shared_count": 3,
            "ixp_ids": ["1001"],
            "ixp_count_50km": 1,
            "asn": "AS64501",
        },
        {
            "name": "Isolated",
            "substation_cluster_id": "sub-west",
            "substation_shared_count": 1,
            "ixp_ids": [],
            "ixp_count_50km": 0,
            "asn": "AS64510",
        },
    ]

    enriched = annotate_topology_scores(datacenters)

    assert enriched[0]["accumulation_flag"] is True
    assert enriched[1]["accumulation_flag"] is True
    assert enriched[2]["accumulation_flag"] is True
    assert enriched[3]["accumulation_flag"] is False
    assert enriched[0]["systemic_importance_score"] > enriched[3]["systemic_importance_score"]
    assert enriched[0]["betweenness_centrality"] >= enriched[3]["betweenness_centrality"]


def test_l5_computes_weighted_composite_risk():
    datacenters = [
        {
            "name": "Weighted DC",
            "hazard_eq": 80,
            "hazard_flood": 60,
            "hazard_cyclone": 40,
            "hazard_fire": 20,
            "grid_score": 50,
            "ixp_count_50km": 2,
            "substation_shared_count": 3,
            "systemic_importance_score": 70,
        }
    ]

    enriched = annotate_composite_risk(datacenters)
    dc = enriched[0]

    assert dc["hazard_risk_score"] == 80
    assert dc["power_risk_score"] >= 50
    assert dc["network_risk_score"] > 0
    assert dc["systemic_risk_score"] == 70
    assert dc["risk_score"] > 0
    assert dc["risk_score"] <= 100


def test_flood_overlay_falls_back_to_legacy_hazard_score_when_raw_flood_inputs_are_missing():
    datacenters = [
        {
            "name": "Legacy Flood DC",
            "lat": 40.0,
            "lng": -75.0,
            "hazard_flood": 65,
            "risk_score": 72.0,
            "jrc_flood_100yr_m": None,
            "fema_flood_zone": None,
        }
    ]

    features = build_dc_flood_zone_features(datacenters)

    assert len(features) == 1
    assert features[0]["properties"]["hazard_flood"] == 65
