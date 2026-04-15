"""Unit tests for enrich_datacenters — pure functions only, no network."""
import math
import pytest
from scripts.enrich_datacenters import (
    _haversine_km,
    composite_risk_score,
    concentration_score,
    grid_score_from_plant,
    nat_cat_score,
    build_power_tree,
    nearest_plant,
    build_dc_density_tree,
    count_dcs_within_50km,
    _UNRELIABLE_FUELS,
)


class TestHaversine:
    def test_same_point_is_zero(self):
        assert _haversine_km(51.5, -0.1, 51.5, -0.1) == 0.0

    def test_london_to_paris_approx(self):
        dist = _haversine_km(51.5, -0.1, 48.85, 2.35)
        assert 330 < dist < 360


class TestNatCatScore:
    def test_returns_max_of_four(self):
        scores = {"hazard_eq": 75, "hazard_flood": 50, "hazard_cyclone": 25, "hazard_fire": 5}
        assert nat_cat_score(scores) == 75

    def test_all_zero(self):
        scores = {"hazard_eq": 0, "hazard_flood": 0, "hazard_cyclone": 0, "hazard_fire": 0}
        assert nat_cat_score(scores) == 0


class TestGridScore:
    def test_close_stable_plant_is_low_risk(self):
        assert grid_score_from_plant(5.0, "Gas") < 10

    def test_far_unreliable_plant_is_high_risk(self):
        score = grid_score_from_plant(250.0, "Solar")
        assert score == 100

    def test_distance_capped_at_60(self):
        score_200 = grid_score_from_plant(200.0, "Gas")
        score_500 = grid_score_from_plant(500.0, "Gas")
        assert score_200 == score_500 == 60

    def test_unreliable_fuels_add_40(self):
        for fuel in _UNRELIABLE_FUELS:
            score = grid_score_from_plant(0.0, fuel)
            assert score == 40

    def test_stable_fuel_adds_zero(self):
        assert grid_score_from_plant(0.0, "Nuclear") == 0


class TestConcentrationScore:
    def test_zero_neighbours_is_zero(self):
        assert concentration_score(0) == 0

    def test_50_neighbours_is_100(self):
        assert concentration_score(50) == 100

    def test_capped_at_100(self):
        assert concentration_score(200) == 100

    def test_25_neighbours_is_50(self):
        assert concentration_score(25) == 50


class TestCompositeRisk:
    def test_max_all_is_100(self):
        assert composite_risk_score(100, 100, 100) == 100.0

    def test_zero_all_is_zero(self):
        assert composite_risk_score(0, 0, 0) == 0.0

    def test_weighted_formula(self):
        # 0.5*60 + 0.3*40 + 0.2*20 = 30 + 12 + 4 = 46.0
        assert composite_risk_score(60, 40, 20) == 46.0


class TestKDTreeNearest:
    def test_nearest_plant_returns_closest(self):
        plants = [
            {"lat": 51.5, "lng": -0.1, "fuel_type": "Gas"},
            {"lat": 48.85, "lng": 2.35, "fuel_type": "Nuclear"},
        ]
        tree, plant_list = build_power_tree(plants)
        dist, fuel = nearest_plant(51.6, -0.2, tree, plant_list)
        assert fuel == "Gas"
        assert dist < 20

    def test_nearest_plant_distance_is_km(self):
        plants = [{"lat": 0.0, "lng": 0.0, "fuel_type": "Coal"}]
        tree, plant_list = build_power_tree(plants)
        dist, _ = nearest_plant(0.0, 1.0, tree, plant_list)
        assert 100 < dist < 120


class TestDCDensity:
    def test_isolated_dc_has_zero_neighbours(self):
        dcs = [
            {"lat": 0.0, "lng": 0.0},
            {"lat": 10.0, "lng": 10.0},
        ]
        tree = build_dc_density_tree(dcs)
        count = count_dcs_within_50km(0.0, 0.0, tree)
        assert count == 0

    def test_clustered_dcs_count_correctly(self):
        dcs = [{"lat": 0.0 + i * 0.001, "lng": 0.0} for i in range(5)]
        tree = build_dc_density_tree(dcs)
        count = count_dcs_within_50km(0.0, 0.0, tree)
        assert count == 4
