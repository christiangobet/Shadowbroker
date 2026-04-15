"""Smoke tests for all API endpoints — verifies routes exist and return valid responses."""

import asyncio

import pytest


class TestHealthEndpoint:
    def test_health_returns_200(self, client):
        r = client.get("/api/health")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ok"
        assert "sources" in data
        assert "freshness" in data

    def test_health_has_uptime(self, client):
        r = client.get("/api/health")
        data = r.json()
        assert "uptime_seconds" in data
        assert isinstance(data["uptime_seconds"], (int, float))


class TestLiveDataEndpoints:
    def test_live_data_returns_200(self, client):
        r = client.get("/api/live-data")
        assert r.status_code == 200

    def test_live_data_fast_returns_200_or_304(self, client):
        r = client.get("/api/live-data/fast")
        assert r.status_code in (200, 304)
        if r.status_code == 200:
            data = r.json()
            assert "freshness" in data

    def test_live_data_slow_returns_200_or_304(self, client):
        r = client.get("/api/live-data/slow")
        assert r.status_code in (200, 304)
        if r.status_code == 200:
            data = r.json()
            assert "freshness" in data

    def test_fast_has_expected_keys(self, client):
        r = client.get("/api/live-data/fast")
        if r.status_code == 200:
            data = r.json()
            for key in ("commercial_flights", "military_flights", "ships", "satellites"):
                assert key in data, f"Missing key: {key}"

    def test_slow_has_expected_keys(self, client):
        r = client.get("/api/live-data/slow")
        if r.status_code == 200:
            data = r.json()
            for key in ("news", "stocks", "weather", "earthquakes"):
                assert key in data, f"Missing key: {key}"

    def test_slow_includes_dc_risk_overlays_and_full_datacenter_contract(self, client, monkeypatch):
        from services.fetchers import _store

        datacenter = {
            "name": "DC Overlay Test",
            "company": "Example Cloud",
            "lat": 40.0,
            "lng": -75.0,
            "hazard_eq": 50,
            "hazard_flood": 60,
            "hazard_cyclone": 20,
            "hazard_fire": 10,
            "jrc_flood_100yr_m": 1.1,
            "fema_flood_zone": "AE",
            "substation_osm_id": "sub-east",
            "substation_shared_count": 3,
            "ixp_ids": ["1001"],
            "ixp_count_50km": 1,
            "asn": "AS64500",
            "betweenness_centrality": 0.75,
            "systemic_importance_score": 82.0,
            "accumulation_flag": True,
            "risk_score": 71.0,
        }
        overlays = {
            "dc_flood_zones": [{"type": "Feature", "properties": {"zone": "AE"}, "geometry": {"type": "Polygon", "coordinates": []}}],
            "dc_power_dependencies": [{"type": "Feature", "properties": {"substation_osm_id": "sub-east"}, "geometry": {"type": "LineString", "coordinates": [[-75.0, 40.0], [-75.01, 40.01]]}}],
            "dc_network_dependencies": [{"type": "Feature", "properties": {"ixp_id": "1001"}, "geometry": {"type": "LineString", "coordinates": [[-75.0, 40.0], [-75.02, 40.02]]}}],
            "dc_accumulation_clusters": [{"type": "Feature", "properties": {"cluster_id": "sub-east"}, "geometry": {"type": "Point", "coordinates": [-75.0, 40.0]}}],
            "dc_cyclone_tracks": [{"type": "Feature", "properties": {"storm_id": "AL01"}, "geometry": {"type": "LineString", "coordinates": [[-70.0, 30.0], [-71.0, 31.0]]}}],
        }

        monkeypatch.setitem(_store.latest_data, "datacenters", [datacenter])
        for key, value in overlays.items():
            monkeypatch.setitem(_store.latest_data, key, value)
        monkeypatch.setitem(_store.active_layers, "datacenters", True)
        monkeypatch.setitem(_store.active_layers, "dc_flood", True)
        monkeypatch.setitem(_store.active_layers, "dc_power_dependencies", True)
        monkeypatch.setitem(_store.active_layers, "dc_network_dependencies", True)
        monkeypatch.setitem(_store.active_layers, "dc_accumulation", True)
        monkeypatch.setitem(_store.active_layers, "dc_cyclone_history", True)

        r = client.get("/api/live-data/slow")

        assert r.status_code == 200
        data = r.json()
        assert data["datacenters"][0]["substation_osm_id"] == "sub-east"
        assert data["datacenters"][0]["ixp_ids"] == ["1001"]
        assert data["datacenters"][0]["systemic_importance_score"] == 82.0
        assert data["datacenters"][0]["accumulation_flag"] is True
        for key in overlays:
            assert key in data
            assert data[key] == overlays[key]

    def test_datacenter_risk_summary_endpoint_returns_aggregate_metrics(self, client, monkeypatch):
        from services.fetchers import _store

        monkeypatch.setitem(
            _store.latest_data,
            "datacenters",
            [
                {"name": "A", "risk_score": 80, "accumulation_flag": True, "fema_flood_zone": "AE"},
                {"name": "B", "risk_score": 55, "accumulation_flag": False, "fema_flood_zone": None},
                {"name": "C", "risk_score": 20, "accumulation_flag": True, "fema_flood_zone": "X"},
            ],
        )

        r = client.get("/api/risk-summary/datacenters")

        assert r.status_code == 200
        data = r.json()
        assert data["total_datacenters"] == 3
        assert data["high_risk_count"] == 1
        assert data["accumulation_flagged_count"] == 2
        assert data["flood_zone_count"] == 2

    def test_fast_returns_full_world_payload_and_filters_disabled_sigint_sources(self, client, monkeypatch):
        from services.fetchers import _store

        ships = [{"lat": float(i % 80), "lng": float((i % 360) - 180), "id": i} for i in range(2000)]
        sigint = (
            [{"source": "aprs", "lat": 1.0, "lng": 1.0, "id": f"a-{i}"} for i in range(50)]
            + [{"source": "meshtastic", "lat": 2.0, "lng": 2.0, "id": f"m-{i}"} for i in range(50)]
            + [{"source": "meshtastic", "from_api": True, "lat": 3.0, "lng": 3.0, "id": f"mm-{i}"} for i in range(50)]
            + [{"source": "js8call", "lat": 4.0, "lng": 4.0, "id": f"j-{i}"} for i in range(50)]
        )

        monkeypatch.setitem(_store.latest_data, "ships", ships)
        monkeypatch.setitem(_store.latest_data, "sigint", sigint)
        monkeypatch.setitem(_store.active_layers, "sigint_aprs", False)
        monkeypatch.setitem(_store.active_layers, "sigint_meshtastic", True)

        r = client.get("/api/live-data/fast")

        assert r.status_code == 200
        data = r.json()
        assert len(data["ships"]) == len(ships)
        assert all(item["source"] != "aprs" for item in data["sigint"])
        assert data["sigint_totals"]["aprs"] == 0
        assert data["sigint_totals"]["meshtastic"] == 100
        assert data["sigint_totals"]["meshtastic_map"] == 50
        assert data["sigint_totals"]["js8call"] == 50

    def test_slow_omits_disabled_power_plants_and_returns_full_world_datacenters(self, client, monkeypatch):
        from services.fetchers import _store

        datacenters = [{"lat": float(i % 80), "lng": float((i % 360) - 180), "id": i} for i in range(2000)]
        power_plants = [{"lat": float(i % 80), "lng": float((i % 360) - 180), "id": i} for i in range(4000)]

        monkeypatch.setitem(_store.latest_data, "datacenters", datacenters)
        monkeypatch.setitem(_store.latest_data, "power_plants", power_plants)
        monkeypatch.setitem(_store.active_layers, "datacenters", True)
        monkeypatch.setitem(_store.active_layers, "power_plants", False)

        r = client.get("/api/live-data/slow")

        assert r.status_code == 200
        data = r.json()
        assert len(data["datacenters"]) == len(datacenters)
        assert data["power_plants"] == []

    def test_slow_handles_geojson_incidents_without_crashing(self, client, monkeypatch):
        from services.fetchers import _store

        gdelt = [
            {
                "type": "Feature",
                "properties": {"name": "Incident A"},
                "geometry": {"type": "Point", "coordinates": [10.0, 20.0]},
            }
        ]

        monkeypatch.setitem(_store.latest_data, "gdelt", gdelt)
        monkeypatch.setitem(_store.active_layers, "global_incidents", True)

        r = client.get("/api/live-data/slow")

        assert r.status_code == 200
        data = r.json()
        assert data["gdelt"] == gdelt

    def test_enabling_viirs_layer_queues_immediate_refresh(self, monkeypatch):
        import main
        from httpx import ASGITransport, AsyncClient
        from services.fetchers import _store

        queued = {"called": False}

        monkeypatch.setitem(_store.active_layers, "viirs_nightlights", False)
        monkeypatch.setattr(main, "_queue_viirs_change_refresh", lambda: queued.__setitem__("called", True))

        async def _exercise():
            transport = ASGITransport(app=main.app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                return await ac.post("/api/layers", json={"layers": {"viirs_nightlights": True}})

        response = asyncio.run(_exercise())

        assert response.status_code == 200
        assert response.json()["status"] == "ok"
        assert queued["called"] is True


class TestDebugEndpoint:
    def test_debug_latest_returns_list(self, client):
        r = client.get("/api/debug-latest")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)


class TestSettingsEndpoints:
    def test_get_api_keys(self, client):
        r = client.get("/api/settings/api-keys")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)

    def test_get_news_feeds(self, client):
        r = client.get("/api/settings/news-feeds")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)


class TestAdminProtection:
    def test_refresh_requires_admin_key(self, client, monkeypatch):
        import main

        monkeypatch.setattr(main, "_ADMIN_KEY", "test-key")
        monkeypatch.setattr(main, "_ALLOW_INSECURE_ADMIN", False)

        r = client.get("/api/refresh")
        assert r.status_code == 403

        r_ok = client.get("/api/refresh", headers={"X-Admin-Key": "test-key"})
        assert r_ok.status_code in (200, 202)


class TestRadioEndpoints:
    def test_radio_top_returns_200(self, client):
        r = client.get("/api/radio/top")
        assert r.status_code == 200

    def test_radio_openmhz_systems(self, client):
        r = client.get("/api/radio/openmhz/systems")
        assert r.status_code == 200


class TestQueryValidation:
    def test_region_dossier_rejects_invalid_lat(self, client):
        r = client.get("/api/region-dossier?lat=999&lng=0")
        assert r.status_code == 422

    def test_region_dossier_rejects_invalid_lng(self, client):
        r = client.get("/api/region-dossier?lat=0&lng=999")
        assert r.status_code == 422

    def test_sentinel_rejects_invalid_coords(self, client):
        r = client.get("/api/sentinel2/search?lat=-100&lng=0")
        assert r.status_code == 422

    def test_radio_nearest_rejects_invalid_lat(self, client):
        r = client.get("/api/radio/nearest?lat=91&lng=0")
        assert r.status_code == 422


class TestETagBehavior:
    def test_fast_returns_etag_header(self, client):
        r = client.get("/api/live-data/fast")
        if r.status_code == 200:
            assert "etag" in r.headers

    def test_slow_returns_etag_header(self, client):
        r = client.get("/api/live-data/slow")
        if r.status_code == 200:
            assert "etag" in r.headers
