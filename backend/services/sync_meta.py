"""
Sync metadata store — tracks enrichment script run state.

Persisted at backend/data/sync_meta.json. Written by scripts on completion
and polled by the frontend via /api/data-sync/status.
"""
from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

_META_PATH = Path(__file__).parent.parent / "data" / "sync_meta.json"
_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Static registry — all known enrichment scripts, in pipeline order
# ---------------------------------------------------------------------------
SCRIPTS_REGISTRY: list[dict] = [
    {
        "id": "enrich_l1_physical",
        "label": "L1 — Physical Assets",
        "description": "Operator type (company heuristics), Tier rating (Uptime Institute), "
                       "MW capacity, year built, cooling type, floor level.",
        "module": "scripts.enrich_l1_physical",
        "layer": 1,
        "fields": ["operator_type", "tier_rating", "mw_capacity", "year_built",
                   "cooling_type", "floor_level"],
        "depends_on": [],
    },
    {
        "id": "enrich_l2_hazard",
        "label": "L2 — Hazard Exposure",
        "description": "Point-extracted hazard scores: JRC flood depth (100yr RP), "
                       "USGS PGA, IBTrACS cyclone density, FIRMS wildfire days, ERA5 heat days.",
        "module": "scripts.enrich_l2_hazard",
        "layer": 2,
        "fields": ["jrc_flood_100yr_m", "usgs_pga_10pct_50yr", "ibtracs_track_density",
                   "wildfire_days_50km", "heat_extreme_days"],
        "depends_on": [],
    },
    {
        "id": "enrich_l3_dependencies",
        "label": "L3 — Dependency Graph",
        "description": "Power substation clustering (OSM), IXP proximity (PeeringDB), "
                       "fibre path count, water stress (FAO AQUASTAT), ASN.",
        "module": "scripts.enrich_l3_dependencies",
        "layer": 3,
        "fields": ["substation_osm_id", "substation_cluster_id", "substation_shared_count",
                   "ixp_count_50km", "fibre_path_count", "water_stress_idx", "asn"],
        "depends_on": [],
    },
    {
        "id": "enrich_l4_topology",
        "label": "L4 — Network Topology",
        "description": "Betweenness centrality, systemic importance score, "
                       "accumulation flag — derived from L3 dependency graph.",
        "module": "scripts.enrich_l4_topology",
        "layer": 4,
        "fields": ["betweenness_centrality", "systemic_importance_score", "accumulation_flag"],
        "depends_on": ["enrich_l3_dependencies"],
    },
    {
        "id": "enrich_l5_risk",
        "label": "L5 — Composite Risk Index",
        "description": "Weighted composite risk_score (0–100) from all layer inputs. "
                       "Weights: physical 30%, power 35%, connectivity 20%, systemic 15%.",
        "module": "scripts.enrich_l5_risk",
        "layer": 5,
        "fields": ["risk_score"],
        "depends_on": ["enrich_l1_physical", "enrich_l2_hazard",
                       "enrich_l3_dependencies", "enrich_l4_topology"],
    },
]

# Index by id for fast lookup
REGISTRY_BY_ID: dict[str, dict] = {s["id"]: s for s in SCRIPTS_REGISTRY}


# ---------------------------------------------------------------------------
# Read / write helpers
# ---------------------------------------------------------------------------

def _load() -> dict:
    try:
        if _META_PATH.exists():
            return json.loads(_META_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save(data: dict) -> None:
    tmp = _META_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, _META_PATH)


def get_all_status() -> list[dict]:
    """Return registry merged with persisted run state — one entry per script."""
    with _lock:
        meta = _load()
    result = []
    for script in SCRIPTS_REGISTRY:
        sid = script["id"]
        run = meta.get(sid, {})
        result.append({
            **script,
            "status": run.get("status", "idle"),          # idle|running|success|error
            "last_run_iso": run.get("last_run_iso"),
            "duration_s": run.get("duration_s"),
            "coverage": run.get("coverage", {}),           # field → pct float
            "log_tail": run.get("log_tail", ""),
            "error": run.get("error"),
        })
    return result


def set_running(script_id: str) -> None:
    with _lock:
        meta = _load()
        meta[script_id] = {
            **meta.get(script_id, {}),
            "status": "running",
            "last_run_iso": datetime.now(timezone.utc).isoformat(),
        }
        _save(meta)


def write_result(
    script_id: str,
    *,
    status: str,                    # "success" | "error"
    duration_s: float,
    coverage: Optional[dict] = None,  # {field: pct_float}
    log_tail: str = "",
    error: Optional[str] = None,
) -> None:
    with _lock:
        meta = _load()
        meta[script_id] = {
            "status": status,
            "last_run_iso": datetime.now(timezone.utc).isoformat(),
            "duration_s": round(duration_s, 1),
            "coverage": coverage or {},
            "log_tail": log_tail[-2000:] if log_tail else "",  # cap at 2 KB
            "error": error,
        }
        _save(meta)
