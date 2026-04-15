#!/usr/bin/env python3
"""Layer 5 — weighted composite risk scoring for data centers."""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path


logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).parent.parent.parent
DC_PATH = _REPO_ROOT / "backend" / "data" / "datacenters_geocoded.json"


def _clamp_score(value: float) -> float:
    return round(max(0.0, min(value, 100.0)), 1)


def annotate_composite_risk(datacenters: list[dict]) -> list[dict]:
    for dc in datacenters:
        hazard_score = max(
            float(dc.get("hazard_eq") or 0),
            float(dc.get("hazard_flood") or 0),
            float(dc.get("hazard_cyclone") or 0),
            float(dc.get("hazard_fire") or 0),
        )
        power_score = max(
            float(dc.get("grid_score") or 0),
            min(100.0, float(dc.get("substation_shared_count") or 0) * 20.0),
        )
        ixp_count = int(dc.get("ixp_count_50km") or 0)
        fibre_count = int(dc.get("fibre_path_count") or 0)
        network_score = max(
            0.0,
            65.0 - min(ixp_count, 3) * 15.0 + max(0, 2 - fibre_count) * 10.0,
        )
        systemic_score = float(dc.get("systemic_importance_score") or 0)

        dc["hazard_risk_score"] = _clamp_score(hazard_score)
        dc["power_risk_score"] = _clamp_score(power_score)
        dc["network_risk_score"] = _clamp_score(network_score)
        dc["systemic_risk_score"] = _clamp_score(systemic_score)
        dc["risk_score"] = _clamp_score(
            dc["hazard_risk_score"] * 0.35
            + dc["power_risk_score"] * 0.25
            + dc["network_risk_score"] * 0.20
            + dc["systemic_risk_score"] * 0.20
        )

    return datacenters


def enrich(force: bool = False, dc_path: Path = DC_PATH) -> tuple[list[dict], dict[str, float]]:
    dcs = json.loads(dc_path.read_text(encoding="utf-8"))
    valid_dcs = [dc for dc in dcs if dc.get("lat") is not None and dc.get("lng") is not None]
    annotate_composite_risk(valid_dcs)
    total = len(valid_dcs)
    coverage = {
        field: round(sum(1 for dc in valid_dcs if dc.get(field) not in (None, "", [], {})) / total * 100, 1)
        for field in (
            "hazard_risk_score",
            "power_risk_score",
            "network_risk_score",
            "systemic_risk_score",
            "risk_score",
        )
    } if total else {}

    tmp_path = dc_path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(dcs, ensure_ascii=False, indent=None), encoding="utf-8")
    os.replace(tmp_path, dc_path)
    logger.info("L5 composite risk enrichment complete")
    return dcs, coverage


if __name__ == "__main__":
    import time as _time

    parser = argparse.ArgumentParser(description="Layer 5 composite risk enrichment")
    parser.add_argument("--force", action="store_true", help="Overwrite existing non-null L5 fields")
    args = parser.parse_args()

    try:
        from services.sync_meta import write_result, set_running

        set_running("enrich_l5_risk")
    except Exception:
        write_result = set_running = None  # type: ignore

    t0 = _time.time()
    try:
        _, coverage = enrich(force=args.force)
        if write_result:
            write_result(
                "enrich_l5_risk",
                status="success",
                duration_s=_time.time() - t0,
                coverage=coverage,
                log_tail="Layer 5 composite risk enrichment complete",
            )
    except Exception as exc:
        if write_result:
            write_result(
                "enrich_l5_risk",
                status="error",
                duration_s=_time.time() - t0,
                error=str(exc),
            )
        raise
