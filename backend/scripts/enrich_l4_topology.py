#!/usr/bin/env python3
"""Layer 4 — topology scoring for data-center dependency graphs."""

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


def annotate_topology_scores(datacenters: list[dict]) -> list[dict]:
    asn_counts: dict[str, int] = {}
    cluster_counts: dict[str, int] = {}
    ixp_popularity: dict[str, int] = {}

    for dc in datacenters:
        asn = str(dc.get("asn") or "").strip()
        if asn:
            asn_counts[asn] = asn_counts.get(asn, 0) + 1
        cluster = str(dc.get("substation_cluster_id") or "").strip()
        if cluster:
            cluster_counts[cluster] = cluster_counts.get(cluster, 0) + 1
        for ixp_id in dc.get("ixp_ids") or []:
            ixp_popularity[str(ixp_id)] = ixp_popularity.get(str(ixp_id), 0) + 1

    max_cluster = max(cluster_counts.values(), default=1)
    max_asn = max(asn_counts.values(), default=1)
    max_ixp = max(ixp_popularity.values(), default=1)

    for dc in datacenters:
        cluster_size = int(dc.get("substation_shared_count") or cluster_counts.get(str(dc.get("substation_cluster_id") or ""), 0))
        asn_size = asn_counts.get(str(dc.get("asn") or "").strip(), 0)
        ixp_ids = [str(ixp) for ixp in (dc.get("ixp_ids") or [])]
        ixp_overlap = max((ixp_popularity.get(ixp_id, 0) for ixp_id in ixp_ids), default=0)
        redundancy_penalty = 1.0 if not ixp_ids else max(0.0, 1 - min(len(ixp_ids), 3) / 3)

        cluster_norm = min(cluster_size / max_cluster, 1.0) if max_cluster else 0.0
        asn_norm = min(asn_size / max_asn, 1.0) if max_asn else 0.0
        ixp_norm = min(ixp_overlap / max_ixp, 1.0) if max_ixp else 0.0

        centrality = round((cluster_norm * 0.5) + (asn_norm * 0.25) + (ixp_norm * 0.25), 3)
        systemic_score = round(
            min(
                100.0,
                (
                    cluster_norm * 45
                    + asn_norm * 25
                    + ixp_norm * 15
                    + redundancy_penalty * 15
                ),
            ),
            1,
        )

        dc["betweenness_centrality"] = centrality
        dc["systemic_importance_score"] = systemic_score
        dc["accumulation_flag"] = cluster_size >= 3 or systemic_score >= 70

    return datacenters


def enrich(force: bool = False, dc_path: Path = DC_PATH) -> tuple[list[dict], dict[str, float]]:
    dcs = json.loads(dc_path.read_text(encoding="utf-8"))
    valid_dcs = [dc for dc in dcs if dc.get("lat") is not None and dc.get("lng") is not None]
    annotate_topology_scores(valid_dcs)
    total = len(valid_dcs)
    coverage = {
        field: round(sum(1 for dc in valid_dcs if dc.get(field) not in (None, "", [], {})) / total * 100, 1)
        for field in ("betweenness_centrality", "systemic_importance_score", "accumulation_flag")
    } if total else {}

    tmp_path = dc_path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(dcs, ensure_ascii=False, indent=None), encoding="utf-8")
    os.replace(tmp_path, dc_path)
    logger.info("L4 topology enrichment complete")
    return dcs, coverage


if __name__ == "__main__":
    import time as _time

    parser = argparse.ArgumentParser(description="Layer 4 topology enrichment")
    parser.add_argument("--force", action="store_true", help="Overwrite existing non-null L4 fields")
    args = parser.parse_args()

    try:
        from services.sync_meta import write_result, set_running

        set_running("enrich_l4_topology")
    except Exception:
        write_result = set_running = None  # type: ignore

    t0 = _time.time()
    try:
        _, coverage = enrich(force=args.force)
        if write_result:
            write_result(
                "enrich_l4_topology",
                status="success",
                duration_s=_time.time() - t0,
                coverage=coverage,
                log_tail="Layer 4 topology enrichment complete",
            )
    except Exception as exc:
        if write_result:
            write_result(
                "enrich_l4_topology",
                status="error",
                duration_s=_time.time() - t0,
                error=str(exc),
            )
        raise
