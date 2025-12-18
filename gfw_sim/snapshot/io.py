# gfw_sim/snapshot/io.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from ..model.entities import Snapshot
from .from_legacy import snapshot_from_legacy_data

def snapshot_to_dict(snap: Snapshot) -> Dict[str, Any]:
    nodes_dict = {}
    for n in getattr(snap, "nodes", {}).values():
        nodes_dict[n.name] = {
            "name": n.name,
            "nodepool": n.nodepool,
            "instance_type": n.instance_type,
            "alloc_cpu_m": int(n.alloc_cpu_m),
            "alloc_mem_b": int(n.alloc_mem_b),
            # --- NEW ---
            "alloc_pods": int(n.alloc_pods),
            
            "capacity_type": n.capacity_type,
            "labels": n.labels,
            "taints": n.taints,
            "is_virtual": n.is_virtual,
            "uptime_hours_24h": float(n.uptime_hours_24h)
        }

    pods_dict = {}
    for p in getattr(snap, "pods", {}).values():
        pods_dict[p.id] = {
            "name": p.name,
            "namespace": p.namespace,
            "node": p.node,
            "owner_kind": p.owner_kind,
            "owner_name": p.owner_name,
            "req_cpu_m": int(p.req_cpu_m or 0),
            "req_mem_b": int(p.req_mem_b or 0),
            "usage_cpu_m": int(p.usage_cpu_m or 0),
            "usage_mem_b": int(p.usage_mem_b or 0),
            "is_daemon": p.is_daemonset,
            "is_system": p.is_system,
            "is_gfw": p.is_gfw,
            "tolerations": p.tolerations,
            "node_selector": p.node_selector,
            "affinity": p.affinity,
            "active_ratio": getattr(p, "active_ratio", 1.0)
        }
    
    nodepools_dict = {}
    for np in getattr(snap, "nodepools", {}).values():
        nodepools_dict[np.name] = {
            "name": np.name,
            "labels": np.labels,
            "taints": np.taints,
            "is_keda": np.is_keda,
            "consolidation_policy": np.consolidation_policy
        }

    prices_map = {}
    for k, v in getattr(snap, "prices", {}).items():
        prices_map[str(k)] = float(v.usd_per_hour)

    return {
        "baseline": {"nodes": nodes_dict, "pods": pods_dict},
        "nodepools": nodepools_dict,
        "prices_by_instance": prices_map,
        "keda_pool": getattr(snap, "keda_pool_name", None),
        "history_usage": getattr(snap, "history_usage", [])
    }

def save_snapshot_to_file(snap: Snapshot, path: Path) -> None:
    data = snapshot_to_dict(snap)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)

def load_snapshot_from_file(path: Path) -> Snapshot:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return snapshot_from_legacy_data(data)