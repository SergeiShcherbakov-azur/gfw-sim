# gfw_sim/snapshot/from_legacy.py
from __future__ import annotations

from typing import Dict, Any
from ..model.entities import Snapshot, Node, Pod, NodePool, InstancePrice, Schedule
from ..types import NodeId, PodId, NodePoolName, InstanceType, Namespace, CpuMillis, Bytes, UsdPerHour

def snapshot_from_legacy_data(data: Dict[str, Any]) -> Snapshot:
    baseline = data.get("baseline", {})
    raw_nodes = baseline.get("nodes", {})
    raw_pods = baseline.get("pods", {})
    raw_pools = data.get("nodepools", {})
    raw_prices = data.get("prices_by_instance", {})
    history_usage = data.get("history_usage", [])

    nodepools = {}
    for k, v in raw_pools.items():
        nodepools[k] = NodePool(
            name=NodePoolName(v.get("name")),
            labels=v.get("labels", {}),
            taints=v.get("taints", []),
            is_keda=v.get("is_keda", False),
            schedule_name="keda-weekdays-12h" if v.get("is_keda") else "default",
            consolidation_policy=v.get("consolidation_policy", "WhenUnderutilized")
        )

    nodes = {}
    for k, v in raw_nodes.items():
        name = v.get("name")
        pool_name = NodePoolName(v.get("nodepool") or "default")
        
        if pool_name not in nodepools:
            is_keda = "keda" in str(pool_name).lower()
            nodepools[pool_name] = NodePool(
                name=pool_name, 
                is_keda=is_keda, 
                schedule_name="keda-weekdays-12h" if is_keda else "default",
                consolidation_policy="WhenUnderutilized"
            )

        nodes[NodeId(name)] = Node(
            id=NodeId(name),
            name=name,
            nodepool=pool_name,
            instance_type=InstanceType(v.get("instance_type", "unknown")),
            alloc_cpu_m=CpuMillis(v.get("alloc_cpu_m", 0)),
            alloc_mem_b=Bytes(v.get("alloc_mem_b", 0)),
            # --- NEW ---
            alloc_pods=int(v.get("alloc_pods", 110)),
            
            capacity_type=v.get("capacity_type", "on_demand"),
            labels=v.get("labels", {}),
            taints=v.get("taints", []),
            is_virtual=v.get("is_virtual", False),
            uptime_hours_24h=v.get("uptime_hours_24h", 24.0)
        )

    pods = {}
    for k, v in raw_pods.items():
        pod_id = PodId(k)
        pods[pod_id] = Pod(
            id=pod_id,
            name=v.get("name", k),
            namespace=Namespace(v.get("namespace", "default")),
            node=NodeId(v.get("node")) if v.get("node") else None,
            owner_kind=v.get("owner_kind"),
            owner_name=v.get("owner_name"),
            req_cpu_m=CpuMillis(v.get("req_cpu_m", 0)),
            req_mem_b=Bytes(v.get("req_mem_b", 0)),
            usage_cpu_m=CpuMillis(v.get("usage_cpu_m", 0)),
            usage_mem_b=Bytes(v.get("usage_mem_b", 0)),
            is_daemonset=v.get("is_daemon", False),
            is_system=v.get("is_system", False),
            is_gfw=v.get("is_gfw", True),
            tolerations=v.get("tolerations", []),
            node_selector=v.get("node_selector", {}),
            affinity=v.get("affinity", {}),
            active_ratio=v.get("active_ratio", 1.0)
        )

    prices = {}
    for k, price in raw_prices.items():
        it = InstanceType(k)
        prices[it] = InstancePrice(instance_type=it, usd_per_hour=UsdPerHour(price))

    schedules = {
        "default": Schedule(name="default"),
        "keda-weekdays-12h": Schedule(name="keda-weekdays-12h", hours_per_day=12.0, days_per_week=5.0)
    }

    return Snapshot(
        nodes=nodes,
        pods=pods,
        nodepools=nodepools,
        prices=prices,
        schedules=schedules,
        keda_pool_name=NodePoolName(data.get("keda_pool", "keda-nightly-al2023-private-c")),
        history_usage=history_usage
    )