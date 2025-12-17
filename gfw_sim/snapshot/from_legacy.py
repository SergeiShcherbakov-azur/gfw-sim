# gfw_sim/snapshot/from_legacy.py
from __future__ import annotations

from typing import Dict, Optional

from ..model.entities import (
    Snapshot,
    Node,
    Pod,
    NodePool,
    InstancePrice,
    Schedule,
)
from ..types import (
    NodeId,
    PodId,
    NodePoolName,
    InstanceType,
    Namespace,
    CpuMillis,
    Bytes,
    UsdPerHour,
)


SYSTEM_NAMESPACES = {
    "default",
    "vector",
    "victoria-metrics",
    "oomkill-exporter",
    "kube-system",
    "mount-s3",
}


def _merge_price_sources(data: dict) -> Dict[InstanceType, InstancePrice]:
    """Сбор цен из разных вариантов полей в legacy-блобе."""
    result: Dict[InstanceType, InstancePrice] = {}

    def add_prices(src: Optional[dict], source_name: str) -> None:
        if not src:
            return
        for inst_str, hourly in src.items():
            inst = InstanceType(inst_str)
            result[inst] = InstancePrice(
                instance_type=inst,
                usd_per_hour=UsdPerHour(float(hourly)),
                purchasing="on_demand",
                source=source_name,
            )

    prices_by_instance = data.get("prices_by_instance")
    if prices_by_instance:
        add_prices(prices_by_instance, "prices_by_instance")
    else:
        add_prices(data.get("prices_default"), "prices_default")
        add_prices(data.get("prices_keda"), "prices_keda")

    return result


def snapshot_from_legacy_data(data: dict) -> Snapshot:
    baseline = data["baseline"]
    prices = _merge_price_sources(data)
    keda_pool = data.get("keda_pool")

    schedules: Dict[str, Schedule] = {
        "default": Schedule(
            name="default",
            hours_per_day=24.0,
            days_per_week=7.0,
        ),
        "keda-weekdays-12h": Schedule(
            name="keda-weekdays-12h",
            hours_per_day=12.0,
            days_per_week=5.0,
        ),
    }

    # NodePool'ы
    nodepools: Dict[NodePoolName, NodePool] = {}
    for node_name, n in baseline["nodes"].items():
        # Защита от отсутствия поля nodepool
        pool_str = n.get("nodepool", "default") or "default"
        pool_name = NodePoolName(pool_str)
        
        if pool_name in nodepools:
            continue
            
        is_keda = bool(keda_pool) and pool_str == keda_pool
        # Если keda_pool не задан, пробуем угадать по имени
        if not is_keda and "keda" in pool_str.lower():
            is_keda = True

        nodepools[pool_name] = NodePool(
            name=pool_name,
            labels={},
            taints=n.get("taints", []),
            is_keda=is_keda,
            schedule_name="keda-weekdays-12h" if is_keda else "default",
        )

    # Ноды
    nodes: Dict[NodeId, Node] = {}
    for node_name, n in baseline["nodes"].items():
        node_id = NodeId(node_name)
        pool_str = n.get("nodepool", "default") or "default"
        
        nodes[node_id] = Node(
            id=node_id,
            name=node_name,
            nodepool=NodePoolName(pool_str),
            instance_type=InstanceType(n.get("instance_type", "unknown")),
            alloc_cpu_m=CpuMillis(int(n.get("alloc_cpu_m", 0))),
            alloc_mem_b=Bytes(int(n.get("alloc_mem_b", 0))),
            capacity_type="on_demand",
            labels=n.get("labels", {}),
            taints=n.get("taints", []),
            is_virtual=False,
        )

    # Pod'ы
    pods: Dict[PodId, Pod] = {}
    for key, p in baseline["pods"].items():
        ns_str = p.get("namespace", "default")
        ns = Namespace(ns_str)
        pod_id = PodId(key)
        node_name: Optional[str] = p.get("node")

        # --- ЧИТАЕМ МЕТРИКИ ---
        u_cpu = p.get("usage_cpu_m")
        u_mem = p.get("usage_mem_b")

        pods[pod_id] = Pod(
            id=pod_id,
            name=p.get("name", ""),
            namespace=ns,
            node=NodeId(node_name) if node_name else None,
            owner_kind=p.get("owner_kind"),
            owner_name=p.get("owner_name"),
            req_cpu_m=CpuMillis(int(p.get("req_cpu_m", 0))),
            req_mem_b=Bytes(int(p.get("req_mem_b", 0))),
            limit_cpu_m=None,
            limit_mem_b=None,
            is_daemonset=bool(p.get("is_daemon", False)),
            is_system=(ns_str in SYSTEM_NAMESPACES),
            is_gfw=bool(p.get("is_gfw", False)),
            tolerations=p.get("tolerations", []) or [],
            node_selector=p.get("node_selector", {}) or {},
            affinity=p.get("affinity", {}) or {},
            
            # Передаем в конструктор
            usage_cpu_m=CpuMillis(u_cpu) if u_cpu is not None else None,
            usage_mem_b=Bytes(u_mem) if u_mem is not None else None,
        )

    snapshot = Snapshot(
        nodes=nodes,
        pods=pods,
        nodepools=nodepools,
        prices=prices,
        schedules=schedules,
        keda_pool_name=NodePoolName(keda_pool) if keda_pool else None,
    )
    return snapshot


def snapshot_from_baseline(
    baseline: dict,
    prices_by_instance: dict,
    keda_pool: Optional[str],
) -> Snapshot:
    data = {
        "baseline": baseline,
        "prices_by_instance": prices_by_instance,
        "keda_pool": keda_pool,
    }
    return snapshot_from_legacy_data(data)