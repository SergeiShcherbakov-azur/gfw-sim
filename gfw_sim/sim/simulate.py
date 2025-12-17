from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Iterable

from . import costs


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class NodeParts:
    """Разбиение потребления ресурсов на группы подов на ноде."""

    gfw_cpu_m: int
    ds_cpu_m: int
    other_cpu_m: int
    gfw_mem_b: int
    ds_mem_b: int
    other_mem_b: int


@dataclass
class NodeRow:
    """Агрегированное представление ноды для фронтенда и отчётов."""

    node: str
    nodepool: str
    instance: str

    gfw_ratio_pct: float

    alloc_cpu_m: int
    alloc_mem_b: int

    sum_req_cpu_m: int
    sum_req_mem_b: int

    ram_util_pct: float
    ram_ds_gib: float
    ram_gfw_gib: float

    cost_daily_usd: float

    parts: NodeParts

    is_virtual: bool
    price_missing: bool


@dataclass
class PodView:
    """Плоское представление пода для фронтенда."""

    namespace: str
    name: str
    owner_kind: Optional[str]
    owner_name: Optional[str]
    is_gfw: bool
    is_daemon: bool
    is_system: bool
    req_cpu_m: int
    req_mem_b: int


@dataclass
class SimulationResult:
    """Результат симуляции, который потребляет API-слой и CLI-утилиты."""

    nodes_table: List[NodeRow]
    pods_by_node: Dict[str, List[PodView]]
    total_cost_daily_usd: float
    total_cost_gfw_nodes_usd: float
    total_cost_keda_nodes_usd: float


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bytes_to_gib(v: int) -> float:
    return v / (1024.0 ** 3) if v else 0.0


def _iter_snapshot_pods(snapshot) -> Iterable:
    """snapshot.pods — dict[str, Pod]."""
    pods = getattr(snapshot, "pods", None)
    if pods is None:
        return []
    if isinstance(pods, dict):
        return pods.values()
    return pods


def _iter_snapshot_nodes(snapshot) -> Iterable:
    """snapshot.nodes — dict[str, Node]."""
    nodes = getattr(snapshot, "nodes", None)
    if nodes is None:
        return []
    if isinstance(nodes, dict):
        return nodes.values()
    return nodes


# ---------------------------------------------------------------------------
# Основная функция симуляции
# ---------------------------------------------------------------------------


def run_simulation(snapshot) -> SimulationResult:
    """Строим агрегированное представление по снапшоту."""

    # 1. Подготовка pods_by_node
    pods_by_node: Dict[str, List[PodView]] = {}

    for pod in _iter_snapshot_pods(snapshot):
        node_name = pod.node
        if not node_name:
            continue

        pv = PodView(
            namespace=pod.namespace,
            name=pod.name,
            owner_kind=pod.owner_kind,
            owner_name=pod.owner_name,
            is_gfw=bool(pod.is_gfw),
            is_daemon=bool(pod.is_daemonset),
            is_system=bool(pod.is_system),
            req_cpu_m=int(pod.req_cpu_m or 0),
            req_mem_b=int(pod.req_mem_b or 0),
        )
        pods_by_node.setdefault(node_name, []).append(pv)

    # 2. Агрегация по нодам
    nodes_table: List[NodeRow] = []
    total_cost_daily = 0.0
    total_cost_gfw_nodes = 0.0
    total_cost_keda_nodes = 0.0

    for node in _iter_snapshot_nodes(snapshot):
        node_name: str = (getattr(node, "name", "") or "")
        nodepool: str = (getattr(node, "nodepool", "") or "")
        instance_type: str = (getattr(node, "instance_type", "") or "")

        alloc_cpu_m: int = int(getattr(node, "alloc_cpu_m", 0) or 0)
        alloc_mem_b: int = int(getattr(node, "alloc_mem_b", 0) or 0)
        is_virtual: bool = bool(getattr(node, "is_virtual", False))

        node_pods = pods_by_node.get(node_name, [])

        total_pods = len(node_pods)
        gfw_pods = [p for p in node_pods if p.is_gfw]
        ds_pods = [p for p in node_pods if p.is_daemon]
        other_pods = [p for p in node_pods if not p.is_gfw and not p.is_daemon]

        gfw_ratio_pct = (len(gfw_pods) / total_pods * 100.0) if total_pods > 0 else 0.0

        gfw_cpu_m = sum(p.req_cpu_m for p in gfw_pods)
        ds_cpu_m = sum(p.req_cpu_m for p in ds_pods)
        other_cpu_m = sum(p.req_cpu_m for p in other_pods)

        gfw_mem_b = sum(p.req_mem_b for p in gfw_pods)
        ds_mem_b = sum(p.req_mem_b for p in ds_pods)
        other_mem_b = sum(p.req_mem_b for p in other_pods)

        sum_req_cpu_m = gfw_cpu_m + ds_cpu_m + other_cpu_m
        sum_req_mem_b = gfw_mem_b + ds_mem_b + other_mem_b

        ram_used_b = sum_req_mem_b
        ram_util_pct = (ram_used_b / alloc_mem_b * 100.0) if alloc_mem_b > 0 else 0.0

        ram_ds_gib = _bytes_to_gib(ds_mem_b)
        ram_gfw_gib = _bytes_to_gib(gfw_mem_b)

        cost_daily_usd, price_missing = costs.node_daily_cost_from_instance(instance_type, nodepool)

        row_parts = NodeParts(
            gfw_cpu_m=gfw_cpu_m,
            ds_cpu_m=ds_cpu_m,
            other_cpu_m=other_cpu_m,
            gfw_mem_b=gfw_mem_b,
            ds_mem_b=ds_mem_b,
            other_mem_b=other_mem_b,
        )

        row = NodeRow(
            node=node_name,
            nodepool=nodepool,
            instance=instance_type,
            gfw_ratio_pct=gfw_ratio_pct,
            alloc_cpu_m=alloc_cpu_m,
            alloc_mem_b=alloc_mem_b,
            sum_req_cpu_m=sum_req_cpu_m,
            sum_req_mem_b=sum_req_mem_b,
            ram_util_pct=ram_util_pct,
            ram_ds_gib=ram_ds_gib,
            ram_gfw_gib=ram_gfw_gib,
            cost_daily_usd=cost_daily_usd,
            parts=row_parts,
            is_virtual=is_virtual,
            price_missing=price_missing,
        )

        nodes_table.append(row)

        total_cost_daily += cost_daily_usd

        has_gfw = row_parts.gfw_cpu_m > 0 or row_parts.gfw_mem_b > 0
        if has_gfw:
            total_cost_gfw_nodes += cost_daily_usd

        if "keda" in nodepool.lower():
            total_cost_keda_nodes += cost_daily_usd

    return SimulationResult(
        nodes_table=nodes_table,
        pods_by_node=pods_by_node,
        total_cost_daily_usd=total_cost_daily,
        total_cost_gfw_nodes_usd=total_cost_gfw_nodes,
        total_cost_keda_nodes_usd=total_cost_keda_nodes,
    )
