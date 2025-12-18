# gfw_sim/sim/simulate.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional, Iterable
from . import costs

@dataclass
class NodeParts:
    gfw_cpu_m: int
    ds_cpu_m: int
    other_cpu_m: int
    gfw_mem_b: int
    ds_mem_b: int
    other_mem_b: int

@dataclass
class NodeRow:
    node: str
    nodepool: str
    instance: str
    gfw_ratio_pct: float
    alloc_cpu_m: int
    alloc_mem_b: int
    sum_req_cpu_m: int
    sum_req_mem_b: int
    sum_usage_cpu_m: int
    sum_usage_mem_b: int
    ram_util_pct: float
    ram_ds_gib: float
    ram_gfw_gib: float
    cost_daily_usd: float
    parts: NodeParts
    is_virtual: bool
    price_missing: bool

@dataclass
class PodView:
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
    nodes_table: List[NodeRow]
    pods_by_node: Dict[str, List[PodView]]
    total_cost_daily_usd: float
    total_cost_gfw_nodes_usd: float
    total_cost_keda_nodes_usd: float
    pool_costs_usd: Dict[str, float]

def _iter_snapshot_pods(snapshot) -> Iterable:
    pods = getattr(snapshot, "pods", None)
    return pods.values() if isinstance(pods, dict) else (pods or [])

def _iter_snapshot_nodes(snapshot) -> Iterable:
    nodes = getattr(snapshot, "nodes", None)
    return nodes.values() if isinstance(nodes, dict) else (nodes or [])

def run_simulation(snapshot) -> SimulationResult:
    pods_by_node: Dict[str, List[PodView]] = {}
    raw_pods_by_node = {}

    for pod in _iter_snapshot_pods(snapshot):
        if not pod.node: continue
        pv = PodView(
            namespace=pod.namespace, name=pod.name, owner_kind=pod.owner_kind, owner_name=pod.owner_name,
            is_gfw=bool(pod.is_gfw), is_daemon=bool(pod.is_daemonset), is_system=bool(pod.is_system),
            req_cpu_m=int(pod.req_cpu_m or 0), req_mem_b=int(pod.req_mem_b or 0),
        )
        pods_by_node.setdefault(pod.node, []).append(pv)
        raw_pods_by_node.setdefault(pod.node, []).append(pod)

    nodes_table: List[NodeRow] = []
    
    # 1. Считаем исторический кост (если есть данные из VM)
    history = getattr(snapshot, "history_usage", [])
    pool_costs_usd: Dict[str, float] = {}
    
    # ВАЖНО: Получаем доступ к сырым ценам
    pricing_state = costs.get_state()
    
    if history:
        for entry in history:
            pool = entry.get("pool", "unknown")
            inst = entry.get("instance", "unknown")
            hours = entry.get("instance_hours_24h", 0.0)
            
            # ИСПРАВЛЕНИЕ: Используем сырую цену часа (On-Demand).
            # Не используем node_daily_cost_from_instance, так как она применяет 
            # коэффициенты расписания (например, 12/24), которые уже учтены в реальном hours.
            hourly_price = pricing_state.hourly_prices.get(inst, 0.0)
            
            cost = hourly_price * hours
            pool_costs_usd[pool] = pool_costs_usd.get(pool, 0.0) + cost
            
    # 2. Считаем текущие ноды (таблица)
    total_cost_daily_projected = 0.0 

    for node in _iter_snapshot_nodes(snapshot):
        node_name = getattr(node, "name", "") or ""
        nodepool_val = getattr(node, "nodepool", "")
        nodepool = str(nodepool_val) if nodepool_val is not None else ""
        instance_type = getattr(node, "instance_type", "") or ""
        alloc_cpu_m = int(getattr(node, "alloc_cpu_m", 0) or 0)
        alloc_mem_b = int(getattr(node, "alloc_mem_b", 0) or 0)
        
        node_pods_view = pods_by_node.get(node_name, [])
        node_pods_raw = raw_pods_by_node.get(node_name, [])

        gfw_pods = [p for p in node_pods_view if p.is_gfw]
        ds_pods = [p for p in node_pods_view if p.is_daemon]
        other_pods = [p for p in node_pods_view if not p.is_gfw and not p.is_daemon]
        gfw_ratio_pct = (len(gfw_pods) / len(node_pods_view) * 100.0) if node_pods_view else 0.0

        parts = NodeParts(
            gfw_cpu_m=sum(p.req_cpu_m for p in gfw_pods), ds_cpu_m=sum(p.req_cpu_m for p in ds_pods), other_cpu_m=sum(p.req_cpu_m for p in other_pods),
            gfw_mem_b=sum(p.req_mem_b for p in gfw_pods), ds_mem_b=sum(p.req_mem_b for p in ds_pods), other_mem_b=sum(p.req_mem_b for p in other_pods),
        )
        
        sum_usage_cpu_m = sum((getattr(p, "usage_cpu_m", 0) or 0) for p in node_pods_raw)
        sum_usage_mem_b = sum((getattr(p, "usage_mem_b", 0) or 0) for p in node_pods_raw)

        # Для таблицы (Run Rate) оставляем старый расчет "будущего" с учетом расписания
        cost_daily_usd, price_missing = costs.node_daily_cost_from_instance(instance_type, nodepool)
        total_cost_daily_projected += cost_daily_usd
        
        # Если истории не было (фоллбек), заполняем pool_costs_usd прогнозными данными
        if not history:
            pool_costs_usd[nodepool] = pool_costs_usd.get(nodepool, 0.0) + cost_daily_usd

        row = NodeRow(
            node=node_name, nodepool=nodepool, instance=instance_type,
            gfw_ratio_pct=gfw_ratio_pct,
            alloc_cpu_m=alloc_cpu_m, alloc_mem_b=alloc_mem_b,
            sum_req_cpu_m=parts.gfw_cpu_m + parts.ds_cpu_m + parts.other_cpu_m, 
            sum_req_mem_b=parts.gfw_mem_b + parts.ds_mem_b + parts.other_mem_b,
            sum_usage_cpu_m=int(sum_usage_cpu_m), sum_usage_mem_b=int(sum_usage_mem_b),
            ram_util_pct=(parts.gfw_mem_b + parts.ds_mem_b + parts.other_mem_b) / alloc_mem_b * 100.0 if alloc_mem_b else 0.0,
            ram_ds_gib=parts.ds_mem_b / (1024**3), ram_gfw_gib=parts.gfw_mem_b / (1024**3),
            cost_daily_usd=cost_daily_usd, parts=parts,
            is_virtual=bool(getattr(node, "is_virtual", False)), price_missing=price_missing
        )
        nodes_table.append(row)

    total_cost_from_pools = sum(pool_costs_usd.values())

    return SimulationResult(
        nodes_table=nodes_table, pods_by_node=pods_by_node,
        total_cost_daily_usd=total_cost_from_pools,
        total_cost_gfw_nodes_usd=0.0,
        total_cost_keda_nodes_usd=0.0,
        pool_costs_usd=pool_costs_usd
    )