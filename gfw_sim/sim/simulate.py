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
    
    # New Aggregated Usage Fields
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

def _bytes_to_gib(v: int) -> float:
    return v / (1024.0 ** 3) if v else 0.0

def _iter_snapshot_pods(snapshot) -> Iterable:
    pods = getattr(snapshot, "pods", None)
    return pods.values() if isinstance(pods, dict) else (pods or [])

def _iter_snapshot_nodes(snapshot) -> Iterable:
    nodes = getattr(snapshot, "nodes", None)
    return nodes.values() if isinstance(nodes, dict) else (nodes or [])

def run_simulation(snapshot) -> SimulationResult:
    pods_by_node: Dict[str, List[PodView]] = {}
    
    # Чтобы посчитать сумму usage, нам нужен доступ к объектам Pod из снапшота, 
    # а PodView их не содержит. Будем считать сумму на лету или сохраним мапу.
    # Проще сохранить сырые поды в мапу для агрегации.
    raw_pods_by_node = {}

    for pod in _iter_snapshot_pods(snapshot):
        if not pod.node: continue
        
        # PodView for frontend
        pv = PodView(
            namespace=pod.namespace, name=pod.name,
            owner_kind=pod.owner_kind, owner_name=pod.owner_name,
            is_gfw=bool(pod.is_gfw), is_daemon=bool(pod.is_daemonset), is_system=bool(pod.is_system),
            req_cpu_m=int(pod.req_cpu_m or 0), req_mem_b=int(pod.req_mem_b or 0),
        )
        pods_by_node.setdefault(pod.node, []).append(pv)
        raw_pods_by_node.setdefault(pod.node, []).append(pod)

    nodes_table: List[NodeRow] = []
    total_cost_daily = 0.0
    total_cost_gfw_nodes = 0.0
    total_cost_keda_nodes = 0.0

    for node in _iter_snapshot_nodes(snapshot):
        node_name = node.name
        nodepool = node.nodepool
        instance_type = node.instance_type
        alloc_cpu_m = int(node.alloc_cpu_m or 0)
        alloc_mem_b = int(node.alloc_mem_b or 0)
        
        node_pods_view = pods_by_node.get(node_name, [])
        node_pods_raw = raw_pods_by_node.get(node_name, [])

        # Request Sums
        gfw_pods = [p for p in node_pods_view if p.is_gfw]
        ds_pods = [p for p in node_pods_view if p.is_daemon]
        other_pods = [p for p in node_pods_view if not p.is_gfw and not p.is_daemon]

        gfw_ratio_pct = (len(gfw_pods) / len(node_pods_view) * 100.0) if node_pods_view else 0.0

        parts = NodeParts(
            gfw_cpu_m=sum(p.req_cpu_m for p in gfw_pods),
            ds_cpu_m=sum(p.req_cpu_m for p in ds_pods),
            other_cpu_m=sum(p.req_cpu_m for p in other_pods),
            gfw_mem_b=sum(p.req_mem_b for p in gfw_pods),
            ds_mem_b=sum(p.req_mem_b for p in ds_pods),
            other_mem_b=sum(p.req_mem_b for p in other_pods),
        )
        
        sum_req_cpu_m = parts.gfw_cpu_m + parts.ds_cpu_m + parts.other_cpu_m
        sum_req_mem_b = parts.gfw_mem_b + parts.ds_mem_b + parts.other_mem_b

        # Usage Sums (Peak 1d)
        # Если usage не собрался (None), считаем как 0, или можно фолбечить на request, но честнее 0.
        sum_usage_cpu_m = sum((p.usage_cpu_m or 0) for p in node_pods_raw)
        sum_usage_mem_b = sum((p.usage_mem_b or 0) for p in node_pods_raw)

        cost_daily_usd, price_missing = costs.node_daily_cost_from_instance(instance_type, nodepool)
        
        row = NodeRow(
            node=node_name, nodepool=nodepool, instance=instance_type,
            gfw_ratio_pct=gfw_ratio_pct,
            alloc_cpu_m=alloc_cpu_m, alloc_mem_b=alloc_mem_b,
            sum_req_cpu_m=sum_req_cpu_m, sum_req_mem_b=sum_req_mem_b,
            sum_usage_cpu_m=int(sum_usage_cpu_m), sum_usage_mem_b=int(sum_usage_mem_b),
            ram_util_pct=(sum_req_mem_b / alloc_mem_b * 100.0) if alloc_mem_b else 0.0,
            ram_ds_gib=_bytes_to_gib(parts.ds_mem_b), ram_gfw_gib=_bytes_to_gib(parts.gfw_mem_b),
            cost_daily_usd=cost_daily_usd, parts=parts,
            is_virtual=node.is_virtual, price_missing=price_missing
        )
        nodes_table.append(row)
        
        total_cost_daily += cost_daily_usd
        if parts.gfw_cpu_m > 0 or parts.gfw_mem_b > 0: total_cost_gfw_nodes += cost_daily_usd
        if "keda" in nodepool.lower(): total_cost_keda_nodes += cost_daily_usd

    return SimulationResult(
        nodes_table=nodes_table, pods_by_node=pods_by_node,
        total_cost_daily_usd=total_cost_daily,
        total_cost_gfw_nodes_usd=total_cost_gfw_nodes,
        total_cost_keda_nodes_usd=total_cost_keda_nodes,
    )