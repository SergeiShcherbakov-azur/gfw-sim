# gfw_sim/sim/simulate.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional, Iterable, Any
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
    active_ratio: float = 1.0

@dataclass
class SimulationResult:
    nodes_table: List[NodeRow]
    pods_by_node: Dict[str, List[PodView]]
    total_cost_daily_usd: float
    pool_costs_usd: Dict[str, float]
    projected_pool_costs_usd: Dict[str, float]
    projected_total_cost_usd: float
    total_cost_gfw_nodes_usd: float
    total_cost_keda_nodes_usd: float

def _iter_snapshot_pods(snapshot) -> Iterable:
    pods = getattr(snapshot, "pods", None)
    return pods.values() if isinstance(pods, dict) else (pods or [])

def _iter_snapshot_nodes(snapshot) -> Iterable:
    nodes = getattr(snapshot, "nodes", None)
    return nodes.values() if isinstance(nodes, dict) else (nodes or [])

def run_simulation(snapshot) -> SimulationResult:
    pods_by_node: Dict[str, List[PodView]] = {}
    raw_pods_by_node = {}
    
    pods_by_pool: Dict[str, List] = {}
    pool_node_specs: Dict[str, Any] = {}
    nodes_map = getattr(snapshot, "nodes", {})

    for pod in _iter_snapshot_pods(snapshot):
        if not pod.node: continue
        pv = PodView(
            namespace=pod.namespace, name=pod.name, owner_kind=pod.owner_kind, owner_name=pod.owner_name,
            is_gfw=bool(pod.is_gfw), is_daemon=bool(pod.is_daemonset), is_system=bool(pod.is_system),
            req_cpu_m=int(pod.req_cpu_m or 0), req_mem_b=int(pod.req_mem_b or 0),
            active_ratio=getattr(pod, "active_ratio", 1.0)
        )
        pods_by_node.setdefault(pod.node, []).append(pv)
        raw_pods_by_node.setdefault(pod.node, []).append(pod)
        
        node_obj = nodes_map.get(pod.node)
        if node_obj:
            pool = str(getattr(node_obj, "nodepool", "unknown"))
            if not pod.is_daemonset:
                pods_by_pool.setdefault(pool, []).append(pod)
            
            if pool not in pool_node_specs:
                pool_node_specs[pool] = {
                    "cpu": getattr(node_obj, "alloc_cpu_m", 0),
                    "mem": getattr(node_obj, "alloc_mem_b", 0),
                    "type": getattr(node_obj, "instance_type", "unknown")
                }

    # 1. Historical
    history = getattr(snapshot, "history_usage", [])
    pool_costs_usd: Dict[str, float] = {}
    pricing_state = costs.get_state()
    
    if history:
        for entry in history:
            pool = entry.get("pool", "unknown")
            inst = entry.get("instance", "unknown")
            hours = entry.get("instance_hours_24h", 0.0)
            hourly_price = pricing_state.hourly_prices.get(inst, 0.0)
            cost = hourly_price * hours
            pool_costs_usd[pool] = pool_costs_usd.get(pool, 0.0) + cost

    # 2. Projected
    projected_pool_costs_usd: Dict[str, float] = {}
    projected_total_cost_usd = 0.0
    EFFICIENCY_FACTOR = 1.15 

    for pool, pool_pods in pods_by_pool.items():
        spec = pool_node_specs.get(pool)
        if not spec or not spec["cpu"]: continue
        
        total_cpu_hours = sum(p.req_cpu_m * 24.0 * getattr(p, "active_ratio", 1.0) for p in pool_pods)
        total_mem_hours = sum(p.req_mem_b * 24.0 * getattr(p, "active_ratio", 1.0) for p in pool_pods)
        
        node_cap_cpu_24h = spec["cpu"] * 24.0
        node_cap_mem_24h = spec["mem"] * 24.0
        
        if node_cap_cpu_24h > 0:
            nodes_needed_cpu = total_cpu_hours / node_cap_cpu_24h
            nodes_needed_mem = total_mem_hours / node_cap_mem_24h
            effective_nodes = max(nodes_needed_cpu, nodes_needed_mem) * EFFICIENCY_FACTOR
            hourly_price = pricing_state.hourly_prices.get(spec["type"], 0.0)
            proj_cost = effective_nodes * hourly_price * 24.0
            projected_pool_costs_usd[pool] = proj_cost
            projected_total_cost_usd += proj_cost

    for node in _iter_snapshot_nodes(snapshot):
        pool = str(getattr(node, "nodepool", "unknown"))
        if pool not in projected_pool_costs_usd:
             inst = getattr(node, "instance_type", "")
             hourly = pricing_state.hourly_prices.get(inst, 0.0)
             cost = hourly * 24.0
             projected_pool_costs_usd[pool] = projected_pool_costs_usd.get(pool, 0.0) + cost
             projected_total_cost_usd += cost

    # 3. Nodes Table
    nodes_table: List[NodeRow] = []
    for node in _iter_snapshot_nodes(snapshot):
        node_name = getattr(node, "name", "")
        nodepool = str(getattr(node, "nodepool", ""))
        instance_type = getattr(node, "instance_type", "")
        alloc_cpu = int(getattr(node, "alloc_cpu_m", 0) or 0)
        alloc_mem = int(getattr(node, "alloc_mem_b", 0) or 0)
        
        node_pods = pods_by_node.get(node_name, [])
        gfw = [p for p in node_pods if p.is_gfw]
        ds = [p for p in node_pods if p.is_daemon]
        oth = [p for p in node_pods if not p.is_gfw and not p.is_daemon]
        
        parts = NodeParts(
            sum(p.req_cpu_m for p in gfw), sum(p.req_cpu_m for p in ds), sum(p.req_cpu_m for p in oth),
            sum(p.req_mem_b for p in gfw), sum(p.req_mem_b for p in ds), sum(p.req_mem_b for p in oth)
        )
        
        sum_usage_cpu_m = sum((getattr(p, "usage_cpu_m", 0) or 0) for p in raw_pods_by_node.get(node_name, []))
        sum_usage_mem_b = sum((getattr(p, "usage_mem_b", 0) or 0) for p in raw_pods_by_node.get(node_name, []))

        cost_daily, missing = costs.node_daily_cost_from_instance(instance_type, nodepool)
        
        nodes_table.append(NodeRow(
            node=node_name, nodepool=nodepool, instance=instance_type,
            gfw_ratio_pct=(len(gfw)/len(node_pods)*100 if node_pods else 0),
            alloc_cpu_m=alloc_cpu, alloc_mem_b=alloc_mem,
            sum_req_cpu_m=parts.gfw_cpu_m+parts.ds_cpu_m+parts.other_cpu_m,
            sum_req_mem_b=parts.gfw_mem_b+parts.ds_mem_b+parts.other_mem_b,
            sum_usage_cpu_m=int(sum_usage_cpu_m), sum_usage_mem_b=int(sum_usage_mem_b),
            ram_util_pct=(parts.gfw_mem_b+parts.ds_mem_b+parts.other_mem_b)/alloc_mem*100 if alloc_mem else 0,
            ram_ds_gib=parts.ds_mem_b/1073741824, ram_gfw_gib=parts.gfw_mem_b/1073741824,
            cost_daily_usd=cost_daily, parts=parts, is_virtual=bool(getattr(node, "is_virtual", False)), price_missing=missing
        ))

    if not history:
        pool_costs_usd = projected_pool_costs_usd
        total_cost_from_pools = projected_total_cost_usd
    else:
        total_cost_from_pools = sum(pool_costs_usd.values())

    return SimulationResult(
        nodes_table=nodes_table, pods_by_node=pods_by_node,
        total_cost_daily_usd=total_cost_from_pools,
        pool_costs_usd=pool_costs_usd,
        projected_pool_costs_usd=projected_pool_costs_usd,
        projected_total_cost_usd=projected_total_cost_usd,
        total_cost_gfw_nodes_usd=0.0,
        total_cost_keda_nodes_usd=0.0,
    )