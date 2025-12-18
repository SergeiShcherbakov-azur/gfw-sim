# gfw_sim/sim/simulate.py
from __future__ import annotations
from dataclasses import dataclass, field
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
    id: str
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
    
    # Scheduling constraints
    affinity: Dict[str, Any] = field(default_factory=dict)
    topology_spread: List[Dict[str, Any]] = field(default_factory=list)
    node_selector: Dict[str, str] = field(default_factory=dict)
    tolerations: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def sort_key(self):
        # Sort by Memory then CPU (most constrained first)
        return (self.req_mem_b, self.req_cpu_m, self.id)

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

@dataclass
class InstanceTypeSpec:
    name: str
    alloc_cpu: int
    alloc_mem: int
    max_pods: int
    price_hourly: float
    ds_overhead_cpu: int = 0
    ds_overhead_mem: int = 0
    
    labels: Dict[str, str] = field(default_factory=dict)
    taints: List[Dict[str, str]] = field(default_factory=list)

    @property
    def net_cpu(self) -> int:
        return max(0, self.alloc_cpu - self.ds_overhead_cpu)
    @property
    def net_mem(self) -> int:
        return max(0, self.alloc_mem - self.ds_overhead_mem)

@dataclass
class SimNode:
    name: str
    spec: InstanceTypeSpec
    pool: str
    is_existing: bool = True
    
    # Usage tracking
    used_cpu: int = 0
    used_mem: int = 0
    pod_count: int = 0
    
    workload_pods: List[PodView] = field(default_factory=list)
    daemon_pods: List[PodView] = field(default_factory=list)

    def is_overloaded(self) -> bool:
        # Check if current usage exceeds capacity
        if self.pod_count > self.spec.max_pods: return True
        if self.used_cpu > self.spec.alloc_cpu: return True
        if self.used_mem > self.spec.alloc_mem: return True
        return False

    def can_fit(self, p: PodView) -> bool:
        # Strict check for new placements
        if self.pod_count + 1 > self.spec.max_pods: return False
        if (self.used_cpu + p.req_cpu_m > self.spec.alloc_cpu): return False
        if (self.used_mem + p.req_mem_b > self.spec.alloc_mem): return False
        
        # Anti-Affinity Check
        if self._check_anti_affinity_conflict(p): return False
        return True

    def add(self, p: PodView):
        self.used_cpu += p.req_cpu_m
        self.used_mem += p.req_mem_b
        self.pod_count += 1
        if p.is_daemon:
            self.daemon_pods.append(p)
        else:
            self.workload_pods.append(p)

    def remove(self, p: PodView):
        self.used_cpu -= p.req_cpu_m
        self.used_mem -= p.req_mem_b
        self.pod_count -= 1
        if p.is_daemon:
            self.daemon_pods.remove(p)
        else:
            self.workload_pods.remove(p)
    
    def _check_anti_affinity_conflict(self, p: PodView) -> bool:
        p_anti = p.affinity.get("podAntiAffinity", {}).get("requiredDuringSchedulingIgnoredDuringExecution", [])
        if not p_anti: return False
        for term in p_anti:
            if term.get("topologyKey") == "kubernetes.io/hostname":
                if self._has_pod_matching_selector(p):
                    return True
        return False

    def _has_pod_matching_selector(self, p: PodView) -> bool:
        if not p.owner_name: return False
        p_prefix = p.owner_name[:15]
        all_pods = self.workload_pods + self.daemon_pods
        for existing in all_pods:
            if existing.namespace == p.namespace:
                 if existing.owner_name and existing.owner_name.startswith(p_prefix):
                     return True
        return False

# --- Helpers ---

def _iter_snapshot_pods(snapshot) -> Iterable:
    pods = getattr(snapshot, "pods", None)
    return pods.values() if isinstance(pods, dict) else (pods or [])

def _iter_snapshot_nodes(snapshot) -> Iterable:
    nodes = getattr(snapshot, "nodes", None)
    return nodes.values() if isinstance(nodes, dict) else (nodes or [])

def _check_taint_tolerations(node_taints: list, pod_tolerations: list) -> bool:
    for taint in node_taints:
        key = taint.get("key")
        val = taint.get("value")
        effect = taint.get("effect")
        if effect not in ("NoSchedule", "NoExecute"): continue     
        tolerated = False
        for tol in pod_tolerations:
            t_key = tol.get("key")
            t_op = tol.get("operator", "Equal")
            t_val = tol.get("value")
            if t_key is None and t_op == "Exists": tolerated = True; break
            if t_key == key:
                if t_op == "Exists": tolerated = True; break
                if t_val == val: tolerated = True; break
        if not tolerated: return False
    return True

def _check_node_selector(node_labels: dict, pod_selector: dict) -> bool:
    for k, v in pod_selector.items():
        if k not in node_labels or node_labels[k] != v: return False
    return True

# --- Scheduler Simulation ---

def bin_pack_pods(
    pods_to_schedule: List[PodView],
    allowed_instances: List[InstanceTypeSpec],
    time_factor: float
) -> float:
    if not pods_to_schedule: return 0.0
    if not allowed_instances: return 0.0

    sorted_instances = sorted(allowed_instances, key=lambda x: x.price_hourly)
    sorted_pods = sorted(pods_to_schedule, key=lambda p: p.sort_key, reverse=True)
    
    new_nodes: List[SimNode] = []
    
    for p in sorted_pods:
        placed = False
        for node in new_nodes:
            if node.can_fit(p):
                node.add(p)
                placed = True
                break
        
        if not placed:
            best_spec = None
            for spec in sorted_instances:
                # Check against Net Capacity (Alloc - DS)
                if p.req_cpu_m <= spec.net_cpu and p.req_mem_b <= spec.net_mem:
                     if spec.max_pods > 0: 
                        best_spec = spec
                        break
            
            if not best_spec:
                best_spec = max(allowed_instances, key=lambda x: x.net_mem)
            
            sim_node = SimNode(
                name=f"new-node-{len(new_nodes)}",
                spec=best_spec,
                pool="dynamic",
                is_existing=False,
                used_cpu=best_spec.ds_overhead_cpu,
                used_mem=best_spec.ds_overhead_mem,
                pod_count=0 
            )
            sim_node.add(p)
            new_nodes.append(sim_node)
            
    cost = 0.0
    for n in new_nodes:
        cost += n.spec.price_hourly * 24.0 * time_factor
    return cost

def run_simulation(snapshot) -> SimulationResult:
    # 1. Prep
    all_pods_raw = list(_iter_snapshot_pods(snapshot))
    pricing_state = costs.get_state()
    
    # Identify DaemonSets
    ds_templates = []
    seen_ds = set()
    for p in all_pods_raw:
        if p.is_daemonset:
            key = (p.namespace, p.owner_name)
            if key not in seen_ds:
                ds_templates.append(p)
                seen_ds.add(key)
    
    # 2. Build Nodes & Catalog
    sim_nodes: Dict[str, SimNode] = {}
    pool_instance_catalog: Dict[str, Dict[str, InstanceTypeSpec]] = {}
    
    def create_spec_from_node(node) -> InstanceTypeSpec:
        inst_type = getattr(node, "instance_type", "unknown")
        price = pricing_state.hourly_prices.get(inst_type, 0.0)
        return InstanceTypeSpec(
            name=inst_type,
            alloc_cpu=int(getattr(node, "alloc_cpu_m", 0)),
            alloc_mem=int(getattr(node, "alloc_mem_b", 0)),
            max_pods=int(getattr(node, "alloc_pods", 110)),
            price_hourly=price,
            labels=getattr(node, "labels", {}),
            taints=getattr(node, "taints", [])
        )

    for node in _iter_snapshot_nodes(snapshot):
        pool = str(getattr(node, "nodepool", "unknown"))
        inst_type = getattr(node, "instance_type", "unknown")
        
        spec = create_spec_from_node(node)
        sim_node = SimNode(
            name=getattr(node, "name"),
            spec=spec,
            pool=pool,
            is_existing=True
        )
        sim_nodes[sim_node.name] = sim_node
        
        if pool not in pool_instance_catalog: pool_instance_catalog[pool] = {}
        if inst_type not in pool_instance_catalog[pool]:
            # Calc DS overhead for this template
            overhead_cpu = 0
            overhead_mem = 0
            for ds in ds_templates:
                if _check_node_selector(spec.labels, ds.node_selector) and \
                   _check_taint_tolerations(spec.taints, ds.tolerations):
                       overhead_cpu += int(ds.req_cpu_m or 0)
                       overhead_mem += int(ds.req_mem_b or 0)
            spec.ds_overhead_cpu = overhead_cpu
            spec.ds_overhead_mem = overhead_mem
            pool_instance_catalog[pool][inst_type] = spec

    # 3. Assign Pods (Hydration)
    overflow_pods: Dict[str, List[PodView]] = {}
    pods_by_node_export: Dict[str, List[PodView]] = {}
    raw_pods_by_node_export = {}

    for pod in all_pods_raw:
        ratio = getattr(pod, "active_ratio", 1.0)
        pv = PodView(
            id=getattr(pod, "id", ""),
            namespace=pod.namespace, name=pod.name, owner_kind=pod.owner_kind, owner_name=pod.owner_name,
            is_gfw=bool(pod.is_gfw), is_daemon=bool(pod.is_daemonset), is_system=bool(pod.is_system),
            req_cpu_m=int(pod.req_cpu_m or 0), req_mem_b=int(pod.req_mem_b or 0),
            active_ratio=ratio,
            affinity=getattr(pod, "affinity", {}),
            topology_spread=getattr(pod, "topology_spread_constraints", []),
            node_selector=getattr(pod, "node_selector", {}),
            tolerations=getattr(pod, "tolerations", [])
        )
        
        node_id = getattr(pod, "node")
        
        if node_id:
            pods_by_node_export.setdefault(node_id, []).append(pv)
            raw_pods_by_node_export.setdefault(node_id, []).append(pod)
        
        assigned = False
        if node_id in sim_nodes:
            node = sim_nodes[node_id]
            # FORCE ADD first to reflect UI state
            node.add(pv)
            assigned = True
        
        if not assigned and not pv.is_daemon:
            # Pod is pending or target node not found
            # Can't reliably guess pool, skipping cost calc for orphaned pods for now
            pass

    # 4. Overflow Handling (Spillover)
    # Check if any node is overloaded. If so, eject pods to overflow.
    for node in sim_nodes.values():
        if node.is_overloaded():
            # Heuristic: Remove Workload pods until it fits
            # We sort by sort_key (Large first) to eject big pods first? 
            # Or Small first to minimize disruption? 
            # Let's eject Smallest first (less impact on bin packing later)
            # Actually, standard eviction is mostly random/QoS. 
            # We assume user moved something IN, so we want to eject that. But we don't know which.
            # Ejecting *Last Added* might be best if order was preserved?
            # List is appended.
            
            # Simple strategy: Pop from workload_pods until fit
            while node.is_overloaded() and node.workload_pods:
                popped = node.workload_pods.pop() # Remove last added
                # Also correct usage counters
                node.used_cpu -= popped.req_cpu_m
                node.used_mem -= popped.req_mem_b
                node.pod_count -= 1
                
                overflow_pods.setdefault(node.pool, []).append(popped)

    # 5. Calculate Costs
    pool_costs_usd: Dict[str, float] = {}
    projected_pool_costs: Dict[str, float] = {}
    projected_total = 0.0
    SCALING_LAG_HOURS = 0.5
    
    active_pools = set(n.pool for n in sim_nodes.values()) | set(overflow_pods.keys())
    
    for pool in active_pools:
        nodes_in_pool = [n for n in sim_nodes.values() if n.pool == pool]
        
        # Scale Down: Node active only if it has workload pods
        active_nodes = [n for n in nodes_in_pool if len(n.workload_pods) > 0]
        
        # KEDA Factor
        all_pool_pods = [p for n in active_nodes for p in n.workload_pods] + overflow_pods.get(pool, [])
        max_ratio = max((p.active_ratio for p in all_pool_pods), default=1.0)
        
        effective_hours = 24.0
        if max_ratio < 0.98:
             effective_hours = (max_ratio * 24.0) + SCALING_LAG_HOURS
             effective_hours = min(24.0, effective_hours)
        time_factor = effective_hours / 24.0
        
        # Cost of existing
        existing_cost = sum(n.spec.price_hourly * 24.0 * time_factor for n in active_nodes)
        
        # Cost of Overflow (Scale Up)
        pending = overflow_pods.get(pool, [])
        new_nodes_cost = 0.0
        if pending:
            catalog = list(pool_instance_catalog.get(pool, {}).values())
            if catalog:
                new_nodes_cost = bin_pack_pods(pending, catalog, time_factor)
        
        total = existing_cost + new_nodes_cost
        projected_pool_costs[pool] = total
        projected_total += total

    # 6. Actuals from History
    actual_costs_from_hist = {}
    history = getattr(snapshot, "history_usage", [])
    if history:
        for entry in history:
            p = entry.get("pool", "unknown")
            i = entry.get("instance", "unknown")
            h = entry.get("instance_hours_24h", 0.0)
            price = pricing_state.hourly_prices.get(i, 0.0)
            actual_costs_from_hist[p] = actual_costs_from_hist.get(p, 0.0) + (price * h)

    # 7. Table
    nodes_table: List[NodeRow] = []
    for node in _iter_snapshot_nodes(snapshot):
        node_name = getattr(node, "name", "")
        pool_name = str(getattr(node, "nodepool", ""))
        inst_type = getattr(node, "instance_type", "")
        
        pods = pods_by_node_export.get(node_name, [])
        gfw = [p for p in pods if p.is_gfw]
        ds = [p for p in pods if p.is_daemon]
        oth = [p for p in pods if not p.is_gfw and not p.is_daemon]
        
        alloc_cpu = int(getattr(node, "alloc_cpu_m", 0))
        alloc_mem = int(getattr(node, "alloc_mem_b", 0))

        parts = NodeParts(
            sum(p.req_cpu_m for p in gfw), sum(p.req_cpu_m for p in ds), sum(p.req_cpu_m for p in oth),
            sum(p.req_mem_b for p in gfw), sum(p.req_mem_b for p in ds), sum(p.req_mem_b for p in oth)
        )
        
        sum_usage_cpu = sum((getattr(p, "usage_cpu_m", 0) or 0) for p in raw_pods_by_node_export.get(node_name, []))
        sum_usage_mem = sum((getattr(p, "usage_mem_b", 0) or 0) for p in raw_pods_by_node_export.get(node_name, []))
        
        cost_daily, missing = costs.node_daily_cost_from_instance(inst_type, pool_name)
        
        nodes_table.append(NodeRow(
            node=node_name, nodepool=pool_name, instance=inst_type,
            gfw_ratio_pct=(len(gfw)/len(pods)*100 if pods else 0),
            alloc_cpu_m=alloc_cpu, alloc_mem_b=alloc_mem,
            sum_req_cpu_m=parts.gfw_cpu_m+parts.ds_cpu_m+parts.other_cpu_m,
            sum_req_mem_b=parts.gfw_mem_b+parts.ds_mem_b+parts.other_mem_b,
            sum_usage_cpu_m=int(sum_usage_cpu), sum_usage_mem_b=int(sum_usage_mem),
            ram_util_pct=(parts.gfw_mem_b+parts.ds_mem_b+parts.other_mem_b)/alloc_mem*100 if alloc_mem else 0,
            ram_ds_gib=parts.ds_mem_b/1073741824, ram_gfw_gib=parts.gfw_mem_b/1073741824,
            cost_daily_usd=cost_daily, parts=parts, is_virtual=bool(getattr(node, "is_virtual", False)), price_missing=missing
        ))

    return SimulationResult(
        nodes_table=nodes_table, pods_by_node=pods_by_node_export,
        total_cost_daily_usd=sum(actual_costs_from_hist.values()),
        pool_costs_usd=actual_costs_from_hist,
        projected_pool_costs_usd=projected_pool_costs,
        projected_total_cost_usd=projected_total,
        total_cost_gfw_nodes_usd=0.0,
        total_cost_keda_nodes_usd=0.0,
    )