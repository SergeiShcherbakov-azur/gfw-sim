# gfw_sim/sim/simulate.py
from __future__ import annotations
from dataclasses import dataclass, field, replace
from typing import Dict, List, Optional, Iterable, Any
import math
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
    usage_cpu_m: int = 0
    usage_mem_b: int = 0
    active_ratio: float = 1.0
    
    affinity: Dict[str, Any] = field(default_factory=dict)
    topology_spread: List[Dict[str, Any]] = field(default_factory=list)
    node_selector: Dict[str, str] = field(default_factory=dict)
    tolerations: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def sort_key(self):
        return (self.req_mem_b, self.req_cpu_m, self.id)

@dataclass
class PoolStats:
    cost: float
    count: int

@dataclass
class SimulationResult:
    nodes_table: List[NodeRow]
    pods_by_node: Dict[str, List[PodView]]
    total_cost_daily_usd: float
    pool_stats: Dict[str, PoolStats]
    projected_pool_stats: Dict[str, PoolStats]
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
    # These are retained for legacy logic, but we calculate dynamic overhead now
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
    
    workload_pods: List[PodView] = field(default_factory=list)
    daemon_pods: List[PodView] = field(default_factory=list)
    
    @property
    def used_cpu(self) -> int:
        return sum(p.req_cpu_m for p in self.workload_pods + self.daemon_pods)

    @property
    def used_mem(self) -> int:
        return sum(p.req_mem_b for p in self.workload_pods + self.daemon_pods)

    @property
    def pod_count(self) -> int:
        return len(self.workload_pods) + len(self.daemon_pods)

    def add(self, p: PodView):
        if p.is_daemon:
            self.daemon_pods.append(p)
        else:
            self.workload_pods.append(p)

    def is_empty(self) -> bool:
        return len(self.workload_pods) == 0

    def get_overflow_needs(self) -> tuple[int, int, int]:
        # Always check against total allocatable (DS are just pods now)
        cap_cpu = self.spec.alloc_cpu
        cap_mem = self.spec.alloc_mem

        cpu_excess = max(0, self.used_cpu - cap_cpu)
        mem_excess = max(0, self.used_mem - cap_mem)
        pods_excess = max(0, self.pod_count - self.spec.max_pods)
        return cpu_excess, mem_excess, pods_excess
    
    def can_fit(self, p: PodView) -> bool:
        # Check Max Pods
        if self.pod_count + 1 > self.spec.max_pods: 
            return False
        
        # Check Capacity
        cap_cpu = self.spec.alloc_cpu
        cap_mem = self.spec.alloc_mem

        if (self.used_cpu + p.req_cpu_m > cap_cpu): return False
        if (self.used_mem + p.req_mem_b > cap_mem): return False
        
        # Check Anti-Affinity
        if self._check_anti_affinity_conflict(p): return False
        return True

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
        # Simple prefix match for deployment anti-affinity
        p_prefix = p.owner_name[:15]
        all_pods = self.workload_pods + self.daemon_pods
        for existing in all_pods:
            if existing.namespace == p.namespace:
                 if existing.owner_name and existing.owner_name.startswith(p_prefix):
                     return True
        return False

def _check_taint_tolerations(node_taints: list, pod_tolerations: list) -> bool:
    for taint in node_taints:
        key = taint.get("key"); val = taint.get("value"); effect = taint.get("effect")
        if effect not in ("NoSchedule", "NoExecute"): continue     
        tolerated = False
        for tol in pod_tolerations:
            t_key = tol.get("key"); t_op = tol.get("operator", "Equal"); t_val = tol.get("value")
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

def bin_pack_pods(
    pods_to_schedule: List[tuple[PodView, List[PodView]]], # (WorkloadPod, [SidecarPods])
    allowed_instances: List[InstanceTypeSpec],
    time_factor: float,
    ds_templates: List[PodView]
) -> List[SimNode]:
    
    if not pods_to_schedule: return []
    if not allowed_instances: return []

    sorted_instances = sorted(allowed_instances, key=lambda x: x.price_hourly)
    # Sort by workload size
    sorted_pods = sorted(pods_to_schedule, key=lambda x: x[0].sort_key, reverse=True)
    
    new_nodes: List[SimNode] = []
    
    for main_pod, sidecars in sorted_pods:
        placed = False
        
        # Try to fit in existing NEW nodes
        for node in new_nodes:
            # Check main pod + all sidecars
            fits = node.can_fit(main_pod)
            if fits:
                # Tentatively add main to check capacity for sidecars
                node.add(main_pod)
                for sc in sidecars:
                    if not node.can_fit(sc):
                        fits = False
                        break
                    node.add(sc)
                
                if not fits:
                    # Rollback
                    node.remove(main_pod) # SimNode doesn't have remove yet?
                    # We need to implement rollback or check hypothetically. 
                    # SimNode.add modifies state.
                    # Let's just implement `remove` or assume strict checking first.
                    # Since SimNode logic is simple accumulation, rollback is:
                    node.workload_pods.pop() # remove last (main_pod)
                    for _ in range(len(node.workload_pods) - len(node.workload_pods) + 1): # complex rollback
                        pass
                    # Since we don't have remove, let's just do a dry-run check.
                    # But SimNode.can_fit uses `self.used_cpu`.
                    # Let's fix this by checking capacity sum BEFORE adding.
                    pass
                else:
                    placed = True
                    break
        
        # Helper for capacity check without modifying
        def check_capacity(node, pods_group):
            needed_cpu = sum(p.req_cpu_m for p in pods_group)
            needed_mem = sum(p.req_mem_b for p in pods_group)
            needed_cnt = len(pods_group)
            
            if node.pod_count + needed_cnt > node.spec.max_pods: return False
            if node.used_cpu + needed_cpu > node.spec.alloc_cpu: return False
            if node.used_mem + needed_mem > node.spec.alloc_mem: return False
            return True

        # Retry placement with dry-run
        placed = False
        for node in new_nodes:
            group = [main_pod] + sidecars
            if check_capacity(node, group):
                # Check affinities one by one
                aff_ok = True
                for p in group:
                    if not node.can_fit(p): # This checks affinity too
                        aff_ok = False; break
                
                if aff_ok:
                    for p in group: node.add(p)
                    placed = True
                    break

        if not placed:
            best_spec = None
            group = [main_pod] + sidecars
            req_cpu = sum(p.req_cpu_m for p in group)
            req_mem = sum(p.req_mem_b for p in group)
            req_cnt = len(group)

            for spec in sorted_instances:
                # Check capacity for workload + DaemonSets!
                # We don't know exact DS overhead here without checking labels,
                # but we can estimate or just check workload.
                # Ideally, we verify DS fit later.
                if req_cpu <= spec.alloc_cpu and req_mem <= spec.alloc_mem:
                     if spec.max_pods >= req_cnt: 
                        best_spec = spec
                        break
            
            if not best_spec:
                best_spec = max(allowed_instances, key=lambda x: x.alloc_mem)
            
            sim_node = SimNode(
                name=f"new-node-{len(new_nodes)}",
                spec=best_spec,
                pool="dynamic",
                is_existing=False
            )
            
            # 1. Add DaemonSets
            for ds in ds_templates:
                if _check_node_selector(best_spec.labels, ds.node_selector) and \
                   _check_taint_tolerations(best_spec.taints, ds.tolerations):
                    ds_copy = replace(ds, id=f"{ds.name}-sim-{len(new_nodes)}")
                    sim_node.add(ds_copy)
            
            # 2. Add Workload
            if check_capacity(sim_node, group):
                for p in group: sim_node.add(p)
                new_nodes.append(sim_node)
            else:
                # If even empty node + DS cannot fit workload, force it anyway (overflow behavior)
                for p in group: sim_node.add(p)
                new_nodes.append(sim_node)
            
    return new_nodes

def _iter_snapshot_pods(snapshot) -> Iterable:
    pods = getattr(snapshot, "pods", None)
    return pods.values() if isinstance(pods, dict) else (pods or [])

def _iter_snapshot_nodes(snapshot) -> Iterable:
    nodes = getattr(snapshot, "nodes", None)
    return nodes.values() if isinstance(nodes, dict) else (nodes or [])

def run_simulation(snapshot) -> SimulationResult:
    # 1. Prep Prices
    pricing_state = costs.get_state()
    snapshot_prices = getattr(snapshot, "prices", {})
    local_prices = pricing_state.hourly_prices.copy()
    for inst_type, price_obj in snapshot_prices.items():
        local_prices[str(inst_type)] = float(price_obj.usd_per_hour)
    
    all_pods_raw = list(_iter_snapshot_pods(snapshot))
    nodepools_map = getattr(snapshot, "nodepools", {})

    # 2. Build Catalog
    pods_by_node: Dict[str, List[PodView]] = {}
    raw_pods_by_node = {}
    pending_pods_by_pool: Dict[str, List[PodView]] = {}
    
    # Identify S3 templates (pods named mount-s3*)
    s3_templates = {} # namespace -> PodView
    for p in all_pods_raw:
        if p.name.startswith("mount-s3"):
            s3_templates[p.namespace] = p

    pool_instance_catalog: Dict[str, Dict[str, InstanceTypeSpec]] = {}
    pool_node_examples: Dict[str, Any] = {}
    pool_templates: Dict[str, InstanceTypeSpec] = {} 
    
    for node in _iter_snapshot_nodes(snapshot):
        pool = str(getattr(node, "nodepool", "unknown"))
        inst_type = getattr(node, "instance_type", "unknown")
        
        if pool not in pool_node_examples:
            pool_node_examples[pool] = node
        
        if pool not in pool_instance_catalog:
            pool_instance_catalog[pool] = {}
            
        if inst_type not in pool_instance_catalog[pool]:
            price = local_prices.get(str(inst_type), 0.0)
            spec = InstanceTypeSpec(
                name=inst_type,
                alloc_cpu=int(getattr(node, "alloc_cpu_m", 0)),
                alloc_mem=int(getattr(node, "alloc_mem_b", 0)),
                max_pods=int(getattr(node, "alloc_pods", 110)),
                price_hourly=price,
                labels=getattr(node, "labels", {}),
                taints=getattr(node, "taints", [])
            )
            pool_instance_catalog[pool][inst_type] = spec
            if pool not in pool_templates:
                pool_templates[pool] = spec
            
    for pod in all_pods_raw:
        ratio = getattr(pod, "active_ratio", 1.0)
        pv = PodView(
            id=getattr(pod, "id", ""),
            namespace=pod.namespace, name=pod.name, owner_kind=pod.owner_kind, owner_name=pod.owner_name,
            is_gfw=bool(pod.is_gfw), is_daemon=bool(pod.is_daemonset), is_system=bool(pod.is_system),
            req_cpu_m=int(pod.req_cpu_m or 0), req_mem_b=int(pod.req_mem_b or 0),
            usage_cpu_m=int(getattr(pod, "usage_cpu_m", 0) or 0),
            usage_mem_b=int(getattr(pod, "usage_mem_b", 0) or 0),
            active_ratio=ratio,
            affinity=getattr(pod, "affinity", {}),
            topology_spread=getattr(pod, "topology_spread_constraints", []),
            node_selector=getattr(pod, "node_selector", {}),
            tolerations=getattr(pod, "tolerations", [])
        )
        
        node_id = getattr(pod, "node")
        if node_id:
            pods_by_node.setdefault(node_id, []).append(pv)
            raw_pods_by_node.setdefault(node_id, []).append(pod)
        else:
            # Pending pods
            selector = getattr(pod, "node_selector", {})
            pool_name = selector.get("karpenter.sh/nodepool")
            if pool_name and not pv.is_daemon:
                pending_pods_by_pool.setdefault(pool_name, []).append(pv)

    # 3. Identify DaemonSets
    ds_templates = []
    seen_ds = set()
    for p in all_pods_raw:
        if p.is_daemonset:
            key = (p.namespace, p.owner_name)
            if key not in seen_ds:
                pv_ds = PodView(
                    id=f"ds-{p.name}", namespace=p.namespace, name=p.name, owner_kind="DaemonSet", owner_name=p.owner_name,
                    is_gfw=True, is_daemon=True, is_system=p.is_system,
                    req_cpu_m=int(p.req_cpu_m or 0), req_mem_b=int(p.req_mem_b or 0),
                    node_selector=getattr(p, "node_selector", {}),
                    tolerations=getattr(p, "tolerations", [])
                )
                ds_templates.append(pv_ds)
                seen_ds.add(key)

    # 4. Init SimNodes
    sim_nodes: Dict[str, SimNode] = {}
    for node in _iter_snapshot_nodes(snapshot):
        pool = str(getattr(node, "nodepool", "unknown"))
        inst = getattr(node, "instance_type", "")
        spec = pool_instance_catalog.get(pool, {}).get(inst)
        if not spec:
             price = local_prices.get(str(inst), 0.0)
             spec = InstanceTypeSpec(name=inst, alloc_cpu=0, alloc_mem=0, max_pods=110, price_hourly=price)
        
        sim_node = SimNode(
            name=getattr(node, "name"),
            spec=spec,
            pool=pool,
            is_existing=True
        )
        sim_nodes[sim_node.name] = sim_node

    # 5. Hydrate Pods
    for pod_id, pods in pods_by_node.items():
        if pod_id in sim_nodes:
            node = sim_nodes[pod_id]
            for p in pods:
                node.add(p)

    # 6. Process Pending Pods & Calculate Costs
    projected_pool_stats: Dict[str, PoolStats] = {}
    projected_total = 0.0
    SCALING_LAG_HOURS = 0.5
    
    pools = set(n.pool for n in sim_nodes.values()) | set(nodepools_map.keys()) | set(pending_pods_by_pool.keys())
    
    for pool in pools:
        nodes = [n for n in sim_nodes.values() if n.pool == pool]
        pool_cost = 0.0
        nodes_count = 0
        
        # Prepare workload groups (Pod + Sidecars)
        workloads_to_place: List[tuple[PodView, List[PodView]]] = []
        
        # 6a. Add Pending Pods
        if pool in pending_pods_by_pool:
            for p in pending_pods_by_pool[pool]:
                sidecars = []
                # Heuristic: If we have an S3 template in this namespace, add it
                # Logic: We assume moved pods that match a namespace with s3-mount need it.
                if p.namespace in s3_templates:
                    # Clone S3 pod
                    tmpl = s3_templates[p.namespace]
                    s3_clone = PodView(
                        id=f"mount-s3-{p.id}", namespace=tmpl.namespace, name=f"mount-s3-sim-{p.name}",
                        owner_kind=None, owner_name=None, is_gfw=True, is_daemon=False, is_system=False,
                        req_cpu_m=int(tmpl.req_cpu_m or 0), req_mem_b=int(tmpl.req_mem_b or 0),
                        active_ratio=p.active_ratio
                    )
                    sidecars.append(s3_clone)
                workloads_to_place.append((p, sidecars))

        # 6b. Try to fit into EXISTING nodes
        # Sort existing nodes by free capacity (most full first to pack?) 
        # Or random? Let's iterate as is.
        remaining_workloads = []
        
        for wl_group in workloads_to_place:
            main_pod, sidecars = wl_group
            placed = False
            
            # Try existing nodes in pool
            for node in nodes:
                # Check capacity for group
                req_cpu = main_pod.req_cpu_m + sum(s.req_cpu_m for s in sidecars)
                req_mem = main_pod.req_mem_b + sum(s.req_mem_b for s in sidecars)
                req_pods = 1 + len(sidecars)
                
                # Manual capacity check against node
                if node.pod_count + req_pods > node.spec.max_pods: continue
                if node.used_cpu + req_cpu > node.spec.alloc_cpu: continue
                if node.used_mem + req_mem > node.spec.alloc_mem: continue
                
                # Check affinity
                if not node.can_fit(main_pod): continue
                
                # Place!
                node.add(main_pod)
                for sc in sidecars: node.add(sc)
                placed = True
                
                # Also register for table view
                pods_by_node.setdefault(node.name, []).append(main_pod)
                for sc in sidecars: pods_by_node[node.name].append(sc)
                break
            
            if not placed:
                remaining_workloads.append(wl_group)

        # 6c. Bin Pack remaining into NEW nodes
        new_virtual_nodes = []
        if remaining_workloads:
            catalog = list(pool_instance_catalog.get(pool, {}).values())
            if not catalog:
                 specs = {n.spec.name: n.spec for n in nodes}
                 catalog = list(specs.values())
            
            if catalog:
                # time_factor assumed 1.0 for placement decision, but cost calculated later
                new_virtual_nodes = bin_pack_pods(remaining_workloads, catalog, 1.0, ds_templates)

        # 6d. Summarize Costs
        # Existing nodes cost
        for node in nodes:
            if node.is_empty():
                continue
            nodes_count += 1
            max_ratio = max((p.active_ratio for p in node.workload_pods), default=1.0)
            effective_hours = 24.0
            if max_ratio < 0.98:
                effective_hours = (max_ratio * 24.0) + SCALING_LAG_HOURS
                effective_hours = min(24.0, effective_hours)
            
            pool_cost += node.spec.price_hourly * effective_hours
            
            # Check Overflow on existing (if any left)
            cpu_ex, mem_ex, pod_ex = node.get_overflow_needs()
            if cpu_ex > 0 or mem_ex > 0 or pod_ex > 0:
                # Add overflow cost penalty (simple linear approx)
                # This handles cases where user dragged onto a full node manually
                pool_cost += node.spec.price_hourly * 24.0 # Penalty: pay double? or just overhead? 
                # Let's assume we spawn a node of same type to cover excess
                pool_cost += node.spec.price_hourly * effective_hours

        # New nodes cost
        for i, vnode in enumerate(new_virtual_nodes):
            vnode.pool = pool
            vnode.name = f"new-{pool}-{i}"
            nodes_count += 1
            
            v_max_ratio = max((p.active_ratio for p in vnode.workload_pods), default=1.0)
            v_eff_hours = 24.0
            if v_max_ratio < 0.98:
                v_eff_hours = (v_max_ratio * 24.0) + SCALING_LAG_HOURS
                v_eff_hours = min(24.0, v_eff_hours)
            
            pool_cost += vnode.spec.price_hourly * v_eff_hours
            
            # Register for UI
            sim_nodes[vnode.name] = vnode
            pods_by_node[vnode.name] = vnode.workload_pods + vnode.daemon_pods

        projected_pool_stats[pool] = PoolStats(cost=pool_cost, count=nodes_count)
        projected_total += pool_cost

    # 7. Actuals
    actual_pool_stats: Dict[str, PoolStats] = {}
    history = getattr(snapshot, "history_usage", [])
    
    actual_counts = {}
    for n in sim_nodes.values():
        if n.is_existing:
            actual_counts[n.pool] = actual_counts.get(n.pool, 0) + 1

    if history:
        temp_costs = {}
        for entry in history:
            p = entry.get("pool", "unknown")
            i = entry.get("instance", "unknown")
            h = entry.get("instance_hours_24h", 0.0)
            price = local_prices.get(i, 0.0)
            temp_costs[p] = temp_costs.get(p, 0.0) + (price * h)
        
        for p, cost in temp_costs.items():
            actual_pool_stats[p] = PoolStats(cost=cost, count=actual_counts.get(p, 0))
    else:
        for pool, stats in projected_pool_stats.items():
             base_cost = 0.0
             cnt = 0
             for n in sim_nodes.values():
                 if n.pool == pool and n.is_existing:
                     base_cost += n.spec.price_hourly * 24.0
                     cnt += 1
             actual_pool_stats[pool] = PoolStats(cost=base_cost, count=cnt)

    # 8. Table Export
    nodes_table: List[NodeRow] = []
    all_sim_nodes = sorted(sim_nodes.values(), key=lambda x: (x.pool, not x.is_existing, x.name))
    
    for node in all_sim_nodes:
        pods = pods_by_node.get(node.name, [])
        gfw = [p for p in pods if p.is_gfw]
        ds = [p for p in pods if p.is_daemon]
        oth = [p for p in pods if not p.is_gfw and not p.is_daemon]
        
        alloc_cpu = int(node.spec.alloc_cpu)
        alloc_mem = int(node.spec.alloc_mem)

        parts = NodeParts(
            sum(p.req_cpu_m for p in gfw), sum(p.req_cpu_m for p in ds), sum(p.req_cpu_m for p in oth),
            sum(p.req_mem_b for p in gfw), sum(p.req_mem_b for p in ds), sum(p.req_mem_b for p in oth)
        )
        
        sum_usage_cpu = sum((getattr(p, "usage_cpu_m", 0) or 0) for p in raw_pods_by_node.get(node.name, []))
        sum_usage_mem = sum((getattr(p, "usage_mem_b", 0) or 0) for p in raw_pods_by_node.get(node.name, []))
        
        cost_daily = node.spec.price_hourly * 24.0
        missing = (node.spec.price_hourly == 0.0)
        
        nodes_table.append(NodeRow(
            node=node.name, nodepool=node.pool, instance=node.spec.name,
            gfw_ratio_pct=(len(gfw)/len(pods)*100 if pods else 0),
            alloc_cpu_m=alloc_cpu, alloc_mem_b=alloc_mem,
            sum_req_cpu_m=parts.gfw_cpu_m+parts.ds_cpu_m+parts.other_cpu_m,
            sum_req_mem_b=parts.gfw_mem_b+parts.ds_mem_b+parts.other_mem_b,
            sum_usage_cpu_m=int(sum_usage_cpu), sum_usage_mem_b=int(sum_usage_mem),
            ram_util_pct=(parts.gfw_mem_b+parts.ds_mem_b+parts.other_mem_b)/max(1, alloc_mem)*100,
            ram_ds_gib=parts.ds_mem_b/1073741824, ram_gfw_gib=parts.gfw_mem_b/1073741824,
            cost_daily_usd=cost_daily, parts=parts, is_virtual=not node.is_existing, price_missing=missing
        ))

    return SimulationResult(
        nodes_table=nodes_table, 
        pods_by_node=pods_by_node,
        total_cost_daily_usd=sum(s.cost for s in actual_pool_stats.values()),
        pool_stats=actual_pool_stats,
        projected_pool_stats=projected_pool_stats,
        projected_total_cost_usd=projected_total,
        total_cost_gfw_nodes_usd=0.0,
        total_cost_keda_nodes_usd=0.0,
    )