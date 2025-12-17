# gfw_sim/sim/packing.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Iterable

from ..model.entities import Snapshot, Node, Pod
from ..types import NodeId, PodId, NodePoolName
from .costs import node_daily_cost


@dataclass
class NodeUsage:
    """Текущее использование ресурсов на ноде (по requests)."""
    cpu_m: int = 0
    mem_b: int = 0


def _compute_initial_usage(
    snapshot: Snapshot,
    pods_to_move: Iterable[PodId],
) -> Dict[NodeId, NodeUsage]:
    pods_to_move_set = set(pods_to_move)
    usage: Dict[NodeId, NodeUsage] = {}

    for node_id in snapshot.nodes.keys():
        usage[node_id] = NodeUsage()

    for pod_id, pod in snapshot.pods.items():
        if pod.node is None:
            continue
        if pod_id in pods_to_move_set:
            continue
        node_id = pod.node
        u = usage.setdefault(node_id, NodeUsage())
        u.cpu_m += int(pod.req_cpu_m)
        u.mem_b += int(pod.req_mem_b)

    return usage


def _can_schedule_on_node(pod: Pod, node: Node, usage: NodeUsage) -> bool:
    needed_cpu = int(pod.req_cpu_m)
    needed_mem = int(pod.req_mem_b)

    if needed_cpu <= 0 and needed_mem <= 0:
        return True

    if usage.cpu_m + needed_cpu > int(node.alloc_cpu_m):
        return False
    if usage.mem_b + needed_mem > int(node.alloc_mem_b):
        return False

    return True


def _choose_template_for_pool(snapshot: Snapshot, pool: NodePoolName) -> Optional[Node]:
    candidates = [n for n in snapshot.nodes.values() if n.nodepool == pool]
    if not candidates:
        return None

    nodepools = snapshot.nodepools
    schedules = snapshot.schedules
    prices = snapshot.prices

    from ..model.entities import Schedule
    default_schedule = schedules.get("default", Schedule(name="default"))

    def node_cost_per_day(n: Node) -> float:
        pool_obj = nodepools.get(n.nodepool)
        schedule_name = pool_obj.schedule_name if pool_obj is not None else "default"
        schedule = schedules.get(schedule_name, default_schedule)
        return node_daily_cost(n, schedule, prices)

    return min(candidates, key=node_cost_per_day)


def move_pods_to_pool(
    snapshot: Snapshot,
    pod_ids: List[PodId],
    target_pool: NodePoolName,
) -> Snapshot:
    if not pod_ids:
        return snapshot

    pods_to_move_set = set(pod_ids)

    # Копируем ноды
    new_nodes: Dict[NodeId, Node] = {}
    for node_id, node in snapshot.nodes.items():
        new_nodes[node_id] = Node(
            id=node.id,
            name=node.name,
            nodepool=node.nodepool,
            instance_type=node.instance_type,
            alloc_cpu_m=node.alloc_cpu_m,
            alloc_mem_b=node.alloc_mem_b,
            capacity_type=node.capacity_type,
            labels=dict(node.labels),
            taints=list(node.taints),
            is_virtual=node.is_virtual,
        )

    # Копируем pod'ы, для переносимых сбрасываем node, но сохраняем все поля
    new_pods: Dict[PodId, Pod] = {}
    for pod_id, pod in snapshot.pods.items():
        if pod_id in pods_to_move_set:
            new_pods[pod_id] = Pod(
                id=pod.id,
                name=pod.name,
                namespace=pod.namespace,
                node=None,
                owner_kind=pod.owner_kind,
                owner_name=pod.owner_name,
                req_cpu_m=pod.req_cpu_m,
                req_mem_b=pod.req_mem_b,
                limit_cpu_m=pod.limit_cpu_m,
                limit_mem_b=pod.limit_mem_b,
                is_daemonset=pod.is_daemonset,
                is_system=pod.is_system,
                is_gfw=pod.is_gfw,
                tolerations=list(pod.tolerations),
                node_selector=dict(pod.node_selector),
                affinity=dict(pod.affinity),
            )
        else:
            new_pods[pod_id] = pod

    usage = _compute_initial_usage(snapshot, pods_to_move_set)

    def create_virtual_node(template: Node, index: int) -> Node:
        base_name = template.name
        new_name = f"{base_name}-virt-{index}"
        new_id = NodeId(new_name)
        return Node(
            id=new_id,
            name=new_name,
            nodepool=template.nodepool,
            instance_type=template.instance_type,
            alloc_cpu_m=template.alloc_cpu_m,
            alloc_mem_b=template.alloc_mem_b,
            capacity_type=template.capacity_type,
            labels=dict(template.labels),
            taints=list(template.taints),
            is_virtual=True,
        )

    virtual_counters: Dict[NodePoolName, int] = {}

    template = _choose_template_for_pool(snapshot, target_pool)
    if template is None:
        raise ValueError(
            f"No nodes found in pool {target_pool}, cannot derive template"
        )

    # Упаковка pod'ов в целевой пул
    for pod_id in pod_ids:
        pod = new_pods[pod_id]

        best_node_id: Optional[NodeId] = None
        best_score: Optional[float] = None

        for node_id, node in new_nodes.items():
            if node.nodepool != target_pool:
                continue
            u = usage.setdefault(node_id, NodeUsage())
            if not _can_schedule_on_node(pod, node, u):
                continue

            remaining_cpu = int(node.alloc_cpu_m) - (u.cpu_m + int(pod.req_cpu_m))
            remaining_mem = int(node.alloc_mem_b) - (u.mem_b + int(pod.req_mem_b))

            score = remaining_cpu + remaining_mem / 1024.0

            if best_score is None or score < best_score:
                best_score = score
                best_node_id = node_id

        if best_node_id is None:
            # создаём виртуальную ноду
            counter = virtual_counters.get(target_pool, 0) + 1
            virtual_counters[target_pool] = counter
            new_node = create_virtual_node(template, counter)
            new_nodes[new_node.id] = new_node
            usage[new_node.id] = NodeUsage()
            best_node_id = new_node.id

        pod.node = best_node_id
        u = usage[best_node_id]
        u.cpu_m += int(pod.req_cpu_m)
        u.mem_b += int(pod.req_mem_b)

    new_snapshot = Snapshot(
        nodes=new_nodes,
        pods=new_pods,
        nodepools=snapshot.nodepools,
        prices=snapshot.prices,
        schedules=snapshot.schedules,
        keda_pool_name=snapshot.keda_pool_name,
    )
    return new_snapshot
