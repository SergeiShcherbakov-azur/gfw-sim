# gfw_sim/sim/selector.py
from __future__ import annotations

from typing import List

from ..model.entities import Snapshot, Pod
from ..types import PodId, Namespace, NodeId


def _pod_matches_flags(
    p: Pod,
    include_system: bool,
    include_daemonsets: bool,
) -> bool:
    """Фильтрация pod'ов по системности/daemonset.

    По умолчанию (оба False) возвращаем только "обычные" workload-поды.
    """
    if not include_system and p.is_system:
        return False
    if not include_daemonsets and p.is_daemonset:
        return False
    return True


def select_pods_by_namespace(
    snapshot: Snapshot,
    namespace: str | Namespace,
    include_system: bool = False,
    include_daemonsets: bool = False,
) -> List[PodId]:
    """Возвращает pod_id всех pod'ов в namespace с учётом фильтров."""
    ns_str = str(namespace)
    result: List[PodId] = []
    for pod_id, p in snapshot.pods.items():
        if str(p.namespace) != ns_str:
            continue
        if not _pod_matches_flags(p, include_system, include_daemonsets):
            continue
        result.append(pod_id)
    return result


def select_pods_by_owner(
    snapshot: Snapshot,
    namespace: str | Namespace,
    owner_kind: str,
    owner_name: str,
    include_system: bool = False,
    include_daemonsets: bool = False,
) -> List[PodId]:
    """Выбор pod'ов по (namespace, owner_kind, owner_name).

    Подходит для Deployment/StatefulSet/ReplicaSet и т.п.
    """
    ns_str = str(namespace)
    owner_kind = owner_kind.lower()
    owner_name = owner_name
    result: List[PodId] = []
    for pod_id, p in snapshot.pods.items():
        if str(p.namespace) != ns_str:
            continue
        if p.owner_kind is None:
            continue
        if p.owner_kind.lower() != owner_kind:
            continue
        if p.owner_name != owner_name:
            continue
        if not _pod_matches_flags(p, include_system, include_daemonsets):
            continue
        result.append(pod_id)
    return result


def select_pods_by_node(
    snapshot: Snapshot,
    node_name: str,
    include_system: bool = False,
    include_daemonsets: bool = False,
) -> List[PodId]:
    """Подбирает pod'ы, живущие на заданной ноде по имени node.name."""
    # Сначала найдём NodeId по имени
    node_id: NodeId | None = None
    for nid, n in snapshot.nodes.items():
        if n.name == node_name:
            node_id = nid
            break
    if node_id is None:
        return []

    result: List[PodId] = []
    for pod_id, p in snapshot.pods.items():
        if p.node != node_id:
            continue
        if not _pod_matches_flags(p, include_system, include_daemonsets):
            continue
        result.append(pod_id)
    return result
