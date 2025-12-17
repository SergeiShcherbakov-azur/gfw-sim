# gfw_sim/sim/operations.py
from __future__ import annotations

from typing import Iterable, List, Sequence, Optional, Dict, Any
from copy import deepcopy

from .packing import move_pods_to_pool
from ..model.entities import Pod, CpuMillis, Bytes, Snapshot


# ---------------------------------------------------------------------------
# Вспомогательные функции классификации подов и чистки нод
# ---------------------------------------------------------------------------


def _is_workload_pod(pod) -> bool:
    """
    Рабочий под — это не system и не daemonset.
    Такие поды «держат» ноду от выключения.
    """
    return not getattr(pod, "is_system", False) and not getattr(pod, "is_daemonset", False)


def _cleanup_empty_nodes(snapshot) -> None:
    """
    Удаляет из snapshot ноды, на которых не осталось ни одного рабочего пода.

    Правило:
      - если на ноде есть хотя бы один pod с not is_system and not is_daemonset → нода остаётся;
      - если на ноде только system/daemonset-поды (или вообще нет подов) → нода считается выключенной:
          * удаляем ноду из snapshot.nodes
          * удаляем system/daemonset-поды на этой ноде из snapshot.pods
    """
    nodes = getattr(snapshot, "nodes", None)
    pods = getattr(snapshot, "pods", None)
    if nodes is None or pods is None:
        return

    # Собираем поды по нодам
    pods_by_node: dict[str, list[tuple[str, object]]] = {}
    for pod_id, pod in list(pods.items()):
        node_name = getattr(pod, "node", None)
        if not node_name:
            continue
        pods_by_node.setdefault(node_name, []).append((pod_id, pod))

    # Определяем ноды, которые можно удалить
    to_delete_nodes: list[str] = []
    for node_name, node in list(nodes.items()):
        pod_entries = pods_by_node.get(node_name, [])

        # Нет подов вообще -> ноду можно удалять
        if not pod_entries:
            to_delete_nodes.append(node_name)
            continue

        # Есть ли рабочие поды?
        has_workload = any(_is_workload_pod(p) for _pid, p in pod_entries)
        if not has_workload:
            # Только system/daemonset-поды -> ноду можно удалять
            to_delete_nodes.append(node_name)

    # Удаляем ноды и их system/daemonset-поды
    for node_name in to_delete_nodes:
        nodes.pop(node_name, None)

        # Удаляем system/daemonset-поды, привязанные к этой ноде
        for pod_id, pod in list(pods.items()):
            if getattr(pod, "node", None) != node_name:
                continue
            if getattr(pod, "is_system", False) or getattr(pod, "is_daemonset", False):
                pods.pop(pod_id, None)
            else:
                # Теоретически сюда не попадём, но если вдруг –
                # обнулим node, чтобы не было ссылки на несуществующую ноду.
                setattr(pod, "node", None)


# ---------------------------------------------------------------------------
# Сбор идентификаторов подов
# ---------------------------------------------------------------------------


def _collect_pods_by_node(snapshot, node_name: str) -> List[str]:
    """Собирает pod_id для всех подов на ноде."""
    pods = getattr(snapshot, "pods", {})
    result: List[str] = []
    for pod_id, pod in pods.items():
        if getattr(pod, "node", None) == node_name:
            result.append(pod_id)
    return result


def _collect_pods_by_namespace(snapshot, namespace: str) -> List[str]:
    """Собирает pod_id для всех подов в namespace."""
    pods = getattr(snapshot, "pods", {})
    result: List[str] = []
    for pod_id, pod in pods.items():
        if getattr(pod, "namespace", None) == namespace:
            result.append(pod_id)
    return result


def _collect_pods_by_owner(snapshot, namespace: str, owner_name: str) -> List[str]:
    """Собирает pod_id для всех подов owner'а (deployment/statefulset) в namespace."""
    pods = getattr(snapshot, "pods", {})
    result: List[str] = []
    for pod_id, pod in pods.items():
        if getattr(pod, "namespace", None) != namespace:
            continue
        if getattr(pod, "owner_name", None) == owner_name:
            result.append(pod_id)
    return result


def _filter_workload_pods(snapshot, pod_ids: Iterable[str]) -> List[str]:
    """
    Отбрасывает системные и daemonset-поды — их не переносим, они живут «поверх» нод.
    """
    pods = getattr(snapshot, "pods", {})
    result: List[str] = []
    for pod_id in pod_ids:
        pod = pods.get(pod_id)
        if pod is None:
            continue
        if _is_workload_pod(pod):
            result.append(pod_id)
    return result


def _filter_pods_for_move(
    snapshot,
    pod_ids: Iterable[str],
    include_system: bool,
    include_daemonsets: bool,
) -> List[str]:
    """
    Фильтрует список pod_ids в зависимости от флагов include_system/include_daemonsets.
    """
    pods = getattr(snapshot, "pods", {})
    result: List[str] = []
    for pod_id in pod_ids:
        pod = pods.get(pod_id)
        if pod is None:
            continue

        is_system = bool(getattr(pod, "is_system", False))
        is_ds = bool(getattr(pod, "is_daemonset", False))

        if is_ds:
            if include_daemonsets:
                result.append(pod_id)
            continue

        if is_system:
            if include_system:
                result.append(pod_id)
            continue

        # обычный workload-под
        result.append(pod_id)

    return result


def _normalize_pool_name(target_pool: str) -> str:
    """
    Нормализует имя пула.
    """
    if not target_pool:
        return target_pool
    parts = str(target_pool).split()
    return parts[-1]


# ---------------------------------------------------------------------------
# Патчинг подов (NEW)
# ---------------------------------------------------------------------------

def patch_pods_in_snapshot(
    snapshot: Snapshot,
    pod_ids: Iterable[str],
    req_cpu_m: Optional[int] = None,
    req_mem_b: Optional[int] = None,
    tolerations: Optional[List[Dict[str, Any]]] = None,
    node_selector: Optional[Dict[str, str]] = None,
    affinity: Optional[Dict[str, Any]] = None,
) -> Snapshot:
    """
    Создает новый снапшот, в котором указанные поды имеют обновленные поля.
    Используется перед переносом, чтобы "исправить" ресурсы или scheduling constraints.
    """
    pods_to_patch = set(pod_ids)
    if not pods_to_patch:
        return snapshot

    # Копируем структуру подов, так как Snapshot частично мутабелен в текущей реализации,
    # но лучше перестраховаться и сделать shallow copy словаря + deepcopy изменяемых подов.
    new_pods = dict(snapshot.pods)
    
    for pid in pods_to_patch:
        original = new_pods.get(pid)
        if not original:
            continue
        
        # Полная копия объекта пода
        p = deepcopy(original)
        
        if req_cpu_m is not None:
            p.req_cpu_m = CpuMillis(req_cpu_m)
        if req_mem_b is not None:
            p.req_mem_b = Bytes(req_mem_b)
        
        if tolerations is not None:
            # Заменяем список целиком
            p.tolerations = deepcopy(tolerations)
        
        if node_selector is not None:
            # Заменяем селектор целиком
            p.node_selector = deepcopy(node_selector)
        
        if affinity is not None:
            p.affinity = deepcopy(affinity)
            
        new_pods[pid] = p

    # Возвращаем новый снапшот (ноды и прочее по ссылке, поды - новый словарь)
    return Snapshot(
        nodes=snapshot.nodes,
        pods=new_pods,
        nodepools=snapshot.nodepools,
        prices=snapshot.prices,
        schedules=snapshot.schedules,
        keda_pool_name=snapshot.keda_pool_name
    )


# ---------------------------------------------------------------------------
# Операции перемещения подов между пулами
# ---------------------------------------------------------------------------


def move_node_pods_to_pool(
    snapshot,
    node_name: str,
    target_pool: str,
    include_system: bool = False,
    include_daemonsets: bool = False,
    overrides=None,
    **kwargs,
):
    target_pool = _normalize_pool_name(target_pool)
    all_pods_on_node = _collect_pods_by_node(snapshot, node_name)
    pod_ids = _filter_pods_for_move(
        snapshot,
        all_pods_on_node,
        include_system=include_system,
        include_daemonsets=include_daemonsets,
    )

    if not pod_ids:
        _cleanup_empty_nodes(snapshot)
        return snapshot

    if overrides:
        snapshot = patch_pods_in_snapshot(
            snapshot, pod_ids, 
            req_cpu_m=overrides.req_cpu_m,
            req_mem_b=overrides.req_mem_b,
            tolerations=overrides.tolerations,
            node_selector=overrides.node_selector,
            affinity=overrides.affinity
        )

    snapshot = move_pods_to_pool(snapshot, pod_ids, target_pool)
    _cleanup_empty_nodes(snapshot)
    return snapshot


def move_namespace_to_pool(snapshot, namespace: str, target_pool: str, overrides=None):
    target_pool = _normalize_pool_name(target_pool)
    all_ns_pods = _collect_pods_by_namespace(snapshot, namespace)
    pod_ids = _filter_workload_pods(snapshot, all_ns_pods)

    if not pod_ids:
        _cleanup_empty_nodes(snapshot)
        return snapshot

    if overrides:
        snapshot = patch_pods_in_snapshot(
            snapshot, pod_ids, 
            req_cpu_m=overrides.req_cpu_m,
            req_mem_b=overrides.req_mem_b,
            tolerations=overrides.tolerations,
            node_selector=overrides.node_selector,
            affinity=overrides.affinity
        )

    snapshot = move_pods_to_pool(snapshot, pod_ids, target_pool)
    _cleanup_empty_nodes(snapshot)
    return snapshot


def move_owner_to_pool(snapshot, namespace: str, owner_name: str, target_pool: str, overrides=None):
    """
    Переместить все рабочие поды одного owner'а (deployment/statefulset) в пул target_pool.
    """
    target_pool = _normalize_pool_name(target_pool)
    all_owner_pods = _collect_pods_by_owner(snapshot, namespace, owner_name)
    pod_ids = _filter_workload_pods(snapshot, all_owner_pods)

    if not pod_ids:
        _cleanup_empty_nodes(snapshot)
        return snapshot

    if overrides:
        snapshot = patch_pods_in_snapshot(
            snapshot, pod_ids, 
            req_cpu_m=overrides.req_cpu_m,
            req_mem_b=overrides.req_mem_b,
            tolerations=overrides.tolerations,
            node_selector=overrides.node_selector,
            affinity=overrides.affinity
        )

    snapshot = move_pods_to_pool(snapshot, pod_ids, target_pool)
    _cleanup_empty_nodes(snapshot)
    return snapshot


# ---------------------------------------------------------------------------
# Операции удаления
# ---------------------------------------------------------------------------


def delete_pods(snapshot, pod_ids: Sequence[str]):
    pods = getattr(snapshot, "pods", {})
    for pod_id in pod_ids:
        pods.pop(pod_id, None)

    _cleanup_empty_nodes(snapshot)
    return snapshot


def delete_namespace(snapshot, namespace: str):
    pods = getattr(snapshot, "pods", {})
    to_delete: List[str] = []
    for pod_id, pod in pods.items():
        if getattr(pod, "namespace", None) == namespace:
            to_delete.append(pod_id)

    for pod_id in to_delete:
        pods.pop(pod_id, None)

    _cleanup_empty_nodes(snapshot)
    return snapshot


def delete_owner(snapshot, namespace: str, owner_name: str):
    pods = getattr(snapshot, "pods", {})
    to_delete: List[str] = []
    for pod_id, pod in pods.items():
        if getattr(pod, "namespace", None) != namespace:
            continue
        if getattr(pod, "owner_name", None) == owner_name:
            to_delete.append(pod_id)

    for pod_id in to_delete:
        pods.pop(pod_id, None)

    _cleanup_empty_nodes(snapshot)
    return snapshot