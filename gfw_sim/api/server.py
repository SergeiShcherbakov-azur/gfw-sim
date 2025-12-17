from __future__ import annotations

import json
import copy
from pathlib import Path
from typing import Dict, List

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from ..snapshot.from_legacy import snapshot_from_legacy_data
from ..sim.simulate import run_simulation

from ..sim.operations import (
    move_namespace_to_pool,
    move_owner_to_pool,
    move_node_pods_to_pool,
    delete_pods,
    delete_namespace,
    delete_owner,
)
from ..sim.constraints import check_all_placements
from ..sim import costs as sim_costs
from ..types import NodePoolName, PodId
from .schema import (
    SimulationResponse,
    SimulationSummaryModel,
    NodeRowModel,
    NodePartsModel,
    PodViewModel,
    MutateRequest,
    OperationModel,
)

from ..sim.packing import move_pods_to_pool



def _iter_nodes(snapshot: Snapshot):
    nodes = getattr(snapshot, "nodes", [])
    if isinstance(nodes, dict):
        return nodes.values()
    return nodes


def _clone_snapshot(snapshot: Snapshot) -> Snapshot:
    if snapshot is None:
        return None
    clone = getattr(snapshot, "clone", None)
    if callable(clone):
        return clone()
    return copy.deepcopy(snapshot)


def _prune_nodes_only_daemonsets(snapshot: Snapshot) -> Snapshot:
    """Удаляет ноды, на которых не осталось ни одного НЕ-daemonset пода.

    В терминах симуляции: если на ноде остались только DaemonSet-поды (или вообще
    не осталось подов), Karpenter при consolidation может удалить такую ноду.

    Важно: любые не-daemonset поды (включая системные, например karpenter)
    считаются "значимой" нагрузкой и сохраняют ноду.
    """
    if snapshot is None:
        return snapshot

    nodes = getattr(snapshot, "nodes", None)
    pods = getattr(snapshot, "pods", None)
    if not isinstance(nodes, dict) or not isinstance(pods, dict):
        return snapshot
    if not nodes:
        return snapshot

    # ноды, на которых есть хотя бы один не-daemonset pod
    keep_nodes = set()
    for p in pods.values():
        n = getattr(p, "node", None)
        if not n:
            continue
        if getattr(p, "is_daemonset", False):
            continue
        keep_nodes.add(n)

    # удаляем все ноды, которые не в keep_nodes
    to_delete = [n for n in list(nodes.keys()) if n not in keep_nodes]
    if not to_delete:
        return snapshot

    to_delete_set = set(to_delete)
    for n in to_delete:
        nodes.pop(n, None)

    # вместе с удалённой нодой исчезают и поды, которые были на ней (daemonset тоже)
    for pid, p in list(pods.items()):
        n = getattr(p, "node", None)
        if n in to_delete_set:
            pods.pop(pid, None)

    return snapshot

# ---------------------------------------------------------------------------
# Глобальное состояние
# ---------------------------------------------------------------------------

app = FastAPI(title="GFW Capacity Simulator")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
STATIC_DIR = PROJECT_ROOT / "static"
LEGACY_PATH = Path(__file__).resolve().parents[1] / "snapshot" / "legacy.json"

baseline_snapshot = None
current_snapshot = None


def _pick_instance_type(node) -> str:
    for name in ("instance_type", "instance", "flavor", "instance_type_name"):
        if hasattr(node, name):
            v = getattr(node, name)
            if v:
                return v
    return ""


def load_baseline() -> None:
    """Грузим legacy.json и инициализируем baseline/current snapshot."""
    global baseline_snapshot, current_snapshot
    data = json.loads(LEGACY_PATH.read_text("utf-8"))
    baseline_snapshot = snapshot_from_legacy_data(data)
    current_snapshot = _clone_snapshot(baseline_snapshot)


@app.on_event("startup")
async def startup_event() -> None:
    # Инициализация снапшота
    load_baseline()

    # Пробуем сразу подтянуть прайсы для всех типов инстансов,
    # которые встречаются в снапшоте. При ошибке costs сам уйдёт в fallback.
    try:
        if baseline_snapshot is not None:
            instance_types = sorted(
                {
                    _pick_instance_type(n)
                    for n in _iter_nodes(baseline_snapshot)
                    if _pick_instance_type(n)
                }
            )
            if instance_types:
                sim_costs.refresh_prices_from_aws(instance_types)
    except Exception:
        # Не роняем приложение из-за проблем с AWS CLI / Pricing API.
        pass

    # Монтируем статический каталог по /static
    app.mount(
        "/static",
        StaticFiles(directory=str(STATIC_DIR)),
        name="static",
    )


# ---------------------------------------------------------------------------
# Вспомогательная конвертация SimulationResult -> SimulationResponse
# ---------------------------------------------------------------------------


def to_simulation_response(snapshot) -> SimulationResponse:
    sim = run_simulation(snapshot)
    violations: Dict[str, List[str]] = check_all_placements(snapshot)

    # Таблица нод
    nodes: List[NodeRowModel] = []
    for row in sim.nodes_table:
        nodes.append(
            NodeRowModel(
                node=row.node or "",
                nodepool=row.nodepool or "",
                instance=row.instance or "",
                gfw_ratio_pct=row.gfw_ratio_pct,
                alloc_cpu_m=row.alloc_cpu_m,
                alloc_mem_b=row.alloc_mem_b,
                sum_req_cpu_m=row.sum_req_cpu_m,
                sum_req_mem_b=row.sum_req_mem_b,
                ram_util_pct=row.ram_util_pct,
                ram_ds_gib=row.ram_ds_gib,
                ram_gfw_gib=row.ram_gfw_gib,
                cost_daily_usd=row.cost_daily_usd,
                parts=NodePartsModel(
                    gfw_cpu_m=row.parts.gfw_cpu_m,
                    ds_cpu_m=row.parts.ds_cpu_m,
                    other_cpu_m=row.parts.other_cpu_m,
                    gfw_mem_b=row.parts.gfw_mem_b,
                    ds_mem_b=row.parts.ds_mem_b,
                    other_mem_b=row.parts.other_mem_b,
                ),
                is_virtual=row.is_virtual,
                price_missing=row.price_missing,
            )
        )

    # Поды по нодам
    pods_by_node: Dict[str, List[PodViewModel]] = {}
    for node_name, pods in sim.pods_by_node.items():
        items: List[PodViewModel] = []
        for p in pods:
            pod_id = f"{p.namespace}/{p.name}"
            items.append(
                PodViewModel(
                    pod_id=pod_id,
                    namespace=p.namespace,
                    name=p.name,
                    owner_kind=getattr(p, "owner_kind", None),
                    owner_name=getattr(p, "owner_name", None),
                    is_gfw=p.is_gfw,
                    is_daemon=p.is_daemon,
                    is_system=p.is_system,
                    req_cpu_m=p.req_cpu_m,
                    req_mem_b=p.req_mem_b,
                )
            )
        pods_by_node[node_name] = items
    return SimulationResponse(
        summary=SimulationSummaryModel(
            total_cost_daily_usd=sim.total_cost_daily_usd,
            total_cost_gfw_nodes_usd=sim.total_cost_gfw_nodes_usd,
            total_cost_keda_nodes_usd=sim.total_cost_keda_nodes_usd,
        ),
        nodes=nodes,
        pods_by_node=pods_by_node,
        violations=violations,
    )


# ---------------------------------------------------------------------------
# Маршруты
# ---------------------------------------------------------------------------


@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    """Отдаём фронт с ./static/index.html."""
    index_path = STATIC_DIR / "index.html"
    return FileResponse(index_path)


@app.get("/simulate", response_model=SimulationResponse)
def simulate() -> SimulationResponse:
    """Текущее состояние симуляции."""
    if current_snapshot is None:
        raise HTTPException(status_code=500, detail="Snapshot is not initialized")
    return to_simulation_response(current_snapshot)



@app.post("/mutate", response_model=SimulationResponse)
def mutate(req: MutateRequest | OperationModel) -> SimulationResponse:
    """
    Применяет одну операцию (DnD) или список операций (batch) и возвращает новое состояние.
    """
    global current_snapshot, baseline_snapshot

    if current_snapshot is None:
        raise HTTPException(status_code=500, detail="Snapshot is not initialized")

    ops = req.operations if isinstance(req, MutateRequest) else [req]
    snap = current_snapshot

    for op in ops:
        if op.op == "reset_to_baseline":
            snap = _clone_snapshot(baseline_snapshot)

        elif op.op == "move_namespace_to_pool":
            snap = move_namespace_to_pool(
                snap,
                namespace=op.namespace,
                target_pool=NodePoolName(op.target_pool),
            )

        elif op.op == "move_owner_to_pool":
            snap = move_owner_to_pool(
                snap,
                namespace=op.namespace,
                owner_kind=op.owner_kind,
                owner_name=op.owner_name,
                target_pool=NodePoolName(op.target_pool),
            )

        elif op.op == "move_node_pods_to_pool":
            snap = move_node_pods_to_pool(
                snap,
                node_name=NodeName(op.node),
                target_pool=NodePoolName(op.target_pool),
            )

        elif op.op == "move_pods_to_pool":
            target_pool = (op.target_pool or "").strip()
            if target_pool:
                # иногда фронт присылает "nodeName nodepool" — берём последний токен
                target_pool = target_pool.split()[-1]
            snap = move_pods_to_pool(
                snap,
                pod_ids=[PodId(pid) for pid in op.pod_ids],
                target_pool=NodePoolName(target_pool),
            )

        elif op.op == "delete_pods":
            snap = delete_pods(
                snap,
                pod_ids=[PodId(pid) for pid in op.pod_ids],
            )

        elif op.op == "delete_namespace":
            snap = delete_namespace(
                snap,
                namespace=op.namespace,
                include_system=op.include_system,
                include_daemonsets=op.include_daemonsets,
            )

        elif op.op == "delete_owner":
            snap = delete_owner(
                snap,
                namespace=op.namespace,
                owner_kind=op.owner_kind,
                owner_name=op.owner_name,
                include_system=op.include_system,
                include_daemonsets=op.include_daemonsets,
            )

        else:
            raise HTTPException(status_code=400, detail=f"Unknown operation: {op.op}")

    # GC: удалить ноды, где остались только DaemonSet-поды
    snap = _prune_nodes_only_daemonsets(snap)

    current_snapshot = snap
    return to_simulation_response(current_snapshot)

@app.post("/admin/refresh-prices")
def admin_refresh_prices() -> dict:
    """
    Ручка под кнопку "запросить данные из AWS".

    Обновляет кеш прайсов из Pricing API для всех instance_type,
    встречающихся в текущем снапшоте.
    """
    if current_snapshot is None:
        raise HTTPException(status_code=500, detail="Snapshot is not initialized")

    instance_types = sorted(
        {
            _pick_instance_type(n)
            for n in _iter_nodes(current_snapshot)
            if _pick_instance_type(n)
        }
    )
    state = sim_costs.refresh_prices_from_aws(instance_types)

    return {
        "ok": True,
        "region": state.region,
        "instance_types": instance_types,
        "hourly_prices": state.hourly_prices,
    }
