# gfw_sim/api/server.py
from __future__ import annotations

import json
import time
import copy
import logging
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from ..model.entities import Snapshot
from ..snapshot.from_legacy import snapshot_from_legacy_data
from ..snapshot.collector import collect_k8s_snapshot
from ..snapshot.io import save_snapshot_to_file, load_snapshot_from_file
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
from ..types import NodePoolName, PodId, NodeId
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

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _iter_nodes(snapshot: Snapshot):
    nodes = getattr(snapshot, "nodes", [])
    if isinstance(nodes, dict):
        return nodes.values()
    return nodes

def _clone_snapshot(snapshot: Snapshot) -> Snapshot:
    if snapshot is None:
        return None
    return copy.deepcopy(snapshot)

def _prune_nodes_only_daemonsets(snapshot: Snapshot) -> Snapshot:
    if snapshot is None:
        return snapshot

    nodes = getattr(snapshot, "nodes", None)
    pods = getattr(snapshot, "pods", None)
    if not isinstance(nodes, dict) or not isinstance(pods, dict):
        return snapshot
    if not nodes:
        return snapshot

    keep_nodes = set()
    for p in pods.values():
        n = getattr(p, "node", None)
        if not n:
            continue
        if getattr(p, "is_daemonset", False):
            continue
        keep_nodes.add(n)

    to_delete = [n for n in list(nodes.keys()) if n not in keep_nodes]
    if not to_delete:
        return snapshot

    to_delete_set = set(to_delete)
    for n in to_delete:
        nodes.pop(n, None)

    for pid, p in list(pods.items()):
        n = getattr(p, "node", None)
        if n in to_delete_set:
            pods.pop(pid, None)

    return snapshot

def _pick_instance_type(node) -> str:
    for name in ("instance_type", "instance", "flavor", "instance_type_name"):
        if hasattr(node, name):
            v = getattr(node, name)
            if v:
                return v
    return ""

def to_simulation_response(snapshot) -> SimulationResponse:
    sim = run_simulation(snapshot)
    
    # 1. Получаем нарушения в формате {NodeName: [{"pod_id":..., "reasons":...}]}
    raw_violations = check_all_placements(snapshot)
    
    # 2. Преобразуем в плоский формат {PodId: [Reasons]} для фронтенда и схемы
    violations: Dict[str, List[str]] = {}
    for node_vs in raw_violations.values():
        for v in node_vs:
            pid = v.get("pod_id")
            reasons = v.get("reasons", [])
            if pid and reasons:
                violations[pid] = reasons

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

    pods_by_node: Dict[str, List[PodViewModel]] = {}
    for node_name, pods in sim.pods_by_node.items():
        items: List[PodViewModel] = []
        for p in pods:
            # PodId во внутренней структуре PodView уже может быть строкой
            # Но для надежности соберем
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
# Snapshot Manager
# ---------------------------------------------------------------------------

class SnapshotManager:
    def __init__(self):
        self.snapshots: Dict[str, Snapshot] = {}
        self.active_id: str | None = None

    def add(self, snapshot_id: str, snapshot: Snapshot):
        self.snapshots[snapshot_id] = snapshot
        if self.active_id is None:
            self.active_id = snapshot_id

    def get_active(self) -> Optional[Snapshot]:
        if self.active_id:
            return self.snapshots.get(self.active_id)
        return None

    def set_active(self, snapshot_id: str):
        if snapshot_id not in self.snapshots:
            raise ValueError(f"Snapshot {snapshot_id} not found")
        self.active_id = snapshot_id

    def update_active(self, new_snapshot: Snapshot):
        if self.active_id:
            self.snapshots[self.active_id] = new_snapshot

manager = SnapshotManager()

# ---------------------------------------------------------------------------
# App & Startup
# ---------------------------------------------------------------------------

app = FastAPI(title="GFW Capacity Simulator")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
STATIC_DIR = PROJECT_ROOT / "static"
# Папка для сохраняемых снапшотов
SNAPSHOTS_DIR = PROJECT_ROOT / "snapshots"
LEGACY_PATH = Path(__file__).resolve().parents[1] / "snapshot" / "legacy.json"

@app.on_event("startup")
async def startup_event() -> None:
    # 1. Load Baseline (legacy.json)
    try:
        if LEGACY_PATH.exists():
            data = json.loads(LEGACY_PATH.read_text("utf-8"))
            baseline = snapshot_from_legacy_data(data)
            manager.add("baseline", baseline)
            
            # По умолчанию создаем рабочую копию из baseline, если нет других
            working = _clone_snapshot(baseline)
            manager.add("working-copy", working)
            manager.set_active("working-copy")
            log.info("Baseline loaded.")
    except Exception as e:
        log.warning(f"Failed to load baseline: {e}")

    # 2. Load Persisted Snapshots from disk
    try:
        if not SNAPSHOTS_DIR.exists():
            SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
        
        count = 0
        for snap_file in SNAPSHOTS_DIR.glob("*.json"):
            try:
                snap = load_snapshot_from_file(snap_file)
                # ID = имя файла без расширения
                sid = snap_file.stem
                manager.add(sid, snap)
                count += 1
            except Exception as e:
                log.error(f"Failed to load snapshot {snap_file}: {e}")
        
        log.info(f"Loaded {count} snapshots from disk.")
        
    except Exception as e:
        log.error(f"Error loading snapshots directory: {e}")

    # 3. Refresh Prices (Initial)
    current = manager.get_active()
    if current:
        try:
            instance_types = sorted({
                _pick_instance_type(n) for n in _iter_nodes(current) if _pick_instance_type(n)
            })
            if instance_types:
                sim_costs.refresh_prices_from_aws(instance_types)
        except Exception:
            pass

    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# ---------------------------------------------------------------------------
# New Snapshot API
# ---------------------------------------------------------------------------

class SnapshotListItem(BaseModel):
    id: str
    nodes_count: int
    pods_count: int
    is_active: bool

class CreateSnapshotResponse(BaseModel):
    id: str
    message: str

@app.get("/snapshots", response_model=List[SnapshotListItem])
def list_snapshots():
    result = []
    active = manager.active_id
    # Сортируем: сначала baseline/working, потом по алфавиту (даты)
    keys = sorted(manager.snapshots.keys())
    
    for sid in keys:
        snap = manager.snapshots[sid]
        result.append(SnapshotListItem(
            id=sid,
            nodes_count=len(getattr(snap, "nodes", [])),
            pods_count=len(getattr(snap, "pods", [])),
            is_active=(sid == active)
        ))
    return result

@app.post("/snapshots/capture", response_model=CreateSnapshotResponse)
def capture_snapshot():
    """Сбор текущего состояния кластера и сохранение на диск."""
    try:
        new_snap = collect_k8s_snapshot()
        
        # ID = k8s-<timestamp>
        new_id = f"k8s-{int(time.time())}"
        
        # Сохраняем на диск
        if not SNAPSHOTS_DIR.exists():
            SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
            
        file_path = SNAPSHOTS_DIR / f"{new_id}.json"
        save_snapshot_to_file(new_snap, file_path)
        
        # Добавляем в менеджер
        manager.add(new_id, new_snap)
        
        # Обновляем прайсы для новых типов
        try:
            itypes = sorted({
                _pick_instance_type(n) for n in _iter_nodes(new_snap) if _pick_instance_type(n)
            })
            if itypes:
                sim_costs.refresh_prices_from_aws(itypes)
        except Exception as e:
            log.warning(f"Price refresh failed after capture: {e}")

        return CreateSnapshotResponse(id=new_id, message=f"Captured and saved as {new_id}")
    except Exception as e:
        log.error(f"Capture failed: {e}")
        raise HTTPException(status_code=500, detail=f"Capture failed: {str(e)}")

@app.post("/snapshots/{snapshot_id}/activate")
def activate_snapshot(snapshot_id: str):
    try:
        manager.set_active(snapshot_id)
        return {"status": "ok", "active": snapshot_id}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

# ---------------------------------------------------------------------------
# Simulation API
# ---------------------------------------------------------------------------

@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    index_path = STATIC_DIR / "index.html"
    return FileResponse(index_path)

@app.get("/simulate", response_model=SimulationResponse)
def simulate() -> SimulationResponse:
    snap = manager.get_active()
    if snap is None:
        # Если ничего не загрузилось, попробуем вернуть пустое состояние или 500
        raise HTTPException(status_code=500, detail="Snapshot is not initialized")
    return to_simulation_response(snap)

@app.post("/mutate", response_model=SimulationResponse)
def mutate(req: MutateRequest | OperationModel) -> SimulationResponse:
    snap = manager.get_active()
    if snap is None:
        raise HTTPException(status_code=500, detail="Snapshot is not initialized")

    ops = req.operations if isinstance(req, MutateRequest) else [req]
    
    for op in ops:
        if op.op == "reset_to_baseline":
            # Пытаемся найти baseline, иначе просто reload текущего с диска, если он сохранен
            # Логика: если активный снапшот "k8s-...", то reset должен вернуть его к исходному состоянию файла
            # Если "working-copy", то к "baseline".
            
            active_id = manager.active_id
            
            if active_id == "working-copy" and "baseline" in manager.snapshots:
                snap = _clone_snapshot(manager.snapshots["baseline"])
            
            elif active_id in manager.snapshots:
                # Если это сохраненный снапшот, перезагрузим его из памяти (или с диска)
                # Сейчас в памяти хранится измененная версия? 
                # Нет, manager хранит ссылку. Mutate возвращает НОВУЮ копию.
                # Чтобы сделать reset, нам нужно найти исходник.
                # Упростим: если reset, пробуем загрузить файл заново, если он есть.
                
                # Для простоты: Reset всегда возвращает к baseline.json, как было раньше?
                # Или сбрасывает изменения текущей сессии? 
                # Давайте сделаем сброс к 'baseline' (legacy.json), так надежнее.
                if "baseline" in manager.snapshots:
                    snap = _clone_snapshot(manager.snapshots["baseline"])
                    # И переключимся на working-copy, чтобы не портить сохраненный файл
                    manager.add("working-copy", snap)
                    manager.set_active("working-copy")
                pass

        elif op.op == "move_namespace_to_pool":
            snap = move_namespace_to_pool(snap, op.namespace, NodePoolName(op.target_pool))

        elif op.op == "move_owner_to_pool":
            snap = move_owner_to_pool(snap, op.namespace, op.owner_kind, op.owner_name, NodePoolName(op.target_pool))

        elif op.op == "move_node_pods_to_pool":
            snap = move_node_pods_to_pool(snap, op.node_name, NodePoolName(op.target_pool))

        elif op.op == "move_pods_to_pool":
            tpool = (op.target_pool or "").strip().split()[-1]
            snap = move_pods_to_pool(snap, [PodId(pid) for pid in op.pod_ids], NodePoolName(tpool))

        elif op.op == "delete_pods":
            snap = delete_pods(snap, [PodId(pid) for pid in op.pod_ids])

        elif op.op == "delete_namespace":
            snap = delete_namespace(snap, op.namespace, op.include_system, op.include_daemonsets)

        elif op.op == "delete_owner":
            snap = delete_owner(snap, op.namespace, op.owner_kind, op.owner_name, op.include_system, op.include_daemonsets)
        
        else:
            raise HTTPException(status_code=400, detail=f"Unknown op: {op.op}")

    snap = _prune_nodes_only_daemonsets(snap)
    manager.update_active(snap)
    
    return to_simulation_response(snap)

@app.post("/admin/refresh-prices")
def admin_refresh_prices() -> dict:
    snap = manager.get_active()
    if snap is None:
        raise HTTPException(status_code=500, detail="Snapshot is not initialized")

    instance_types = sorted({
        _pick_instance_type(n) for n in _iter_nodes(snap) if _pick_instance_type(n)
    })
    state = sim_costs.refresh_prices_from_aws(instance_types)

    return {
        "ok": True,
        "region": state.region,
        "instance_types": instance_types,
        "hourly_prices": state.hourly_prices,
    }