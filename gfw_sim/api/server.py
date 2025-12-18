# gfw_sim/api/server.py
from __future__ import annotations

import json
import time
import copy
import logging
import os  # <-- Нужно для сортировки по времени
from pathlib import Path
from typing import Dict, List, Optional, Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from ..model.entities import Snapshot, NodePool
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
    patch_pods_in_snapshot,
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
    PlanMoveRequest,
    PlanMoveResponse,
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
    
    raw_violations = check_all_placements(snapshot)
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
                
                # --- FIX: Прокидываем usage в API ---
                sum_usage_cpu_m=getattr(row, "sum_usage_cpu_m", 0),
                sum_usage_mem_b=getattr(row, "sum_usage_mem_b", 0),
                
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
            
            # --- FIX: Прокидываем pool_costs ---
            pool_costs_usd=getattr(sim, "pool_costs_usd", {})
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
        # Логика "первого" больше не нужна здесь, 
        # активный снапшот будем выставлять явно при загрузке.
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
SNAPSHOTS_DIR = PROJECT_ROOT / "snapshots"
LEGACY_PATH = Path(__file__).resolve().parents[1] / "snapshot" / "legacy.json"

@app.on_event("startup")
async def startup_event() -> None:
    # 1. Загрузка Legacy Baseline (если есть)
    try:
        if LEGACY_PATH.exists():
            data = json.loads(LEGACY_PATH.read_text("utf-8"))
            baseline = snapshot_from_legacy_data(data)
            manager.add("baseline", baseline)
            # Временно ставим активным, но позже перепишем, если найдем файлы
            manager.set_active("baseline")
            log.info("Baseline loaded.")
    except Exception as e:
        log.warning(f"Failed to load baseline: {e}")

    # 2. Загрузка снапшотов из папки + выбор последнего
    try:
        if not SNAPSHOTS_DIR.exists():
            SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
        
        # Получаем список файлов и сортируем по времени изменения (от старых к новым)
        snap_files = list(SNAPSHOTS_DIR.glob("*.json"))
        snap_files.sort(key=lambda p: p.stat().st_mtime)
        
        count = 0
        last_loaded_id = None
        
        for snap_file in snap_files:
            try:
                snap = load_snapshot_from_file(snap_file)
                sid = snap_file.stem
                manager.add(sid, snap)
                last_loaded_id = sid
                count += 1
            except Exception as e:
                log.error(f"Failed to load snapshot {snap_file}: {e}")
        
        log.info(f"Loaded {count} snapshots from disk.")
        
        # 3. Активируем самый свежий (последний в списке)
        if last_loaded_id:
            manager.set_active(last_loaded_id)
            log.info(f"Activated latest snapshot: {last_loaded_id}")
        
    except Exception as e:
        log.error(f"Error loading snapshots directory: {e}")

    # Обновление цен для активного снапшота
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
    # Сортируем ключи, чтобы список был стабильным (можно по времени, но по имени тоже ок)
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
    try:
        new_snap = collect_k8s_snapshot()
        new_id = f"k8s-{int(time.time())}"
        
        if not SNAPSHOTS_DIR.exists():
            SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
            
        file_path = SNAPSHOTS_DIR / f"{new_id}.json"
        save_snapshot_to_file(new_snap, file_path)
        
        manager.add(new_id, new_snap)
        manager.set_active(new_id) # Сразу активируем новый
        
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
# Logic Helper for Plan
# ---------------------------------------------------------------------------

def _derive_placement_patches(node: Any, nodepool: Optional[NodePool]) -> tuple[List[Dict], Dict]:
    tolerations = []
    seen_keys = set()
    
    def _add_tol(key, value, operator, effect):
        if key in seen_keys: return
        tolerations.append({
            "key": key, 
            "operator": operator, 
            "value": value, 
            "effect": effect
        })
        seen_keys.add(key)

    node_taints = getattr(node, "taints", []) or []
    for t in node_taints:
        k = t.get("key")
        v = t.get("value")
        eff = t.get("effect")
        op = "Equal" if v else "Exists"
        _add_tol(k, v, op, eff)

    if nodepool:
        pool_taints = getattr(nodepool, "taints", []) or []
        for t in pool_taints:
            k = t.get("key")
            v = t.get("value")
            eff = t.get("effect")
            op = "Equal" if v else "Exists"
            _add_tol(k, v, op, eff)
            
    node_selector = {}
    pool_name = getattr(node, "nodepool", None)
    if pool_name:
        node_selector["karpenter.sh/nodepool"] = pool_name
    
    return tolerations, node_selector

@app.post("/plan_move", response_model=PlanMoveResponse)
def plan_move(req: PlanMoveRequest) -> PlanMoveResponse:
    snap = manager.get_active()
    if snap is None:
        raise HTTPException(status_code=500, detail="Snapshot is not initialized")
    
    pod = snap.pods.get(req.pod_id)
    if not pod:
        pod = snap.pods.get(PodId(req.pod_id))
    
    if not pod:
         raise HTTPException(status_code=404, detail=f"Pod {req.pod_id} not found")
         
    target_node = snap.nodes.get(NodeId(req.target_node))
    if not target_node:
        raise HTTPException(status_code=404, detail=f"Target node {req.target_node} not found")

    target_pool_name = getattr(target_node, "nodepool", None)
    target_pool = snap.nodepools.get(target_pool_name) if target_pool_name else None
    
    suggested_tols, suggested_sel = _derive_placement_patches(target_node, target_pool)
    
    return PlanMoveResponse(
        pod_id=req.pod_id,
        owner_kind=getattr(pod, "owner_kind", None),
        owner_name=getattr(pod, "owner_name", None),
        current_req_cpu_m=int(pod.req_cpu_m or 0),
        current_req_mem_b=int(pod.req_mem_b or 0),
        suggested_tolerations=suggested_tols,
        suggested_node_selector=suggested_sel
    )

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
            # Сбрасываем к последнему загруженному (baseline может быть не актуален)
            # Логичнее сбрасывать к "активному на момент старта" или перечитывать файл
            # В текущей логике "baseline" - это legacy файл.
            # Если мы хотим reset к текущему снапшоту, нужно хранить его копию.
            # Пока оставим как есть или можно сделать reload активного ID.
            if manager.active_id and manager.active_id in manager.snapshots:
                 # ПРОСТОЙ RESET: Перезагружаем текущий активный снапшот из памяти (или файла)
                 # Но так как мы мутируем объект в памяти, нам нужна исходная копия.
                 # Для упрощения: просто перезагрузим файл с диска для активного ID
                 if manager.active_id.startswith("k8s-"):
                     path = SNAPSHOTS_DIR / f"{manager.active_id}.json"
                     if path.exists():
                         snap = load_snapshot_from_file(path)
                         manager.snapshots[manager.active_id] = snap
            elif "baseline" in manager.snapshots:
                snap = _clone_snapshot(manager.snapshots["baseline"])
                manager.add("working-copy", snap)
                manager.set_active("working-copy")

        elif op.op == "move_namespace_to_pool":
            snap = move_namespace_to_pool(
                snap, op.namespace, NodePoolName(op.target_pool), 
                overrides=op.overrides
            )

        elif op.op == "move_owner_to_pool":
            snap = move_owner_to_pool(
                snap, op.namespace, op.owner_name, NodePoolName(op.target_pool),
                overrides=op.overrides
            )

        elif op.op == "move_node_pods_to_pool":
            snap = move_node_pods_to_pool(
                snap, op.node_name, NodePoolName(op.target_pool),
                overrides=op.overrides
            )

        elif op.op == "move_pods_to_pool":
            tpool = (op.target_pool or "").strip().split()[-1]
            pids = [PodId(pid) for pid in op.pod_ids]
            
            if op.overrides:
                snap = patch_pods_in_snapshot(
                    snap, pids,
                    req_cpu_m=op.overrides.req_cpu_m,
                    req_mem_b=op.overrides.req_mem_b,
                    tolerations=op.overrides.tolerations,
                    node_selector=op.overrides.node_selector,
                    affinity=op.overrides.affinity
                )
                
            snap = move_pods_to_pool(snap, pids, NodePoolName(tpool))

        elif op.op == "delete_pods":
            snap = delete_pods(snap, [PodId(pid) for pid in op.pod_ids])

        elif op.op == "delete_namespace":
            snap = delete_namespace(snap, op.namespace, op.include_system, op.include_daemonsets)

        elif op.op == "delete_owner":
            snap = delete_owner(snap, op.namespace, op.owner_name, op.include_system, op.include_daemonsets)
        
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