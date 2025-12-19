# gfw_sim/api/server.py
from __future__ import annotations

import json
import time
import copy
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from ..snapshot.io import load_snapshot_from_file, save_snapshot_to_file
from ..snapshot.from_legacy import snapshot_from_legacy_data
from ..snapshot.collector import collect_k8s_snapshot
from ..sim import simulate, costs as sim_costs, constraints
from ..model.entities import Snapshot, NodePool
from ..sim.operations import (
    move_namespace_to_pool, move_owner_to_pool, move_node_pods_to_pool,
    move_pods_to_pool, delete_pods, delete_namespace, delete_owner, patch_pods_in_snapshot
)
from ..types import NodePoolName, PodId, NodeId
from .schema import (
    SimulationResponse, SimulationSummaryModel, NodeRowModel, NodePartsModel,
    PodViewModel, MutateRequest, OperationModel, PlanMoveRequest, PlanMoveResponse,
    PoolCostModel, LogEntry # NEW
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

log = logging.getLogger("uvicorn")

# --- PATHS ---
MODULE_ROOT = Path(__file__).resolve().parent.parent 
PROJECT_ROOT = MODULE_ROOT.parent    
STATIC_DIR = PROJECT_ROOT / "static"
SNAPSHOTS_DIR = PROJECT_ROOT / "snapshots"
LEGACY_PATH = MODULE_ROOT / "snapshot" / "legacy.json"

CURRENT_SNAPSHOT: Snapshot | None = None

# --- STATE ---
# Хранилище логов для каждого снапшота (id -> list[LogEntry])
SNAPSHOT_LOGS: Dict[str, List[LogEntry]] = {}

# --- Helpers ---

def _iter_nodes(snapshot: Snapshot):
    nodes = getattr(snapshot, "nodes", [])
    if isinstance(nodes, dict): return nodes.values()
    return nodes

def _clone_snapshot(snapshot: Snapshot) -> Snapshot:
    if snapshot is None: return None
    return copy.deepcopy(snapshot)

def _prune_nodes_only_daemonsets(snapshot: Snapshot) -> Snapshot:
    if snapshot is None: return snapshot
    nodes = getattr(snapshot, "nodes", {})
    pods = getattr(snapshot, "pods", {})
    if not isinstance(nodes, dict) or not isinstance(pods, dict): return snapshot
    if not nodes: return snapshot

    keep_nodes = set()
    for p in pods.values():
        n = getattr(p, "node", None)
        if not n: continue
        if getattr(p, "is_daemonset", False): continue
        keep_nodes.add(n)

    to_delete = [n for n in list(nodes.keys()) if n not in keep_nodes]
    for n in to_delete:
        nodes.pop(n, None)
        
    for pid, p in list(pods.items()):
        n = getattr(p, "node", None)
        if n and n not in nodes:
             p.node = None 

    return snapshot

def _pick_instance_type(node) -> str:
    for name in ("instance_type", "instance", "flavor", "instance_type_name"):
        if hasattr(node, name):
            v = getattr(node, name)
            if v: return v
    return ""

def _ensure_history(snapshot: Snapshot):
    if not snapshot: return
    history = getattr(snapshot, "history_usage", [])
    if history: return 

    new_hist = []
    nodes = getattr(snapshot, "nodes", {})
    agg = {}
    for n in nodes.values():
        pool = getattr(n, "nodepool", "default")
        inst = getattr(n, "instance_type", "unknown")
        key = (str(pool), str(inst))
        agg[key] = agg.get(key, 0.0) + 24.0
    
    for (p, i), hours in agg.items():
        new_hist.append({
            "pool": p,
            "instance": i,
            "instance_hours_24h": hours
        })
    snapshot.history_usage = new_hist

# --- Snapshot Manager ---

class SnapshotManager:
    def __init__(self):
        self.snapshots: Dict[str, Snapshot] = {}
        self.active_id: str | None = None

    def add(self, snapshot_id: str, snapshot: Snapshot):
        _ensure_history(snapshot)
        self.snapshots[snapshot_id] = snapshot
        if self.active_id is None:
            self.active_id = snapshot_id
        if snapshot_id not in SNAPSHOT_LOGS:
            SNAPSHOT_LOGS[snapshot_id] = []

    def get_active(self) -> Optional[Snapshot]:
        if self.active_id:
            return self.snapshots.get(self.active_id)
        return None

    def set_active(self, snapshot_id: str):
        if snapshot_id not in self.snapshots:
            raise ValueError(f"Snapshot {snapshot_id} not found")
        self.active_id = snapshot_id
        if snapshot_id not in SNAPSHOT_LOGS:
            SNAPSHOT_LOGS[snapshot_id] = []

    def update_active(self, new_snapshot: Snapshot):
        if self.active_id:
            self.snapshots[self.active_id] = new_snapshot

manager = SnapshotManager()

def to_simulation_response(snapshot) -> SimulationResponse:
    sim = simulate.run_simulation(snapshot)
    
    # Мы убрали валидацию Constraints, чтобы не захламлять лог
    # Если нужно вернуть - раскомментируйте, но в ответе шлем логи
    
    nodes_list = []
    for row in sim.nodes_table:
        nodes_list.append(
            NodeRowModel(
                node=row.node or "",
                nodepool=row.nodepool or "",
                instance=row.instance or "",
                gfw_ratio_pct=row.gfw_ratio_pct,
                alloc_cpu_m=row.alloc_cpu_m,
                alloc_mem_b=row.alloc_mem_b,
                sum_req_cpu_m=row.sum_req_cpu_m,
                sum_req_mem_b=row.sum_req_mem_b,
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

    pods_by_node = {}
    for node_name, pods in sim.pods_by_node.items():
        items = []
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
                    usage_cpu_m=p.usage_cpu_m,
                    usage_mem_b=p.usage_mem_b,
                    active_ratio=p.active_ratio
                )
            )
        pods_by_node[node_name] = items
        
    logs = SNAPSHOT_LOGS.get(manager.active_id, [])
    # Сортируем: новые сверху
    logs = sorted(logs, key=lambda x: x.timestamp, reverse=True)

    return SimulationResponse(
        summary=SimulationSummaryModel(
            total_cost_daily_usd=sim.total_cost_daily_usd,
            pool_stats={k: PoolCostModel(cost=v.cost, nodes_count=v.count) for k,v in sim.pool_stats.items()},
            projected_pool_stats={k: PoolCostModel(cost=v.cost, nodes_count=v.count) for k,v in sim.projected_pool_stats.items()},
            projected_total_cost_usd=sim.projected_total_cost_usd,
            total_cost_gfw_nodes_usd=sim.total_cost_gfw_nodes_usd,
            total_cost_keda_nodes_usd=sim.total_cost_keda_nodes_usd,
        ),
        nodes=nodes_list,
        pods_by_node=pods_by_node,
        logs=logs  # --- SEND LOGS INSTEAD OF VIOLATIONS
    )

@app.on_event("startup")
async def startup_event() -> None:
    try:
        if LEGACY_PATH.exists():
            data = json.loads(LEGACY_PATH.read_text("utf-8"))
            baseline = snapshot_from_legacy_data(data)
            manager.add("baseline", baseline)
            manager.set_active("baseline")
    except Exception: pass

    try:
        if not SNAPSHOTS_DIR.exists():
            SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
        
        snap_files = list(SNAPSHOTS_DIR.glob("*.json"))
        snap_files.sort(key=lambda p: p.stat().st_mtime)
        
        last_loaded_id = None
        for snap_file in snap_files:
            try:
                snap = load_snapshot_from_file(snap_file)
                sid = snap_file.stem
                manager.add(sid, snap)
                last_loaded_id = sid
            except Exception as e:
                log.error(f"Failed to load {snap_file}: {e}")
        
        if last_loaded_id:
            manager.set_active(last_loaded_id)
            
    except Exception as e:
        log.error(f"Error loading snapshots: {e}")

    current = manager.get_active()
    if current:
        try:
            instance_types = sorted({
                _pick_instance_type(n) for n in _iter_nodes(current) if _pick_instance_type(n)
            })
            if instance_types:
                sim_costs.refresh_prices_from_aws(instance_types)
        except Exception: pass

    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# --- Endpoints ---

@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    if STATIC_DIR.exists():
        return FileResponse(STATIC_DIR / "index.html")
    raise HTTPException(status_code=404, detail="Static files not found")

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
        manager.set_active(new_id)
        return CreateSnapshotResponse(id=new_id, message=f"Captured {new_id}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/snapshots/{snapshot_id}/activate")
def activate_snapshot(snapshot_id: str):
    try:
        manager.set_active(snapshot_id)
        return {"status": "ok", "active": snapshot_id}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

def _derive_placement_patches(node: Any, nodepool: Optional[NodePool]) -> tuple[List[Dict], Dict]:
    tolerations = []
    seen_keys = set()
    def _add_tol(key, value, operator, effect):
        if key in seen_keys: return
        tolerations.append({"key": key, "operator": operator, "value": value, "effect": effect})
        seen_keys.add(key)
    node_taints = getattr(node, "taints", []) or []
    for t in node_taints:
        k = t.get("key"); v = t.get("value"); eff = t.get("effect")
        op = "Equal" if v else "Exists"
        _add_tol(k, v, op, eff)
    if nodepool:
        pool_taints = getattr(nodepool, "taints", []) or []
        for t in pool_taints:
            k = t.get("key"); v = t.get("value"); eff = t.get("effect")
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
    if snap is None: raise HTTPException(status_code=500, detail="Snapshot not init")
    
    pod = snap.pods.get(PodId(req.pod_id))
    if not pod: raise HTTPException(status_code=404, detail=f"Pod {req.pod_id} not found")
    target_node = snap.nodes.get(NodeId(req.target_node))
    if not target_node: raise HTTPException(status_code=404, detail=f"Target node {req.target_node} not found")

    target_pool_name = getattr(target_node, "nodepool", None)
    target_pool = snap.nodepools.get(target_pool_name) if target_pool_name else None
    
    suggested_tols, suggested_sel = _derive_placement_patches(target_node, target_pool)
    
    owner_kind = getattr(pod, "owner_kind", None)
    owner_name = getattr(pod, "owner_name", None)
    
    if owner_kind == "ReplicaSet" and owner_name:
        parts = owner_name.rsplit("-", 1)
        if len(parts) > 1:
             owner_kind = "Deployment"
             owner_name = parts[0]

    return PlanMoveResponse(
        pod_id=req.pod_id,
        owner_kind=owner_kind,
        owner_name=owner_name,
        current_req_cpu_m=int(pod.req_cpu_m or 0),
        current_req_mem_b=int(pod.req_mem_b or 0),
        suggested_tolerations=suggested_tols,
        suggested_node_selector=suggested_sel
    )

@app.get("/simulate", response_model=SimulationResponse)
def simulate_endpoint() -> SimulationResponse:
    snap = manager.get_active()
    if snap is None: raise HTTPException(status_code=500, detail="Snapshot is not initialized")
    return to_simulation_response(snap)

def _add_log(message: str, details: Optional[Dict] = None):
    if manager.active_id:
        if manager.active_id not in SNAPSHOT_LOGS:
            SNAPSHOT_LOGS[manager.active_id] = []
        SNAPSHOT_LOGS[manager.active_id].append(
            LogEntry(timestamp=time.time(), message=message, details=details)
        )

@app.post("/mutate", response_model=SimulationResponse)
def mutate(req: MutateRequest | OperationModel) -> SimulationResponse:
    snap = manager.get_active()
    if snap is None: raise HTTPException(status_code=500, detail="Snapshot is not initialized")

    ops = req.operations if isinstance(req, MutateRequest) else [req]
    
    for op in ops:
        if op.op == "reset_to_baseline":
            if manager.active_id and manager.active_id.startswith("k8s-"):
                 path = SNAPSHOTS_DIR / f"{manager.active_id}.json"
                 if path.exists():
                     snap = load_snapshot_from_file(path)
                     _ensure_history(snap)
                     manager.snapshots[manager.active_id] = snap
            elif "baseline" in manager.snapshots:
                snap = _clone_snapshot(manager.snapshots["baseline"])
                manager.add("working-copy", snap)
                manager.set_active("working-copy")
            
            # Clear logs on reset
            if manager.active_id in SNAPSHOT_LOGS:
                SNAPSHOT_LOGS[manager.active_id] = []
            _add_log("Simulation reset to baseline")

        elif op.op == "move_pod_to_node":
            target_node_id = NodeId(op.node_name)
            if target_node_id not in snap.nodes:
                 raise HTTPException(404, f"Node {target_node_id} not found")
            pids = [PodId(pid) for pid in op.pod_ids]
            
            details = {}
            if op.overrides:
                snap = patch_pods_in_snapshot(
                    snap, pids,
                    req_cpu_m=op.overrides.req_cpu_m,
                    req_mem_b=op.overrides.req_mem_b,
                    tolerations=op.overrides.tolerations,
                    node_selector=op.overrides.node_selector,
                    affinity=op.overrides.affinity
                )
                details = op.overrides.dict(exclude_none=True)
            
            for pid in pids:
                if pid in snap.pods:
                    snap.pods[pid].node = target_node_id
            
            _add_log(f"Moved {len(pids)} pod(s) to node {target_node_id}", details)

        elif op.op == "move_owner_to_pool":
            target_pool_name = NodePoolName(op.target_pool)
            owner_name_prefix = op.owner_name
            owner_kind = op.owner_kind
            
            affected_pids = []
            
            for pid, p in snap.pods.items():
                p_owner_kind = getattr(p, "owner_kind", "")
                p_owner_name = getattr(p, "owner_name", "")
                
                match = False
                if p_owner_kind == owner_kind and p_owner_name == owner_name_prefix:
                    match = True
                elif owner_kind == "Deployment" and p_owner_kind == "ReplicaSet":
                    if p_owner_name and p_owner_name.startswith(owner_name_prefix):
                        match = True
                
                if match:
                    if op.overrides:
                        if op.overrides.req_cpu_m: p.req_cpu_m = op.overrides.req_cpu_m
                        if op.overrides.req_mem_b: p.req_mem_b = op.overrides.req_mem_b
                        if op.overrides.tolerations is not None: p.tolerations = op.overrides.tolerations
                        if op.overrides.node_selector is not None: p.node_selector = op.overrides.node_selector
                        if op.overrides.affinity is not None: p.affinity = op.overrides.affinity
                    
                    if not p.node_selector: p.node_selector = {}
                    p.node_selector["karpenter.sh/nodepool"] = target_pool_name
                    
                    p.node = None
                    affected_pids.append(pid)
            
            details = op.overrides.dict(exclude_none=True) if op.overrides else {}
            _add_log(f"Moved {owner_kind} {owner_name_prefix} ({len(affected_pids)} pods) to pool {target_pool_name}", details)

        elif op.op == "move_namespace_to_pool":
            snap = move_namespace_to_pool(snap, op.namespace, NodePoolName(op.target_pool), overrides=op.overrides)
            _add_log(f"Moved Namespace {op.namespace} to pool {op.target_pool}")
        elif op.op == "move_node_pods_to_pool":
            snap = move_node_pods_to_pool(snap, op.node_name, NodePoolName(op.target_pool), overrides=op.overrides)
            _add_log(f"Evacuated Node {op.node_name} to pool {op.target_pool}")
        elif op.op == "move_pods_to_pool":
            # legacy op
            pass
        elif op.op == "delete_pods":
            snap = delete_pods(snap, [PodId(pid) for pid in op.pod_ids])
            _add_log(f"Deleted {len(op.pod_ids)} pods")
        elif op.op == "delete_namespace":
            snap = delete_namespace(snap, op.namespace, op.include_system, op.include_daemonsets)
            _add_log(f"Deleted Namespace {op.namespace}")
        elif op.op == "delete_owner":
            snap = delete_owner(snap, op.namespace, op.owner_name, op.include_system, op.include_daemonsets)
            _add_log(f"Deleted Owner {op.owner_name}")

    snap = _prune_nodes_only_daemonsets(snap)
    manager.update_active(snap)
    return to_simulation_response(snap)

@app.post("/admin/refresh-prices")
def admin_refresh_prices() -> dict:
    snap = manager.get_active()
    if snap is None: raise HTTPException(status_code=500, detail="Snapshot is not initialized")
    instance_types = sorted({_pick_instance_type(n) for n in _iter_nodes(snap) if _pick_instance_type(n)})
    state = sim_costs.refresh_prices_from_aws(instance_types)
    return {"ok": True, "region": state.region, "instance_types": instance_types, "hourly_prices": state.hourly_prices}