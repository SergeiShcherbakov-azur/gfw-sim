# gfw_sim/api/schema.py
from __future__ import annotations

from typing import Dict, List, Optional, Any
from pydantic import BaseModel

class SimulationSummaryModel(BaseModel):
    total_cost_daily_usd: float
    total_cost_gfw_nodes_usd: float
    total_cost_keda_nodes_usd: float

class NodePartsModel(BaseModel):
    gfw_cpu_m: int
    ds_cpu_m: int
    other_cpu_m: int
    gfw_mem_b: int
    ds_mem_b: int
    other_mem_b: int

class NodeRowModel(BaseModel):
    node: str
    nodepool: str
    instance: str
    gfw_ratio_pct: float
    alloc_cpu_m: int
    alloc_mem_b: int
    sum_req_cpu_m: int
    sum_req_mem_b: int
    ram_util_pct: float
    ram_ds_gib: float
    ram_gfw_gib: float
    cost_daily_usd: float
    parts: NodePartsModel
    is_virtual: bool
    price_missing: bool

class PodViewModel(BaseModel):
    pod_id: str
    namespace: str
    name: str
    owner_kind: Optional[str]
    owner_name: Optional[str]
    is_gfw: bool
    is_daemon: bool
    is_system: bool
    req_cpu_m: int
    req_mem_b: int

class SimulationResponse(BaseModel):
    summary: SimulationSummaryModel
    nodes: List[NodeRowModel]
    pods_by_node: Dict[str, List[PodViewModel]]
    violations: Dict[str, List[str]]

# --- Новые структуры для редактирования и планирования ---

class PodPatchSpec(BaseModel):
    """Спецификация изменений, применяемых к поду перед переносом."""
    req_cpu_m: Optional[int] = None
    req_mem_b: Optional[int] = None
    tolerations: Optional[List[Dict[str, Any]]] = None
    node_selector: Optional[Dict[str, str]] = None
    affinity: Optional[Dict[str, Any]] = None

class OperationModel(BaseModel):
    op: str  # move_namespace_to_pool, move_pods_to_pool, delete_pods, etc.
    
    # Параметры (опциональные, зависят от op)
    namespace: Optional[str] = None
    owner_kind: Optional[str] = None
    owner_name: Optional[str] = None
    node_name: Optional[str] = None
    pod_ids: Optional[List[str]] = None
    target_pool: Optional[str] = None
    
    include_system: bool = False
    include_daemonsets: bool = False
    
    # Новое поле: параметры, которые нужно применить к подам перед операцией
    overrides: Optional[PodPatchSpec] = None

class MutateRequest(BaseModel):
    operations: List[OperationModel]

class PlanMoveRequest(BaseModel):
    pod_id: str
    target_node: str

class PlanMoveResponse(BaseModel):
    pod_id: str
    owner_kind: Optional[str]
    owner_name: Optional[str]
    current_req_cpu_m: int
    current_req_mem_b: int
    
    # Предзаполненные данные, чтобы под "влез" на нодпул
    suggested_tolerations: List[Dict[str, Any]]
    suggested_node_selector: Dict[str, str]