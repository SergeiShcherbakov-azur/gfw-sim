# gfw_sim/api/schema.py
from __future__ import annotations

from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field


# --- Response side ---


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
    pod_id: str = Field(..., description="Внутренний идентификатор pod'а (ns/name)")
    namespace: str
    name: str
    owner_kind: Optional[str] = None
    owner_name: Optional[str] = None
    is_gfw: bool
    is_daemon: bool
    is_system: bool
    req_cpu_m: int
    req_mem_b: int


class SimulationSummaryModel(BaseModel):
    total_cost_daily_usd: float
    total_cost_gfw_nodes_usd: float
    total_cost_keda_nodes_usd: float


class SimulationResponse(BaseModel):
    summary: SimulationSummaryModel
    nodes: List[NodeRowModel]
    pods_by_node: Dict[str, List[PodViewModel]]
    violations: Dict[str, List[str]]


# --- Request side: operations ---


class MoveNamespaceOp(BaseModel):
    op: Literal["move_namespace_to_pool"]
    namespace: str
    target_pool: str
    include_system: bool = False
    include_daemonsets: bool = False


class MoveOwnerOp(BaseModel):
    op: Literal["move_owner_to_pool"]
    namespace: str
    owner_kind: str
    owner_name: str
    target_pool: str
    include_system: bool = False
    include_daemonsets: bool = False


class MoveNodeOp(BaseModel):
    op: Literal["move_node_pods_to_pool"]
    node_name: str
    target_pool: str
    include_system: bool = False
    include_daemonsets: bool = False


class MovePodsToPoolOp(BaseModel):
    op: Literal["move_pods_to_pool"] = "move_pods_to_pool"
    pod_ids: List[str]
    target_pool: str

class DeletePodsOp(BaseModel):
    op: Literal["delete_pods"]
    pod_ids: List[str]


class DeleteNamespaceOp(BaseModel):
    op: Literal["delete_namespace"]
    namespace: str
    include_system: bool = False
    include_daemonsets: bool = False


class DeleteOwnerOp(BaseModel):
    op: Literal["delete_owner"]
    namespace: str
    owner_kind: str
    owner_name: str
    include_system: bool = False
    include_daemonsets: bool = False


class ResetOp(BaseModel):
    op: Literal["reset_to_baseline"]


OperationModel = (
    MoveNamespaceOp
    | MoveOwnerOp
    | MoveNodeOp
    | MovePodsToPoolOp
    | DeletePodsOp
    | DeleteNamespaceOp
    | DeleteOwnerOp
    | ResetOp
)


class MutateRequest(BaseModel):
    operations: List[OperationModel]
