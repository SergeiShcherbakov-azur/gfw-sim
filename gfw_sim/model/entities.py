# gfw_sim/model/entities.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any

from ..types import (
    NodeId, PodId, NodePoolName, InstanceType, Namespace, CpuMillis, Bytes, UsdPerHour
)

@dataclass
class NodePool:
    name: NodePoolName
    labels: Dict[str, str] = field(default_factory=dict)
    taints: List[Dict[str, str]] = field(default_factory=list)
    is_keda: bool = False
    schedule_name: str = "default"
    consolidation_policy: str = "WhenUnderutilized"

@dataclass
class Node:
    id: NodeId
    name: str
    nodepool: NodePoolName
    instance_type: InstanceType
    alloc_cpu_m: CpuMillis
    alloc_mem_b: Bytes
    # --- NEW: Лимит подов (Max Pods) ---
    alloc_pods: int = 110 
    
    capacity_type: str = "on_demand"
    labels: Dict[str, str] = field(default_factory=dict)
    taints: List[Dict[str, str]] = field(default_factory=list)
    is_virtual: bool = False
    uptime_hours_24h: float = 24.0

@dataclass
class Pod:
    id: PodId
    name: str
    namespace: Namespace
    node: Optional[NodeId]
    owner_kind: Optional[str]
    owner_name: Optional[str]
    req_cpu_m: CpuMillis
    req_mem_b: Bytes
    limit_cpu_m: Optional[CpuMillis] = None
    limit_mem_b: Optional[Bytes] = None
    is_daemonset: bool = False
    is_system: bool = False
    is_gfw: bool = False

    tolerations: List[Dict[str, Any]] = field(default_factory=list)
    node_selector: Dict[str, str] = field(default_factory=dict)
    affinity: Dict[str, Any] = field(default_factory=dict)

    usage_cpu_m: Optional[CpuMillis] = None
    usage_mem_b: Optional[Bytes] = None
    active_ratio: float = 1.0

@dataclass
class InstancePrice:
    instance_type: InstanceType
    usd_per_hour: UsdPerHour
    purchasing: str = "on_demand"
    source: str = "unknown"

@dataclass
class Schedule:
    name: str
    hours_per_day: float = 24.0
    days_per_week: float = 7.0

    @property
    def effective_hours_per_day(self) -> float:
        return self.hours_per_day * (self.days_per_week / 7.0)

@dataclass
class Snapshot:
    nodes: Dict[NodeId, Node]
    pods: Dict[PodId, Pod]
    nodepools: Dict[NodePoolName, NodePool]
    prices: Dict[InstanceType, InstancePrice]
    schedules: Dict[str, Schedule]
    keda_pool_name: Optional[NodePoolName] = None
    history_usage: List[Dict[str, Any]] = field(default_factory=list)