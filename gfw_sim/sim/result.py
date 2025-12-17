# gfw_sim/sim/result.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from ..types import CpuMillis, Bytes


@dataclass
class NodeParts:
    """Разбиение ресурсов ноды по группам pod’ов."""
    gfw_cpu_m: CpuMillis
    ds_cpu_m: CpuMillis
    other_cpu_m: CpuMillis
    gfw_mem_b: Bytes
    ds_mem_b: Bytes
    other_mem_b: Bytes


@dataclass
class NodeRow:
    """
    Строка в таблице нод, ориентирована на то, что уже есть в html_report.js.
    """
    node: str
    nodepool: str
    instance: str

    gfw_ratio_pct: float  # % pod’ов GFW от общего числа pod’ов ноды

    alloc_cpu_m: int
    alloc_mem_b: int

    sum_req_cpu_m: int
    sum_req_mem_b: int

    ram_util_pct: float      # суммарные requests / allocatable RAM, %
    ram_ds_gib: float        # RAM, занятая daemonset+system pod’ами, GiB
    ram_gfw_gib: float       # RAM, занятая GFW pod’ами, GiB

    cost_daily_usd: float    # эффективная стоимость ноды в день

    parts: NodeParts         # детальное разбиение по группам
    is_virtual: bool         # виртуальная нода (создана симулятором)
    price_missing: bool      # нет цены для instance_type


@dataclass
class PodView:
    """
    Упрощённое представление pod’а для отображения в UI при клике "pods".
    """
    namespace: str
    name: str
    owner_kind: str | None
    is_gfw: bool
    is_daemon: bool
    is_system: bool
    req_cpu_m: int
    req_mem_b: int


@dataclass
class SimulationResult:
    """
    То, что будет уходить в html_report / API.
    """
    nodes_table: List[NodeRow]

    total_cost_daily_usd: float
    total_cost_gfw_nodes_usd: float
    total_cost_keda_nodes_usd: float

    # ключ — имя ноды (node.name), а не NodeId, чтобы проще мапить в UI
    pods_by_node: Dict[str, List[PodView]]
