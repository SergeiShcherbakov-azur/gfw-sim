# gfw_sim/sim/overcommit.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ..model.entities import Node
from ..types import CpuMillis, Bytes


@dataclass
class NodeUsageSums:
    """
    Суммарное потребление ресурсов на ноде,
    агрегированное по всем pod’ам.
    """
    # по requests
    cpu_requests_m: CpuMillis
    mem_requests_b: Bytes

    # по суточным пикам
    cpu_peak_m: Optional[CpuMillis] = None
    mem_peak_b: Optional[Bytes] = None

    # по стартовому профилю (рестарт ноды)
    cpu_startup_m: Optional[CpuMillis] = None
    mem_startup_b: Optional[Bytes] = None


@dataclass
class NodeOvercommit:
    """
    Отношение суммарной нагрузки к allocatable по различным сценариям.
    1.0 == ровно по allocatable, >1.0 — оверкоммит.
    """
    cpu_requests_ratio: float
    mem_requests_ratio: float

    cpu_peak_ratio: Optional[float] = None
    mem_peak_ratio: Optional[float] = None

    cpu_startup_ratio: Optional[float] = None
    mem_startup_ratio: Optional[float] = None


def compute_node_overcommit(node: Node, sums: NodeUsageSums) -> NodeOvercommit:
    """
    Простейший расчёт оверкоммита:
    - requests / allocatable
    - peak / allocatable
    - startup / allocatable
    """
    def ratio(used: Optional[int], alloc: int) -> Optional[float]:
        if used is None:
            return None
        if alloc <= 0:
            return None
        return float(used) / float(alloc)

    cpu_req = ratio(int(sums.cpu_requests_m), int(node.alloc_cpu_m)) or 0.0
    mem_req = ratio(int(sums.mem_requests_b), int(node.alloc_mem_b)) or 0.0

    return NodeOvercommit(
        cpu_requests_ratio=cpu_req,
        mem_requests_ratio=mem_req,
        cpu_peak_ratio=ratio(
            int(sums.cpu_peak_m) if sums.cpu_peak_m is not None else None,
            int(node.alloc_cpu_m),
        ),
        mem_peak_ratio=ratio(
            int(sums.mem_peak_b) if sums.mem_peak_b is not None else None,
            int(node.alloc_mem_b),
        ),
        cpu_startup_ratio=ratio(
            int(sums.cpu_startup_m) if sums.cpu_startup_m is not None else None,
            int(node.alloc_cpu_m),
        ),
        mem_startup_ratio=ratio(
            int(sums.mem_startup_b) if sums.mem_startup_b is not None else None,
            int(node.alloc_mem_b),
        ),
    )
