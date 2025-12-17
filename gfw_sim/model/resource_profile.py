# gfw_sim/model/resource_profile.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ..types import CpuMillis, Bytes


@dataclass
class ResourceProfile:
    """
    Профиль потребления ресурсов pod’а.

    Всё в тех же единицах, что и в snapshot:
      - CPU: milliCPU
      - RAM: байты
    """
    # requests из спеков
    req_cpu_m: CpuMillis
    req_mem_b: Bytes

    # дневной пик (max за сутки), по данным из VM
    peak_cpu_m: Optional[CpuMillis] = None
    peak_mem_b: Optional[Bytes] = None

    # потребление на старте pod’а (N минут после запуска)
    startup_cpu_m: Optional[CpuMillis] = None
    startup_mem_b: Optional[Bytes] = None

    # доля времени в троттлинге (0..1), усреднённая по дню
    throttling_ratio: Optional[float] = None

    # на будущее: p95/p99, отдельные профили по времени суток и т.п.
