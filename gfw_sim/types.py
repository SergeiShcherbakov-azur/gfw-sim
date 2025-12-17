# gfw_sim/types.py
from __future__ import annotations

from dataclasses import dataclass
from typing import NewType


# ID-шники / имена
NodeId = NewType("NodeId", str)
PodId = NewType("PodId", str)
NodePoolName = NewType("NodePoolName", str)
InstanceType = NewType("InstanceType", str)
Namespace = NewType("Namespace", str)

# Ресурсы
CpuMillis = NewType("CpuMillis", int)  # milliCPU
Bytes = NewType("Bytes", int)          # байты

# Деньги
UsdPerHour = NewType("UsdPerHour", float)


@dataclass(frozen=True)
class PriceKey:
    """
    Ключ для поиска цены инстанса.
    Сейчас: только тип + on_demand/spot.
    При необходимости потом добавим OS, license, tenancy и т.д.
    """
    instance_type: InstanceType
    purchasing: str = "on_demand"  # или "spot"
