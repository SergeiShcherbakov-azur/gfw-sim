# gfw_sim/sim/costs.py
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional, Union

log = logging.getLogger(__name__)


@dataclass
class PricingState:
    """Состояние прайсов по типам инстансов.

    hourly_prices: on‑demand цена за час работы инстанса в USD.
    """

    region: str
    hourly_prices: Dict[str, float]


_DEFAULT_REGION = "eu-central-1"

# Базовая таблица, полученная через pricing_cli для eu-central-1
_DEFAULT_PRICES: Dict[str, float] = {
    "t3a.medium": 0.0432,
    "t3a.large": 0.0864,
    "t3a.xlarge": 0.1728,
    "r6a.large": 0.1368,
    "r6a.xlarge": 0.2736,
}

_STATE: Optional[PricingState] = PricingState(
    region=_DEFAULT_REGION,
    hourly_prices=dict(_DEFAULT_PRICES),
)


# ---------------------------------------------------------------------
# Базовый доступ к состоянию
# ---------------------------------------------------------------------


def get_state() -> PricingState:
    global _STATE
    if _STATE is None:
        _STATE = PricingState(region=_DEFAULT_REGION, hourly_prices=dict(_DEFAULT_PRICES))
    return _STATE


def set_state(state: PricingState) -> None:
    global _STATE
    _STATE = state


# ---------------------------------------------------------------------
# Загрузка/обновление прайсов
# ---------------------------------------------------------------------


def load_prices(path: Union[str, Path]) -> PricingState:
    """Загрузка прайсов из JSON-файла.

    Ожидаемый формат:
    {
      "prices": { "t3a.medium": 0.0432, ... },
      "region": "eu-central-1"
    }
    или
    {
      "hourly_prices": {...},
      "region": "..."
    }
    """
    p = Path(path)
    data = json.loads(p.read_text("utf-8"))
    prices = data.get("prices") or data.get("hourly_prices") or {}
    region = data.get("region") or _DEFAULT_REGION
    state = PricingState(
        region=region,
        hourly_prices={str(k): float(v) for k, v in prices.items()},
    )
    set_state(state)
    log.info("Loaded pricing from %s for region %s (%d instance types)", p, region, len(prices))
    return state


def load_prices_from_file(path: Path) -> PricingState:
    """Алиас для обратной совместимости."""
    return load_prices(path)


def refresh_prices_from_aws(instance_types: Iterable[str]) -> PricingState:
    """Заглушка-обновление прайсов из AWS.

    Сейчас мы *не* ходим во внешние сервисы, а лишь логируем запрос
    и оставляем текущее состояние как есть, чтобы не обнулять цены,
    если что-то пойдет не так.

    При желании сюда можно интегрировать вызов модуля pricing_cli
    или boto3 / aws cli.
    """
    state = get_state()
    types_list = list(instance_types)
    log.info(
        "refresh_prices_from_aws called for %d instance types: %s; keeping existing prices",
        len(types_list),
        types_list,
    )
    return state


# ---------------------------------------------------------------------
# Стоимость ноды
# ---------------------------------------------------------------------


def node_daily_cost_from_instance(instance_type: str, nodepool: str) -> tuple[float, bool]:
    """Стоимость ноды в день по типу инстанса и nodepool.

    Возвращает (стоимость_в_USD_в_день, price_missing).
    Всегда считаем 24 часа (без учета расписаний).
    """
    state = get_state()
    hourly = state.hourly_prices.get(instance_type)
    missing = hourly is None
    if hourly is None:
        hourly = 0.0

    # Убрали _effective_daily_hours_for_pool, теперь жестко 24 часа.
    daily_cost = hourly * 24.0
    return daily_cost, missing


def node_daily_cost(node, schedule=None, prices=None) -> float:
    """Обратная совместимость с прежним API.

    Старый вызов из packing.py: node_daily_cost(node, schedule, prices)

    Здесь мы игнорируем schedule/prices и используем глобальную таблицу
    цен (get_state), чтобы получать стоимость за день.
    """
    instance_type = getattr(node, "instance_type", "") or ""
    nodepool = getattr(node, "nodepool", "") or ""
    cost, _missing = node_daily_cost_from_instance(instance_type, nodepool)
    return cost