from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
from pathlib import Path
from typing import Dict, Iterable, List, Tuple, Optional


# ---------------------------
# Low-level AWS CLI wrapper
# ---------------------------


def _run_aws_ce(
    start: str,
    end: str,
    group_by: List[dict],
    metrics: Iterable[str] = ("UnblendedCost",),
    profile: Optional[str] = None,
) -> dict:
    """
    Вызвать `aws ce get-cost-and-usage` и вернуть распарсенный JSON.

    :param start: YYYY-MM-DD (включительно)
    :param end: YYYY-MM-DD (исключая)
    :param group_by: [{"Type": "DIMENSION", "Key": "INSTANCE_TYPE"}, ...]
    """
    cmd = [
        "aws",
        "ce",
        "get-cost-and-usage",
        "--time-period",
        f"Start={start},End={end}",
        "--granularity",
        "DAILY",
    ]

    # metrics
    metrics_list = list(metrics)
    if not metrics_list:
        metrics_list = ["UnblendedCost"]
    cmd.extend(["--metrics"] + metrics_list)

    # group-by (Cost Explorer позволяет максимум 2 group-by)
    for gb in group_by:
        t = gb["Type"]
        k = gb["Key"]
        cmd.extend(["--group-by", f"Type={t},Key={k}"])

    # ВАЖНО:
    # Никаких фильтров по REGION / SERVICE сейчас не используем.
    # Cost Explorer глобальный, и мы полагаемся на то, что интересующие
    # нас instance types и nodepools используются в основном в нужном регионе.

    cmd.extend(["--output", "json"])

    if profile:
        cmd.extend(["--profile", profile])

    proc = subprocess.run(
        cmd,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(proc.stdout)


def _parse_results_single_day(resp: dict) -> List[dict]:
    """
    Достаём Groups для одного дня.

    В нашем сценарии [start, end) — это один день, но если дать диапазон,
    можно суммировать снаружи.
    """
    results = resp.get("ResultsByTime", [])
    if not results:
        return []

    if len(results) > 1:
        groups: List[dict] = []
        for r in results:
            groups.extend(r.get("Groups", []))
        return groups

    return results[0].get("Groups", [])


def _parse_amount_usd(group: dict, metric: str = "UnblendedCost") -> float:
    m = group["Metrics"][metric]
    return float(m["Amount"])


# ---------------------------
# Public helpers
# ---------------------------


def fetch_costs_by_instance_type(
    start: str,
    end: str,
    profile: Optional[str] = None,
) -> Dict[str, float]:
    """
    Вернёт dict: instance_type -> cost (USD) за период [start, end).

    Пример ключа: "t3a.medium".
    """
    resp = _run_aws_ce(
        start=start,
        end=end,
        group_by=[{"Type": "DIMENSION", "Key": "INSTANCE_TYPE"}],
        profile=profile,
    )
    groups = _parse_results_single_day(resp)
    result: Dict[str, float] = {}
    for g in groups:
        # Keys: ["t3a.medium"] или ["No instance type"]
        key = g["Keys"][0]
        amount = _parse_amount_usd(g)
        result[key] = amount
    return result


def fetch_costs_by_nodepool(
    start: str,
    end: str,
    profile: Optional[str] = None,
    tag_key: str = "karpenter.sh/nodepool",
) -> Dict[str, float]:
    """
    Вернёт dict: nodepool -> cost (USD) за период [start, end).

    Ключи берутся из группировки по TAG. AWS обычно формирует их как
    "tagKey$tagValue" или "$tagValue" — мы возвращаем только часть после "$".
    """
    resp = _run_aws_ce(
        start=start,
        end=end,
        group_by=[{"Type": "TAG", "Key": tag_key}],
        profile=profile,
    )
    groups = _parse_results_single_day(resp)
    result: Dict[str, float] = {}
    for g in groups:
        raw_key = g["Keys"][0]
        # "karpenter.sh/nodepool$workload-al2023-high-private-c" -> "workload-al2023-high-private-c"
        if "$" in raw_key:
            _, value = raw_key.split("$", 1)
        else:
            value = raw_key
        amount = _parse_amount_usd(g)
        result[value] = amount
    return result


def fetch_costs_by_instance_and_nodepool(
    start: str,
    end: str,
    profile: Optional[str] = None,
    tag_key: str = "karpenter.sh/nodepool",
) -> Dict[Tuple[str, str], float]:
    """
    Вернёт dict: (instance_type, nodepool) -> cost (USD) за период [start, end).

    Использует 2 group-by: INSTANCE_TYPE + TAG:karpenter.sh/nodepool.
    Это максимум, который умеет Cost Explorer (3 измерения сделать нельзя).
    """
    resp = _run_aws_ce(
        start=start,
        end=end,
        group_by=[
            {"Type": "DIMENSION", "Key": "INSTANCE_TYPE"},
            {"Type": "TAG", "Key": tag_key},
        ],
        profile=profile,
    )
    groups = _parse_results_single_day(resp)
    result: Dict[Tuple[str, str], float] = {}
    for g in groups:
        keys = g["Keys"]
        if len(keys) != 2:
            # Что-то странное, но падать не будем
            continue
        inst = keys[0]
        tag_raw = keys[1]
        if "$" in tag_raw:
            _, tag = tag_raw.split("$", 1)
        else:
            tag = tag_raw
        amount = _parse_amount_usd(g)
        result[(inst, tag)] = amount
    return result


# ---------------------------
# CLI-обёртка для ручного запуска
# ---------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch EC2 costs from AWS Cost Explorer via aws-cli.",
    )
    parser.add_argument(
        "--date",
        help=(
            "Один день в формате YYYY-MM-DD (стоимость именно за этот день). "
            "Если не задано, используется вчера."
        ),
    )
    parser.add_argument(
        "--start",
        help="Начальная дата YYYY-MM-DD (включительно). "
             "Если задано, нужно также указать --end.",
    )
    parser.add_argument(
        "--end",
        help="Конечная дата YYYY-MM-DD (исключительно).",
    )
    parser.add_argument(
        "--profile",
        default="shared-dev",
        help="Имя профиля AWS CLI.",
    )
    parser.add_argument(
        "--mode",
        choices=["instance", "nodepool", "instance-nodepool"],
        default="instance",
        help="Режим агрегации.",
    )
    parser.add_argument(
        "--tag-key",
        default="karpenter.sh/nodepool",
        help="Ключ тега для nodepool (по умолчанию karpenter.sh/nodepool).",
    )
    parser.add_argument(
        "--out",
        help=(
            "Путь для записи JSON c агрегированными костами. "
            "Если не указан, JSON печатается в stdout."
        ),
    )
    return parser.parse_args()


def _resolve_dates(args: argparse.Namespace) -> Tuple[str, str]:
    if args.start and args.end:
        return args.start, args.end

    if args.date:
        start = dt.date.fromisoformat(args.date)
    else:
        # по умолчанию — вчера
        start = dt.date.today() - dt.timedelta(days=1)
    end = start + dt.timedelta(days=1)
    return start.isoformat(), end.isoformat()


def main_cli() -> None:
    args = _parse_args()
    start, end = _resolve_dates(args)

    if args.mode == "instance":
        data = fetch_costs_by_instance_type(
            start=start,
            end=end,
            profile=args.profile,
        )
    elif args.mode == "nodepool":
        data = fetch_costs_by_nodepool(
            start=start,
            end=end,
            profile=args.profile,
            tag_key=args.tag_key,
        )
    else:
        data = fetch_costs_by_instance_and_nodepool(
            start=start,
            end=end,
            profile=args.profile,
            tag_key=args.tag_key,
        )

    if args.out:
        out_path = Path(args.out)
        out_path.write_text(json.dumps(data, indent=2, sort_keys=True))
        print(f"Written {args.mode} costs to {out_path}")
    else:
        print(json.dumps(data, indent=2, sort_keys=True))


if __name__ == "__main__":
    main_cli()
