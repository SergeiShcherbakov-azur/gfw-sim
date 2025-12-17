from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import dataclass
from typing import Dict, List, Optional
from pathlib import Path


# Location name, как его видит Pricing API, для региона eu-central-1.
# При необходимости дополни таблицу другими регионами.
REGION_LOCATION = {
    "eu-central-1": "EU (Frankfurt)",
    "eu-west-1": "EU (Ireland)",
    "eu-west-2": "EU (London)",
    "us-east-1": "US East (N. Virginia)",
    "us-east-2": "US East (Ohio)",
    "us-west-2": "US West (Oregon)",
}


@dataclass
class PricingQuery:
    instance_type: str
    region_code: str = "eu-central-1"
    operating_system: str = "Linux"
    preinstalled_sw: str = "NA"
    tenancy: str = "Shared"
    capacity_status: str = "Used"


def _build_filters(q: PricingQuery) -> List[str]:
    """
    Собираем список фильтров для aws pricing get-products.

    Важно: region_code (eu-central-1) конвертируем в location ("EU (Frankfurt)").
    """
    location = REGION_LOCATION.get(q.region_code)
    if not location:
        raise ValueError(f"Unknown region_code={q.region_code}, add it to REGION_LOCATION")

    filters = [
        f"Type=TERM_MATCH,Field=instanceType,Value={q.instance_type}",
        f"Type=TERM_MATCH,Field=location,Value={location}",
        f"Type=TERM_MATCH,Field=operatingSystem,Value={q.operating_system}",
        f"Type=TERM_MATCH,Field=preInstalledSw,Value={q.preinstalled_sw}",
        f"Type=TERM_MATCH,Field=tenancy,Value={q.tenancy}",
        f"Type=TERM_MATCH,Field=capacitystatus,Value={q.capacity_status}",
        # Можно добавлять ещё:
        # "Type=TERM_MATCH,Field=marketoption,Value=OnDemand",
        # "Type=TERM_MATCH,Field=licenseModel,Value=No License required",
        # "Type=TERM_MATCH,Field=operation,Value=RunInstances",
    ]
    return filters


def _run_pricing_cli(filters: List[str], profile: Optional[str] = None) -> dict:
    """
    Вызов aws pricing get-products и парсинг JSON.

    ВАЖНО: region для сервиса pricing всегда us-east-1 (так рекомендует AWS).
    """
    cmd = [
        "aws",
        "pricing",
        "get-products",
        "--service-code",
        "AmazonEC2",
        "--region",
        "us-east-1",
        "--output",
        "json",
        "--format-version",
        "aws_v1",
        "--max-results",
        "1",  # нам достаточно одного совпадения
        "--filters",
    ]
    cmd.extend(filters)

    if profile:
        cmd.extend(["--profile", profile])

    proc = subprocess.run(
        cmd,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(proc.stdout)


def _extract_ondemand_price_usd(resp: dict) -> float:
    """
    Из ответа pricing вытащить цену OnDemand в USD за 1 час.
    """
    price_list = resp.get("PriceList")
    if not price_list:
        raise RuntimeError("Pricing API returned empty PriceList")

    # PriceList — это массив строк JSON, каждая строка — вложенный JSON.
    obj = json.loads(price_list[0])

    terms = obj["terms"]["OnDemand"]
    # terms: dict[offerTermCode -> {...}]
    term = next(iter(terms.values()))
    price_dimensions = term["priceDimensions"]
    dim = next(iter(price_dimensions.values()))

    unit = dim["unit"]
    if unit not in ("Hrs", "Hour"):
        # на всякий случай, чтобы не считать что-то типа GB-Mo как часы
        raise RuntimeError(f"Unexpected unit in pricing: {unit}")

    price_str = dim["pricePerUnit"]["USD"]
    return float(price_str)


def fetch_hourly_price(
    instance_type: str,
    region_code: str = "eu-central-1",
    profile: Optional[str] = None,
) -> float:
    """
    Получить on-demand стоимость 1 часа работы instance_type в регионе region_code.

    Использует AWS Pricing API через aws-cli.
    """
    q = PricingQuery(instance_type=instance_type, region_code=region_code)
    filters = _build_filters(q)
    resp = _run_pricing_cli(filters, profile=profile)
    return _extract_ondemand_price_usd(resp)


def fetch_hourly_prices_bulk(
    instance_types: List[str],
    region_code: str = "eu-central-1",
    profile: Optional[str] = None,
) -> Dict[str, float]:
    """
    Массовый запрос цен для списка instance_types.
    """
    result: Dict[str, float] = {}
    for inst in instance_types:
        price = fetch_hourly_price(inst, region_code=region_code, profile=profile)
        result[inst] = price
    return result


# ---------------------------
# CLI
# ---------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch EC2 hourly prices from AWS Pricing API via aws-cli.",
    )
    parser.add_argument(
        "--instances",
        required=True,
        help="Список instance types через запятую, например: t3a.medium,r6a.large",
    )
    parser.add_argument(
        "--region",
        default="eu-central-1",
        help="Код региона (по умолчанию eu-central-1).",
    )
    parser.add_argument(
        "--profile",
        help="Имя профиля AWS CLI.",
    )
    parser.add_argument(
        "--out",
        help="Путь к JSON-файлу для записи результата. Если не указан, печатаем в stdout.",
    )
    return parser.parse_args()


def main_cli() -> None:
    args = _parse_args()
    instances = [x.strip() for x in args.instances.split(",") if x.strip()]
    prices = fetch_hourly_prices_bulk(
        instances,
        region_code=args.region,
        profile=args.profile,
    )

    data = {
        "region": args.region,
        "prices": prices,
    }

    if args.out:
        Path(args.out).write_text(json.dumps(data, indent=2, sort_keys=True))
        print(f"Written prices to {args.out}")
    else:
        print(json.dumps(data, indent=2, sort_keys=True))


if __name__ == "__main__":
    main_cli()
