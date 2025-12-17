# check_backend.py
from __future__ import annotations

import json
from pathlib import Path

import requests

from gfw_sim.snapshot.from_legacy import snapshot_from_legacy_data
from gfw_sim.sim.simulate import run_simulation


BASE_URL = "http://localhost:8000"


def load_cli_simulation():
    """Запускаем симуляцию напрямую из legacy.json (без HTTP)."""
    legacy_path = Path("gfw_sim/snapshot/legacy.json")
    data = json.loads(legacy_path.read_text("utf-8"))
    snapshot = snapshot_from_legacy_data(data)
    sim = run_simulation(snapshot)
    return sim


def fetch_api_simulation():
    """Получаем результат от бекенда по /simulate."""
    resp = requests.get(f"{BASE_URL}/simulate")
    resp.raise_for_status()
    return resp.json()


def compare_summary(cli_sim, api_json):
    print("=== SUMMARY ===")
    cli_total = cli_sim.total_cost_daily_usd
    cli_gfw = cli_sim.total_cost_gfw_nodes_usd
    cli_keda = cli_sim.total_cost_keda_nodes_usd

    api_total = api_json["summary"]["total_cost_daily_usd"]
    api_gfw = api_json["summary"]["total_cost_gfw_nodes_usd"]
    api_keda = api_json["summary"]["total_cost_keda_nodes_usd"]

    print(f"CLI  total: {cli_total:.4f}")
    print(f"API  total: {api_total:.4f}")
    print(f"CLI  GFW:   {cli_gfw:.4f}")
    print(f"API  GFW:   {api_gfw:.4f}")
    print(f"CLI  KEDA:  {cli_keda:.4f}")
    print(f"API  KEDA:  {api_keda:.4f}")

    def close(a, b, eps=1e-6):
        return abs(a - b) <= eps

    ok_total = close(cli_total, api_total)
    ok_gfw = close(cli_gfw, api_gfw)
    ok_keda = close(cli_keda, api_keda)

    print("SUMMARY OK:", ok_total and ok_gfw and ok_keda)
    print()


def compare_nodes(cli_sim, api_json):
    print("=== NODES ===")
    api_nodes = api_json["nodes"]
    cli_nodes = {row.node: row for row in cli_sim.nodes_table}

    print(f"CLI nodes: {len(cli_nodes)}, API nodes: {len(api_nodes)}")

    missing_in_api = sorted(set(cli_nodes.keys()) - {n["node"] for n in api_nodes})
    missing_in_cli = sorted({n["node"] for n in api_nodes} - set(cli_nodes.keys()))

    if missing_in_api:
        print("Nodes present in CLI but missing in API:", missing_in_api)
    if missing_in_cli:
        print("Nodes present in API but missing in CLI:", missing_in_cli)

    # Проверим несколько метрик по совпадающим нодам
    print()
    print("Per-node checks (first 20 nodes):")
    count = 0
    for an in api_nodes:
        name = an["node"]
        if name not in cli_nodes:
            continue
        cn = cli_nodes[name]

        # Стоимость
        api_cost = an["cost_daily_usd"]
        cli_cost = cn.cost_daily_usd

        # alloc CPU
        api_alloc_cpu = an["alloc_cpu_m"]
        cli_alloc_cpu = cn.alloc_cpu_m

        # alloc RAM
        api_alloc_mem = an["alloc_mem_b"]
        cli_alloc_mem = cn.alloc_mem_b

        print(f"- {name}")
        print(f"  cost:  CLI={cli_cost:.6f}, API={api_cost:.6f}")
        print(f"  cpu:   CLI={cli_alloc_cpu}, API={api_alloc_cpu}")
        print(f"  mem:   CLI={cli_alloc_mem}, API={api_alloc_mem}")
        print()

        count += 1
        if count >= 20:
            break


def main():
    print("Loading CLI simulation...")
    cli_sim = load_cli_simulation()
    print("Fetching API simulation...")
    api_json = fetch_api_simulation()

    compare_summary(cli_sim, api_json)
    compare_nodes(cli_sim, api_json)


if __name__ == "__main__":
    main()
