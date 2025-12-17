from pathlib import Path
import json

from gfw_sim.snapshot.from_legacy import snapshot_from_legacy_data
from gfw_sim.sim.simulate import run_simulation
from gfw_sim.sim.packing import move_pods_to_pool
from gfw_sim.types import PodId, NodePoolName


# 1) грузим legacy.json
legacy = json.loads(Path("gfw_sim/snapshot/legacy.json").read_text("utf-8"))

snapshot = snapshot_from_legacy_data(legacy)

# 2) выбираем, какие pod'ы хотим пересадить
#    например, все GFW pod'ы из какого-то namespace
pods_to_move = [
    pid
    for pid, p in snapshot.pods.items()
    if str(p.namespace) == "bowmasters-dev1" and p.is_gfw
]

# 3) задаём целевой пул
target_pool = NodePoolName("keda-nightly-al2023-private-c")

# 4) получаем новый snapshot с пересаженными pod'ами
snapshot2 = move_pods_to_pool(snapshot, pods_to_move, target_pool)

# 5) считаем стоимость/утилизацию по новому состоянию
sim1 = run_simulation(snapshot, scenario=None)
sim2 = run_simulation(snapshot2, scenario=None)

print("Before:", sim1.total_cost_daily_usd)
print("After: ", sim2.total_cost_daily_usd)
