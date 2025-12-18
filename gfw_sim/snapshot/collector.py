# gfw_sim/snapshot/collector.py
from __future__ import annotations

import logging
import re
import json
import os
import subprocess
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional

from kubernetes import client, config

from ..model.entities import Snapshot, Node, Pod, NodePool, InstancePrice, Schedule
from ..types import (
    NodeId, PodId, NodePoolName, InstanceType, Namespace, CpuMillis, Bytes, UsdPerHour
)

log = logging.getLogger(__name__)

VM_URL = "https://victoria-metrics-cluster.infra.prod.aws.eu-central-1.azurgames.dev/select/0/prometheus/api/v1/query"

def parse_cpu(quantity: str | None) -> CpuMillis:
    if not quantity: return CpuMillis(0)
    quantity = str(quantity)
    if quantity.endswith('m'): return CpuMillis(int(quantity[:-1]))
    if quantity.endswith('n'): return CpuMillis(int(int(quantity[:-1]) / 1_000_000))
    try: return CpuMillis(int(float(quantity) * 1000))
    except ValueError: return CpuMillis(0)

def parse_memory(quantity: str | None) -> Bytes:
    if not quantity: return Bytes(0)
    quantity = str(quantity)
    multipliers = {'Ki': 1024, 'Mi': 1024**2, 'Gi': 1024**3, 'Ti': 1024**4, 'K': 1000, 'M': 1000**2, 'G': 1000**3, 'T': 1000**4}
    suffix_match = re.search(r'[A-Za-z]+$', quantity)
    if suffix_match:
        suffix = suffix_match.group(0)
        number_part = quantity[:-len(suffix)]
        mult = multipliers.get(suffix, 1)
        try: return Bytes(int(float(number_part) * mult))
        except ValueError: return Bytes(0)
    try: return Bytes(int(quantity))
    except ValueError: return Bytes(0)

def _run_kubectl(args: List[str], context: str | None) -> Dict[str, Any]:
    cmd = ["kubectl"] + args + ["-o", "json"]
    if context: cmd.extend(["--context", context])
    log.info(f"Running: {' '.join(cmd)}")
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.PIPE)
        return json.loads(output)
    except subprocess.CalledProcessError as e:
        log.warning(f"kubectl command failed: {e.stderr.decode('utf-8').strip()}")
        raise

def _get_query_timestamp() -> int:
    """
    Возвращает timestamp (int) конца периода выборки (00:00:00 UTC целевого дня).
    По умолчанию: 00:00:00 UTC сегодня (выборка за вчера).
    Можно переопределить через ENV 'GFW_SNAPSHOT_DATE' (YYYY-MM-DD), чтобы выбрать конкретный день.
    """
    env_date = os.getenv("GFW_SNAPSHOT_DATE")
    if env_date:
        try:
            # Если задана дата 2025-12-16, мы хотим данные за весь этот день.
            # Значит, нам нужно окно [1d] заканчивающееся в 2025-12-17 00:00:00 UTC.
            dt = datetime.strptime(env_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            target = dt + timedelta(days=1)
            log.info(f"Using forced date from env: {env_date}. Query end time: {target}")
            return int(target.timestamp())
        except ValueError:
            log.error(f"Invalid GFW_SNAPSHOT_DATE format: {env_date}. Expected YYYY-MM-DD. Using default.")
    
    now = datetime.now(timezone.utc)
    today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(today_midnight.timestamp())

def _collect_vm_metrics() -> Dict[str, Dict[str, float]]:
    results: Dict[str, Dict[str, float]] = {}
    ts = _get_query_timestamp()
    
    q_cpu = 'max_over_time(sum(rate(container_cpu_usage_seconds_total{job="kubelet", metrics_path="/metrics/cadvisor", container!="", container!="POD", cluster="shared-dev"}[5m])) by (namespace, pod)[24h])'
    q_mem = 'max_over_time(sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", container!="", container!="POD", cluster="shared-dev"}) by (namespace, pod)[24h])'

    def _do_query(query_str):
        try:
            resp = requests.get(VM_URL, params={"query": query_str, "time": ts}, timeout=20)
            resp.raise_for_status()
            return resp.json().get("data", {}).get("result", [])
        except Exception:
            return []

    for r in _do_query(q_cpu):
        m = r.get("metric", {})
        val = r.get("value", [0, "0"])[1]
        if m.get("namespace") and m.get("pod"):
            key = f"{m['namespace']}/{m['pod']}"
            if key not in results: results[key] = {}
            try: results[key]["cpu_m"] = float(val) * 1000.0
            except: pass

    for r in _do_query(q_mem):
        m = r.get("metric", {})
        val = r.get("value", [0, "0"])[1]
        if m.get("namespace") and m.get("pod"):
            key = f"{m['namespace']}/{m['pod']}"
            if key not in results: results[key] = {}
            try: results[key]["mem_b"] = float(val)
            except: pass
    
    return results

def _collect_aws_metadata(region="eu-central-1", profile: Optional[str] = None) -> Dict[str, Any]:
    """
    Запрашивает метаданные из AWS API: Lifecycle (Spot/OnDemand) и LaunchTime.
    """
    try:
        cmd = [
            "aws", "ec2", "describe-instances",
            "--region", region,
            "--filters", "Name=instance-state-name,Values=running",
            "--query", "Reservations[].Instances[].{Name:PrivateDnsName, Lifecycle:InstanceLifecycle, LaunchTime:LaunchTime}",
            "--output", "json"
        ]
        
        if profile:
            cmd.extend(["--profile", profile])
            
        log.info(f"Fetching AWS EC2 metadata (profile={profile})...")
        res = subprocess.check_output(cmd, stderr=subprocess.PIPE)
        data = json.loads(res)
        
        result = {}
        now = datetime.now(timezone.utc)
        
        for item in data:
            name = item.get("Name")
            if not name: continue
            
            lifecycle = item.get("Lifecycle") or "on_demand"
            launch_time_str = item.get("LaunchTime")
            uptime_hours = 24.0
            
            if launch_time_str:
                try:
                    lt = datetime.fromisoformat(launch_time_str.replace("Z", "+00:00"))
                    delta = now - lt
                    uptime_hours = min(24.0, delta.total_seconds() / 3600.0)
                except Exception:
                    pass

            result[name] = {
                "capacity_type": lifecycle,
                "uptime_hours": uptime_hours
            }
            
        log.info(f"Collected AWS metadata for {len(result)} instances.")
        return result
    except subprocess.CalledProcessError as e:
        log.error(f"AWS CLI failed: {e.stderr.decode('utf-8', errors='ignore')}")
        return {}
    except Exception as e:
        log.error(f"Unexpected error collecting AWS metadata: {e}")
        return {}

def _collect_historical_usage() -> List[Dict[str, Any]]:
    """
    Считает суммарное время работы (Instance-Hours) за ВЧЕРАШНИЕ сутки.
    Реализует JOIN на стороне Python для максимальной надежности.
    """
    history = []
    ts = _get_query_timestamp()
    
    start_dt = datetime.fromtimestamp(ts - 86400, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    log.info(f"Querying history for window: {start_dt} -> {end_dt} (UTC)")
    
    # 1. Запрашиваем Usage (Часы работы по нодам)
    # Используем 'up' от kubelet, так как он есть всегда, пока нода жива.
    # max by (node) убирает дубликаты.
    # [1d:1m] нормализует в минуты.
    q_usage = """
    sum(
      sum_over_time(
        (max by (node) (up{job="kubelet", cluster="shared-dev"} == 1))[1d:1m]
      )
    ) by (node) / 60
    """
    
    # 2. Запрашиваем Metadata (Лейблы по нодам)
    # last_over_time находит последние известные лейблы за сутки.
    q_meta = """
    last_over_time(
      kube_node_labels{
        cluster="shared-dev",
        label_karpenter_sh_nodepool!=""
      }[1d]
    )
    """
    
    node_usage_map: Dict[str, float] = {}
    node_meta_map: Dict[str, Dict[str, str]] = {}
    
    try:
        # Fetch Usage
        clean_q_usage = re.sub(r'\s+', ' ', q_usage).strip()
        log.info(f"Querying per-node usage (metric: up)...")
        resp = requests.get(VM_URL, params={"query": clean_q_usage, "time": ts}, timeout=60)
        resp.raise_for_status()
        for r in resp.json().get("data", {}).get("result", []):
            node = r.get("metric", {}).get("node")
            try:
                hours = float(r.get("value", [0, "0"])[1])
                if node and hours > 0:
                    node_usage_map[node] = hours
            except ValueError: pass

        # Fetch Metadata
        clean_q_meta = re.sub(r'\s+', ' ', q_meta).strip()
        log.info(f"Querying per-node metadata...")
        resp = requests.get(VM_URL, params={"query": clean_q_meta, "time": ts}, timeout=60)
        resp.raise_for_status()
        for r in resp.json().get("data", {}).get("result", []):
            metric = r.get("metric", {})
            node = metric.get("node")
            pool = metric.get("label_karpenter_sh_nodepool")
            inst = metric.get("label_node_kubernetes_io_instance_type")
            if node and pool:
                node_meta_map[node] = {
                    "pool": pool,
                    "instance": inst or "unknown"
                }

        # Python Join & Aggregate
        aggregated: Dict[tuple, float] = {}
        missing_meta_count = 0
        
        for node, hours in node_usage_map.items():
            meta = node_meta_map.get(node)
            if not meta:
                # Попробуем найти ноду без домена, если метрики отличаются
                short_name = node.split('.')[0]
                for k, v in node_meta_map.items():
                    if k.startswith(short_name):
                        meta = v
                        break
            
            if not meta:
                missing_meta_count += 1
                continue 

            pool = meta["pool"]
            inst = meta["instance"]
            key = (pool, inst)
            aggregated[key] = aggregated.get(key, 0.0) + hours
            
        if missing_meta_count > 0:
            log.warning(f"Skipped {missing_meta_count} nodes due to missing metadata (labels).")

        for (pool, inst), hours in aggregated.items():
            history.append({
                "pool": pool,
                "instance": inst,
                "instance_hours_24h": hours
            })
            
        log.info(f"Collected history: {len(history)} groups processed from {len(node_usage_map)} nodes.")
        
    except Exception as e:
        log.warning(f"History collection failed: {e}")
        
    return history

def _collect_via_kubectl(context: str | None, aws_profile: str | None) -> Snapshot:
    metrics_map = _collect_vm_metrics()
    history_data = _collect_historical_usage()
    aws_meta = _collect_aws_metadata(profile=aws_profile)

    log.info("Fetching Nodes via kubectl...")
    nodes_data = _run_kubectl(["get", "nodes"], context).get("items", [])
    
    log.info("Fetching Pods via kubectl...")
    pods_data = _run_kubectl(["get", "pods", "--all-namespaces", "--field-selector=status.phase=Running"], context).get("items", [])
    
    nodepools: Dict[NodePoolName, NodePool] = {}
    kp_pools_items = []
    try:
        data = _run_kubectl(["get", "nodepools.karpenter.sh"], context)
        kp_pools_items = data.get("items", [])
    except Exception:
        pass

    for item in kp_pools_items:
        meta = item.get("metadata", {})
        spec = item.get("spec", {})
        name_str = meta.get("name")
        if not name_str: continue
        name = NodePoolName(name_str)
        labels = spec.get("template", {}).get("metadata", {}).get("labels", {})
        taints = [{"key": t.get("key"), "value": t.get("value"), "effect": t.get("effect")} 
                  for t in spec.get("template", {}).get("spec", {}).get("taints", [])]
        is_keda = "keda" in name_str.lower()
        if is_keda and not any(t["key"] == "keda_nightly" for t in taints):
            taints.append({"key": "keda_nightly", "value": "true", "effect": "NoSchedule"})
        nodepools[name] = NodePool(name=name, labels=labels, taints=taints, is_keda=is_keda, schedule_name="keda-weekdays-12h" if is_keda else "default")

    nodes: Dict[NodeId, Node] = {}
    for kn in nodes_data:
        meta = kn.get("metadata", {})
        spec = kn.get("spec", {})
        status = kn.get("status", {})
        labels = meta.get("labels", {})
        name = meta.get("name")
        pool_name = NodePoolName(labels.get("karpenter.sh/nodepool") or labels.get("node.kubernetes.io/instance-group") or "default")
        instance_type = labels.get("node.kubernetes.io/instance-type") or "unknown"
        alloc = status.get("allocatable", {})
        
        if pool_name not in nodepools:
             is_keda = "keda" in str(pool_name).lower()
             nodepools[pool_name] = NodePool(name=pool_name, is_keda=is_keda, schedule_name="keda-weekdays-12h" if is_keda else "default")
        
        node_taints = [{"key": t.get("key"), "value": t.get("value"), "effect": t.get("effect")} for t in spec.get("taints", [])]
        
        am = aws_meta.get(name, {})
        real_uptime = am.get("uptime_hours", 24.0)
        real_capacity = am.get("capacity_type", "on_demand")
        
        nodes[NodeId(name)] = Node(
            id=NodeId(name), name=name, nodepool=pool_name, instance_type=InstanceType(instance_type),
            alloc_cpu_m=parse_cpu(alloc.get("cpu")), alloc_mem_b=parse_memory(alloc.get("memory")),
            capacity_type=real_capacity,
            labels=labels, taints=node_taints, 
            uptime_hours_24h=real_uptime
        )

    pods: Dict[PodId, Pod] = {}
    for kp in pods_data:
        meta = kp.get("metadata", {})
        spec = kp.get("spec", {})
        node_name = spec.get("nodeName")
        if not node_name: continue
        pod_id = PodId(f"{meta.get('namespace')}/{meta.get('name')}")
        
        req_cpu_acc = 0
        req_mem_acc = 0
        for c in (spec.get("containers") or []) + (spec.get("initContainers") or []):
             res = c.get("resources", {}).get("requests", {})
             if res:
                 req_cpu_acc += int(parse_cpu(res.get("cpu")))
                 req_mem_acc += int(parse_memory(res.get("memory")))
        
        owner_kind = meta["ownerReferences"][0].get("kind") if meta.get("ownerReferences") else None
        owner_name = meta["ownerReferences"][0].get("name") if meta.get("ownerReferences") else None
        is_ds = (owner_kind == "DaemonSet")
        is_system = (meta.get("namespace") in ["kube-system", "monitoring", "logging", "ingress-nginx"])
        tols = [{"key": t.get("key"), "operator": t.get("operator"), "value": t.get("value"), "effect": t.get("effect")} for t in spec.get("tolerations", [])]
        
        usage = metrics_map.get(str(pod_id), {})
        u_cpu = CpuMillis(int(usage.get("cpu_m", 0))) if "cpu_m" in usage else None
        u_mem = Bytes(int(usage.get("mem_b", 0))) if "mem_b" in usage else None
        
        pods[pod_id] = Pod(
            id=pod_id, name=meta.get("name"), namespace=Namespace(meta.get("namespace")),
            node=NodeId(node_name) if node_name in nodes else None,
            owner_kind=owner_kind, owner_name=owner_name,
            req_cpu_m=CpuMillis(req_cpu_acc), req_mem_b=Bytes(req_mem_acc),
            is_daemonset=is_ds, is_system=is_system, is_gfw=(not is_ds and not is_system),
            tolerations=tols, node_selector=spec.get("nodeSelector") or {},
            usage_cpu_m=u_cpu, usage_mem_b=u_mem
        )

    return _create_snapshot_result(nodes, pods, nodepools, history_data)

def _create_snapshot_result(nodes, pods, nodepools, history) -> Snapshot:
    schedules = {
        "default": Schedule(name="default", hours_per_day=24.0, days_per_week=7.0),
        "keda-weekdays-12h": Schedule(name="keda-weekdays-12h", hours_per_day=12.0, days_per_week=5.0)
    }
    return Snapshot(
        nodes=nodes, pods=pods, nodepools=nodepools, 
        prices={}, schedules=schedules, 
        keda_pool_name=NodePoolName("keda-nightly-al2023-private-c"),
        history_usage=history
    )

def collect_k8s_snapshot(k8s_context: str | None = None, method: str = "kubectl", aws_profile: str | None = "shared-dev") -> Snapshot:
    if method == "kubectl":
        try:
            return _collect_via_kubectl(k8s_context, aws_profile)
        except Exception as e:
            log.error(f"kubectl failed: {e}, falling back...")
            return _collect_via_kubectl(k8s_context, aws_profile)
    return _collect_via_kubectl(k8s_context, aws_profile)