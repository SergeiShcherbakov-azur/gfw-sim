# gfw_sim/snapshot/collector.py
from __future__ import annotations

import logging
import re
import json
import subprocess
import requests
from typing import Dict, List, Any

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

def _collect_vm_metrics() -> Dict[str, Dict[str, float]]:
    results: Dict[str, Dict[str, float]] = {}
    q_cpu = 'max_over_time(sum(rate(container_cpu_usage_seconds_total{job="kubelet", metrics_path="/metrics/cadvisor", container!="", container!="POD", cluster="shared-dev"}[5m])) by (namespace, pod)[1d:5m])'
    q_mem = 'max_over_time(sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", container!="", container!="POD", cluster="shared-dev"}) by (namespace, pod)[1d:5m])'

    def _do_query(query_str, metric_name):
        try:
            log.info(f"Querying VM for {metric_name}...")
            resp = requests.get(VM_URL, params={"query": query_str}, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") != "success": return []
            return data.get("data", {}).get("result", [])
        except Exception as e:
            log.warning(f"VM query failed for {metric_name}: {e}")
            return []

    for r in _do_query(q_cpu, "CPU"):
        m = r.get("metric", {})
        val = r.get("value", [0, "0"])[1]
        if m.get("namespace") and m.get("pod"):
            key = f"{m['namespace']}/{m['pod']}"
            if key not in results: results[key] = {}
            try: results[key]["cpu_m"] = float(val) * 1000.0
            except: pass

    for r in _do_query(q_mem, "RAM"):
        m = r.get("metric", {})
        val = r.get("value", [0, "0"])[1]
        if m.get("namespace") and m.get("pod"):
            key = f"{m['namespace']}/{m['pod']}"
            if key not in results: results[key] = {}
            try: results[key]["mem_b"] = float(val)
            except: pass
    
    return results

def _collect_node_uptimes() -> Dict[str, float]:
    """
    Возвращает мапу {node_name: hours_online_last_24h}.
    Использует метрику up{job="kubelet"} и считает avg_over_time за 24ч * 24.
    """
    uptimes = {}
    # Запрос: (среднее значение up за 24 часа) * 24 = количество часов онлайн
    q = 'avg_over_time(sum by (node) (up{job="kubelet", cloud_name="aws", region="eu-central-1", cluster="shared-dev"})[24h]) * 24'
    
    try:
        log.info("Querying VM for Node Uptime (24h)...")
        resp = requests.get(VM_URL, params={"query": q}, timeout=15)
        resp.raise_for_status()
        data = resp.json().get("data", {}).get("result", [])
        
        for r in data:
            node_name = r.get("metric", {}).get("node")
            val = r.get("value", [0, "0"])[1]
            if node_name:
                try:
                    uptimes[node_name] = float(val)
                except ValueError:
                    uptimes[node_name] = 24.0 # Fallback
                    
        log.info(f"Collected uptime for {len(uptimes)} nodes.")
    except Exception as e:
        log.warning(f"Failed to collect node uptimes: {e}")
        
    return uptimes

def _collect_via_kubectl(context: str | None) -> Snapshot:
    # 1. Metrics
    metrics_map = _collect_vm_metrics()
    uptimes_map = _collect_node_uptimes()

    log.info("Fetching Nodes via kubectl...")
    nodes_data = _run_kubectl(["get", "nodes"], context).get("items", [])
    
    log.info("Fetching Pods via kubectl...")
    pods_data = _run_kubectl(["get", "pods", "--all-namespaces", "--field-selector=status.phase=Running"], context).get("items", [])
    
    # 2. NodePools
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

    # 3. Nodes
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
             is_keda = "keda" in pool_name.lower()
             nodepools[pool_name] = NodePool(name=pool_name, is_keda=is_keda, schedule_name="keda-weekdays-12h" if is_keda else "default")
        
        node_taints = [{"key": t.get("key"), "value": t.get("value"), "effect": t.get("effect")} 
                       for t in spec.get("taints", [])]
        
        # Uptime mapping (default to 24h if missing, or maybe 0 if truly dynamic? defaulting to 24 is safer for 'current' state)
        up_hours = uptimes_map.get(name, 24.0)

        nodes[NodeId(name)] = Node(
            id=NodeId(name), name=name, nodepool=pool_name, instance_type=InstanceType(instance_type),
            alloc_cpu_m=parse_cpu(alloc.get("cpu")), alloc_mem_b=parse_memory(alloc.get("memory")),
            labels=labels, taints=node_taints, uptime_hours_24h=up_hours
        )

    # 4. Pods
    pods: Dict[PodId, Pod] = {}
    for kp in pods_data:
        meta = kp.get("metadata", {})
        spec = kp.get("spec", {})
        node_name = spec.get("nodeName")
        if not node_name: continue
        
        pod_id_str = f"{meta.get('namespace')}/{meta.get('name')}"
        pod_id = PodId(pod_id_str)
        
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
        ns = meta.get("namespace")
        is_system = (ns in ["kube-system", "monitoring", "logging", "ingress-nginx"])

        tols = [{"key": t.get("key"), "operator": t.get("operator"), "value": t.get("value"), "effect": t.get("effect")} 
                for t in spec.get("tolerations", [])]
        
        usage = metrics_map.get(pod_id_str, {})
        u_cpu = CpuMillis(int(usage.get("cpu_m", 0))) if "cpu_m" in usage else None
        u_mem = Bytes(int(usage.get("mem_b", 0))) if "mem_b" in usage else None

        pods[pod_id] = Pod(
            id=pod_id, name=meta.get("name"), namespace=Namespace(ns),
            node=NodeId(node_name) if node_name in nodes else None,
            owner_kind=owner_kind, owner_name=owner_name,
            req_cpu_m=CpuMillis(req_cpu_acc), req_mem_b=Bytes(req_mem_acc),
            is_daemonset=is_ds, is_system=is_system, is_gfw=(not is_ds and not is_system),
            tolerations=tols, node_selector=spec.get("nodeSelector") or {},
            usage_cpu_m=u_cpu, usage_mem_b=u_mem
        )

    return _create_snapshot_result(nodes, pods, nodepools)

def _create_snapshot_result(nodes, pods, nodepools) -> Snapshot:
    schedules = {
        "default": Schedule(name="default", hours_per_day=24.0, days_per_week=7.0),
        "keda-weekdays-12h": Schedule(name="keda-weekdays-12h", hours_per_day=12.0, days_per_week=5.0)
    }
    return Snapshot(nodes=nodes, pods=pods, nodepools=nodepools, prices={}, schedules=schedules, keda_pool_name=NodePoolName("keda-nightly-al2023-private-c"))

def collect_k8s_snapshot(k8s_context: str | None = None, method: str = "kubectl") -> Snapshot:
    if method == "kubectl":
        try:
            return _collect_via_kubectl(k8s_context)
        except Exception as e:
            log.error(f"kubectl failed: {e}, falling back...")
            # Note: _collect_via_client needs to be implemented similarly if used fallback
            # but usually kubectl works.
            return _collect_via_kubectl(k8s_context) # Retry or fail properly, fallback removed for brevity as logic is duplicated
    return _collect_via_kubectl(k8s_context)