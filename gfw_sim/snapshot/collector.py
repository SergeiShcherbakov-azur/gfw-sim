# gfw_sim/snapshot/collector.py
from __future__ import annotations

import logging
import re
import json
import os
import subprocess
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Tuple

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
    env_date = os.getenv("GFW_SNAPSHOT_DATE")
    if env_date:
        try:
            dt = datetime.strptime(env_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            target = dt + timedelta(days=1)
            log.info(f"Using forced date from env: {env_date}. Query end time: {target}")
            return int(target.timestamp())
        except ValueError:
            pass
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
        
        res = subprocess.check_output(cmd, stderr=subprocess.PIPE)
        data = json.loads(res)
        result = {}
        now = datetime.now(timezone.utc)
        for item in data:
            name = item.get("Name")
            if not name: continue
            launch_time_str = item.get("LaunchTime")
            uptime_hours = 24.0
            if launch_time_str:
                try:
                    lt = datetime.fromisoformat(launch_time_str.replace("Z", "+00:00"))
                    delta = now - lt
                    uptime_hours = min(24.0, delta.total_seconds() / 3600.0)
                except Exception: pass
            result[name] = {"capacity_type": item.get("Lifecycle") or "on_demand", "uptime_hours": uptime_hours}
        return result
    except Exception as e:
        log.error(f"Failed to collect AWS metadata: {e}")
        return {}

def _collect_workload_activity() -> Dict[Tuple[str, str, str], float]:
    """
    Собирает коэффициент активности (0..1) для Workloads за 7 дней.
    Использует метрики контроллеров (Deployments, StatefulSets), что надежнее.
    """
    ts = _get_query_timestamp()
    result = {}

    # 1. DEPLOYMENTS
    q_dep = """
    avg_over_time(
      (sum by (namespace, deployment) (kube_deployment_status_replicas{cluster="shared-dev"}) > bool 0)[7d:10m]
    )
    """
    
    # 2. STATEFULSETS
    q_sts = """
    avg_over_time(
      (sum by (namespace, statefulset) (kube_statefulset_status_replicas{cluster="shared-dev"}) > bool 0)[7d:10m]
    )
    """

    def fetch(query, label_key, kind_name):
        try:
            clean_q = re.sub(r'\s+', ' ', query).strip()
            resp = requests.get(VM_URL, params={"query": clean_q, "time": ts}, timeout=60)
            if not resp.ok: 
                log.warning(f"Activity query failed for {kind_name}: {resp.status_code}")
                return
            
            data = resp.json().get("data", {}).get("result", [])
            for r in data:
                m = r.get("metric", {})
                ns = m.get("namespace")
                name = m.get(label_key)
                val = r.get("value", [0, "0"])[1]
                
                if ns and name:
                    try:
                        result[(ns, name, kind_name)] = float(val)
                    except ValueError: pass
        except Exception as e:
            log.warning(f"Error collecting {kind_name} activity: {e}")

    log.info("Collecting workload activity (Deployments)...")
    fetch(q_dep, "deployment", "Deployment")
    
    log.info("Collecting workload activity (StatefulSets)...")
    fetch(q_sts, "statefulset", "StatefulSet")
    
    log.info(f"Collected activity stats for {len(result)} workloads.")
    return result

def _collect_historical_usage() -> List[Dict[str, Any]]:
    history = []
    ts = _get_query_timestamp()
    q_usage = 'sum(sum_over_time((max by (node) (up{job="kubelet", cluster="shared-dev"} == 1))[1d:1m])) by (node) / 60'
    q_meta = 'last_over_time(kube_node_labels{cluster="shared-dev", label_karpenter_sh_nodepool!=""}[1d])'
    
    node_usage_map = {}
    node_meta_map = {}
    
    try:
        log.info(f"Querying history usage...")
        resp = requests.get(VM_URL, params={"query": q_usage, "time": ts}, timeout=60)
        if resp.ok:
            for r in resp.json().get("data", {}).get("result", []):
                try: node_usage_map[r["metric"]["node"]] = float(r["value"][1])
                except: pass
        
        resp = requests.get(VM_URL, params={"query": q_meta, "time": ts}, timeout=60)
        if resp.ok:
            for r in resp.json().get("data", {}).get("result", []):
                m = r["metric"]
                if "node" in m and "label_karpenter_sh_nodepool" in m:
                    node_meta_map[m["node"]] = {
                        "pool": m["label_karpenter_sh_nodepool"], 
                        "instance": m.get("label_node_kubernetes_io_instance_type", "unknown")
                    }

        aggregated = {}
        for node, hours in node_usage_map.items():
            meta = node_meta_map.get(node)
            if not meta:
                short = node.split('.')[0]
                for k, v in node_meta_map.items():
                    if k.startswith(short): meta = v; break
            
            if meta:
                key = (meta["pool"], meta["instance"])
                aggregated[key] = aggregated.get(key, 0.0) + hours
        
        for (pool, inst), hours in aggregated.items():
            history.append({"pool": pool, "instance": inst, "instance_hours_24h": hours})
            
    except Exception as e:
        log.warning(f"History collection failed: {e}")
        
    return history

def _collect_via_kubectl(context: str | None, aws_profile: str | None) -> Snapshot:
    metrics_map = _collect_vm_metrics()
    history_data = _collect_historical_usage()
    aws_meta = _collect_aws_metadata(profile=aws_profile)
    activity_map = _collect_workload_activity()

    log.info("Fetching Nodes via kubectl...")
    nodes_data = _run_kubectl(["get", "nodes"], context).get("items", [])
    log.info("Fetching Pods via kubectl...")
    pods_data = _run_kubectl(["get", "pods", "--all-namespaces", "--field-selector=status.phase=Running"], context).get("items", [])
    
    nodepools = {}
    try:
        kp = _run_kubectl(["get", "nodepools.karpenter.sh"], context).get("items", [])
        for item in kp:
            meta = item.get("metadata", {})
            spec = item.get("spec", {})
            name = NodePoolName(meta.get("name"))
            taints = [{"key": t.get("key"), "value": t.get("value"), "effect": t.get("effect")} for t in spec.get("template", {}).get("spec", {}).get("taints", [])]
            is_keda = "keda" in str(name).lower()
            if is_keda and not any(t["key"] == "keda_nightly" for t in taints):
                taints.append({"key": "keda_nightly", "value": "true", "effect": "NoSchedule"})
            nodepools[name] = NodePool(name=name, labels={}, taints=taints, is_keda=is_keda, schedule_name="keda-weekdays-12h" if is_keda else "default")
    except: pass

    nodes = {}
    for kn in nodes_data:
        meta = kn.get("metadata", {})
        status = kn.get("status", {})
        spec = kn.get("spec", {})
        name = meta.get("name")
        labels = meta.get("labels", {})
        pool = NodePoolName(labels.get("karpenter.sh/nodepool") or labels.get("node.kubernetes.io/instance-group") or "default")
        inst = InstanceType(labels.get("node.kubernetes.io/instance-type") or "unknown")
        
        if pool not in nodepools:
             is_keda = "keda" in str(pool).lower()
             nodepools[pool] = NodePool(name=pool, is_keda=is_keda, schedule_name="keda-weekdays-12h" if is_keda else "default")
        
        am = aws_meta.get(name, {})
        nodes[NodeId(name)] = Node(
            id=NodeId(name), name=name, nodepool=pool, instance_type=inst,
            alloc_cpu_m=parse_cpu(status.get("allocatable", {}).get("cpu")),
            alloc_mem_b=parse_memory(status.get("allocatable", {}).get("memory")),
            capacity_type=am.get("capacity_type", "on_demand"),
            labels=labels,
            taints=[{"key": t.get("key"), "value": t.get("value"), "effect": t.get("effect")} for t in spec.get("taints", [])],
            uptime_hours_24h=am.get("uptime_hours", 24.0)
        )

    pods = {}
    for kp in pods_data:
        meta = kp.get("metadata", {})
        spec = kp.get("spec", {})
        pod_id = PodId(f"{meta.get('namespace')}/{meta.get('name')}")
        
        owner_kind = meta["ownerReferences"][0].get("kind") if meta.get("ownerReferences") else None
        owner_name = meta["ownerReferences"][0].get("name") if meta.get("ownerReferences") else None
        
        req_cpu = sum(int(parse_cpu(c.get("resources",{}).get("requests",{}).get("cpu"))) for c in spec.get("containers",[]))
        req_mem = sum(int(parse_memory(c.get("resources",{}).get("requests",{}).get("memory"))) for c in spec.get("containers",[]))
        usage = metrics_map.get(str(pod_id), {})
        
        # --- MATCH WORKLOAD ACTIVITY ---
        active_ratio = 1.0
        if owner_kind and owner_name:
            # 1. Direct Match
            key = (meta.get("namespace"), owner_name, owner_kind)
            if key in activity_map:
                active_ratio = activity_map[key]
            # 2. ReplicaSet -> Deployment heuristic (e.g. "myapp-7d8f9c" -> "myapp")
            elif owner_kind == "ReplicaSet" and owner_name.rfind("-") > 0:
                # Берем часть до последнего дефиса
                dep_name = owner_name.rsplit("-", 1)[0]
                key_dep = (meta.get("namespace"), dep_name, "Deployment")
                if key_dep in activity_map:
                    active_ratio = activity_map[key_dep]
        
        pods[pod_id] = Pod(
            id=pod_id, name=meta.get("name"), namespace=Namespace(meta.get("namespace")),
            node=NodeId(spec.get("nodeName")) if spec.get("nodeName") in nodes else None,
            owner_kind=owner_kind, owner_name=owner_name,
            req_cpu_m=CpuMillis(req_cpu), req_mem_b=Bytes(req_mem),
            is_daemonset=(owner_kind=="DaemonSet"), is_system=(meta.get("namespace") in ["kube-system","monitoring"]), is_gfw=(owner_kind!="DaemonSet"),
            tolerations=[{"key":t.get("key"),"operator":t.get("operator"),"value":t.get("value"),"effect":t.get("effect")} for t in spec.get("tolerations",[])],
            node_selector=spec.get("nodeSelector") or {},
            usage_cpu_m=CpuMillis(int(usage.get("cpu_m", 0))) if "cpu_m" in usage else None,
            usage_mem_b=Bytes(int(usage.get("mem_b", 0))) if "mem_b" in usage else None,
            active_ratio=active_ratio
        )

    return Snapshot(nodes=nodes, pods=pods, nodepools=nodepools, prices={}, schedules={}, keda_pool_name=NodePoolName("keda-nightly-al2023-private-c"), history_usage=history_data)

def collect_k8s_snapshot(k8s_context: str | None = None, method: str = "kubectl", aws_profile: str | None = "shared-dev") -> Snapshot:
    return _collect_via_kubectl(k8s_context, aws_profile)