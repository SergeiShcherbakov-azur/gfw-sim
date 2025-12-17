# gfw_sim/snapshot/collector.py
from __future__ import annotations

import logging
import re
import json
import subprocess
import requests  # <-- Нужно убедиться, что requests установлен
from typing import Dict, List, Any, Tuple

from kubernetes import client, config

from ..model.entities import Snapshot, Node, Pod, NodePool, InstancePrice, Schedule
from ..types import (
    NodeId, PodId, NodePoolName, InstanceType, Namespace, CpuMillis, Bytes, UsdPerHour
)

log = logging.getLogger(__name__)

VM_URL = "https://victoria-metrics-cluster.infra.prod.aws.eu-central-1.azurgames.dev/select/0/prometheus/api/v1/query"

def parse_cpu(quantity: str | None) -> CpuMillis:
    """Парсинг CPU из формата K8s (например, '100m', '1', '0.5') в milliCPU."""
    if not quantity:
        return CpuMillis(0)
    quantity = str(quantity)
    if quantity.endswith('m'):
        return CpuMillis(int(quantity[:-1]))
    if quantity.endswith('n'):
        return CpuMillis(int(int(quantity[:-1]) / 1_000_000))
    try:
        return CpuMillis(int(float(quantity) * 1000))
    except ValueError:
        return CpuMillis(0)

def parse_memory(quantity: str | None) -> Bytes:
    """Парсинг памяти из формата K8s (например, '1Gi', '512Mi') в байты."""
    if not quantity:
        return Bytes(0)
    quantity = str(quantity)
    
    multipliers = {
        'Ki': 1024, 'Mi': 1024**2, 'Gi': 1024**3, 'Ti': 1024**4,
        'K': 1000, 'M': 1000**2, 'G': 1000**3, 'T': 1000**4
    }
    
    suffix_match = re.search(r'[A-Za-z]+$', quantity)
    if suffix_match:
        suffix = suffix_match.group(0)
        number_part = quantity[:-len(suffix)]
        mult = multipliers.get(suffix, 1)
        try:
            return Bytes(int(float(number_part) * mult))
        except ValueError:
            return Bytes(0)
            
    try:
        return Bytes(int(quantity))
    except ValueError:
        return Bytes(0)

def _run_kubectl(args: List[str], context: str | None) -> Dict[str, Any]:
    cmd = ["kubectl"] + args + ["-o", "json"]
    if context:
        cmd.extend(["--context", context])
    
    log.info(f"Running: {' '.join(cmd)}")
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.PIPE)
        return json.loads(output)
    except subprocess.CalledProcessError as e:
        log.warning(f"kubectl command failed: {e.stderr.decode('utf-8').strip()}")
        raise

def _collect_vm_metrics() -> Dict[str, Dict[str, float]]:
    """
    Запрашивает у VictoriaMetrics пиковое потребление CPU/RAM за последние 24 часа.
    Возвращает структуру: { "namespace/pod_name": { "cpu_m": 123.0, "mem_b": 456789.0 } }
    """
    results: Dict[str, Dict[str, float]] = {}

    # CPU Query (Peak 5m rate over 1d) -> returns cores
    q_cpu = 'max_over_time(sum(rate(container_cpu_usage_seconds_total{job="kubelet", metrics_path="/metrics/cadvisor", container!="", container!="POD", cluster="shared-dev"}[5m])) by (namespace, pod)[1d:5m])'
    
    # RAM Query (Peak usage over 1d) -> returns bytes
    q_mem = 'max_over_time(sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", container!="", container!="POD", cluster="shared-dev"}) by (namespace, pod)[1d:5m])'

    def _do_query(q):
        try:
            resp = requests.get(VM_URL, params={"query": q}, timeout=10)
            resp.raise_for_status()
            return resp.json().get("data", {}).get("result", [])
        except Exception as e:
            log.warning(f"VM query failed: {e}")
            return []

    log.info("Fetching CPU metrics from VictoriaMetrics...")
    cpu_data = _do_query(q_cpu)
    for r in cpu_data:
        m = r.get("metric", {})
        val = r.get("value", [0, "0"])[1]
        ns = m.get("namespace")
        pod = m.get("pod")
        if ns and pod:
            key = f"{ns}/{pod}"
            if key not in results: results[key] = {}
            # Конвертируем cores -> milliCPU
            results[key]["cpu_m"] = float(val) * 1000.0

    log.info("Fetching RAM metrics from VictoriaMetrics...")
    mem_data = _do_query(q_mem)
    for r in mem_data:
        m = r.get("metric", {})
        val = r.get("value", [0, "0"])[1]
        ns = m.get("namespace")
        pod = m.get("pod")
        if ns and pod:
            key = f"{ns}/{pod}"
            if key not in results: results[key] = {}
            results[key]["mem_b"] = float(val)

    log.info(f"Collected usage metrics for {len(results)} pods.")
    return results

def _collect_via_client(k8s_context: str | None) -> Snapshot:
    # ... (стандартная логика загрузки конфига) ...
    try:
        if k8s_context:
            config.load_kube_config(context=k8s_context)
        else:
            try:
                config.load_kube_config()
            except config.ConfigException:
                config.load_incluster_config()
    except Exception as e:
        log.error(f"Failed to load k8s config: {e}")
        raise

    v1 = client.CoreV1Api()
    cust = client.CustomObjectsApi()

    # --- Metrics ---
    metrics_map = _collect_vm_metrics()

    log.info("Fetching Nodes from K8s (client)...")
    k8s_nodes = v1.list_node().items
    
    log.info("Fetching Pods from K8s (client)...")
    k8s_pods = v1.list_pod_for_all_namespaces(field_selector="status.phase=Running").items

    # --- 1. NodePools ---
    nodepools: Dict[NodePoolName, NodePool] = {}
    kp_pools_items = []
    versions_to_try = ["v1", "v1beta1"]

    for version in versions_to_try:
        try:
            resp = cust.list_cluster_custom_object(group="karpenter.sh", version=version, plural="nodepools")
            kp_pools_items = resp.get("items", [])
            if kp_pools_items: break
        except client.exceptions.ApiException:
            continue

    for item in kp_pools_items:
        metadata = item.get("metadata", {})
        spec = item.get("spec", {})
        template = spec.get("template", {})
        spec_meta = template.get("metadata", {})
        
        name_str = metadata.get("name")
        if not name_str: continue
        name = NodePoolName(name_str)
        labels = spec_meta.get("labels", {})
        taints_raw = spec.get("template", {}).get("spec", {}).get("taints", [])
        
        taints = []
        for t in taints_raw:
            taints.append({"key": t.get("key"), "value": t.get("value"), "effect": t.get("effect")})

        is_keda = "keda" in name_str.lower() or "nightly" in name_str.lower()
        if is_keda and not any(t["key"] == "keda_nightly" for t in taints):
             taints.append({"key": "keda_nightly", "value": "true", "effect": "NoSchedule"})

        nodepools[name] = NodePool(
            name=name, labels=labels, taints=taints, is_keda=is_keda,
            schedule_name="keda-weekdays-12h" if is_keda else "default"
        )

    # --- 2. Nodes ---
    nodes: Dict[NodeId, Node] = {}
    for kn in k8s_nodes:
        meta = kn.metadata
        spec = kn.spec
        status = kn.status
        labels = meta.labels or {}
        name = meta.name
        
        pool_name_str = labels.get("karpenter.sh/nodepool") or labels.get("node.kubernetes.io/instance-group") or "default"
        pool_name = NodePoolName(pool_name_str)
        instance_type_str = labels.get("node.kubernetes.io/instance-type") or "unknown"
        
        alloc = status.allocatable or {}
        cpu_m = parse_cpu(alloc.get("cpu"))
        mem_b = parse_memory(alloc.get("memory"))

        node_taints = []
        if spec.taints:
            for t in spec.taints:
                node_taints.append({"key": t.key, "value": t.value, "effect": t.effect})

        if pool_name not in nodepools:
            # Inference logic same as before...
            is_keda = "keda" in pool_name_str.lower() or "nightly" in pool_name_str.lower()
            inferred_taints = list(node_taints)
            if is_keda and not any(t["key"] == "keda_nightly" for t in inferred_taints):
                 inferred_taints.append({"key": "keda_nightly", "value": "true", "effect": "NoSchedule"})
            
            nodepools[pool_name] = NodePool(
                name=pool_name, labels=labels, taints=inferred_taints, is_keda=is_keda,
                schedule_name="keda-weekdays-12h" if is_keda else "default"
            )

        nodes[NodeId(name)] = Node(
            id=NodeId(name), name=name, nodepool=pool_name,
            instance_type=InstanceType(instance_type_str),
            alloc_cpu_m=cpu_m, alloc_mem_b=mem_b, labels=labels, taints=node_taints
        )

    # --- 3. Pods ---
    pods: Dict[PodId, Pod] = {}
    for kp in k8s_pods:
        meta = kp.metadata
        spec = kp.spec
        if not spec.node_name: continue
            
        pod_id_str = f"{meta.namespace}/{meta.name}"
        pod_id = PodId(pod_id_str)
        
        req_cpu_acc = 0
        req_mem_acc = 0
        all_containers = (spec.containers or []) + (spec.init_containers or [])
        for c in all_containers:
            res = c.resources
            if res and res.requests:
                req_cpu_acc += int(parse_cpu(res.requests.get("cpu")))
                req_mem_acc += int(parse_memory(res.requests.get("memory")))
        
        owner_kind, owner_name = None, None
        if meta.owner_references:
            owner_kind = meta.owner_references[0].kind
            owner_name = meta.owner_references[0].name
            
        is_ds = (owner_kind == "DaemonSet")
        is_system = (meta.namespace in ["kube-system", "monitoring", "logging", "ingress-nginx"])
        is_gfw = (not is_ds) and (not is_system)

        pod_tols = []
        if spec.tolerations:
            for t in spec.tolerations:
                pod_tols.append({"key": t.key, "operator": t.operator, "value": t.value, "effect": t.effect})
                
        # Metrics Mapping
        usage = metrics_map.get(pod_id_str, {})
        u_cpu = CpuMillis(int(usage.get("cpu_m", 0))) if "cpu_m" in usage else None
        u_mem = Bytes(int(usage.get("mem_b", 0))) if "mem_b" in usage else None

        pods[pod_id] = Pod(
            id=pod_id, name=meta.name, namespace=Namespace(meta.namespace),
            node=NodeId(spec.node_name) if spec.node_name in nodes else None,
            owner_kind=owner_kind, owner_name=owner_name,
            req_cpu_m=CpuMillis(req_cpu_acc), req_mem_b=Bytes(req_mem_acc),
            is_daemonset=is_ds, is_system=is_system, is_gfw=is_gfw,
            tolerations=pod_tols, node_selector=spec.node_selector or {}, affinity=(spec.affinity.to_dict() if spec.affinity else {}),
            usage_cpu_m=u_cpu, usage_mem_b=u_mem
        )

    return _create_snapshot_result(nodes, pods, nodepools)

def _collect_via_kubectl(context: str | None) -> Snapshot:
    # --- Metrics ---
    metrics_map = _collect_vm_metrics()

    log.info("Fetching Nodes via kubectl...")
    nodes_data = _run_kubectl(["get", "nodes"], context).get("items", [])
    
    log.info("Fetching Pods via kubectl...")
    pods_data = _run_kubectl(["get", "pods", "--all-namespaces", "--field-selector=status.phase=Running"], context).get("items", [])
    
    # ... (NodePool logic similar to above) ...
    nodepools: Dict[NodePoolName, NodePool] = {}
    kp_pools_items = []
    
    try:
        data = _run_kubectl(["get", "nodepools.karpenter.sh"], context)
        kp_pools_items = data.get("items", [])
    except Exception:
        pass

    for item in kp_pools_items:
        metadata = item.get("metadata", {})
        spec = item.get("spec", {})
        name_str = metadata.get("name")
        if not name_str: continue
        name = NodePoolName(name_str)
        # ... (taint/label extraction same as before) ...
        taints_raw = spec.get("template", {}).get("spec", {}).get("taints", [])
        taints = [{"key": t.get("key"), "value": t.get("value"), "effect": t.get("effect")} for t in taints_raw]
        is_keda = "keda" in name_str.lower()
        if is_keda and not any(t["key"] == "keda_nightly" for t in taints):
             taints.append({"key": "keda_nightly", "value": "true", "effect": "NoSchedule"})
        nodepools[name] = NodePool(name=name, taints=taints, is_keda=is_keda, schedule_name="keda-weekdays-12h" if is_keda else "default")

    # Nodes
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
        
        # ... (Node pool inference same as before) ...
        if pool_name not in nodepools:
             is_keda = "keda" in str(pool_name).lower()
             nodepools[pool_name] = NodePool(name=pool_name, is_keda=is_keda, schedule_name="keda-weekdays-12h" if is_keda else "default")
        
        raw_taints = spec.get("taints", [])
        node_taints = [{"key": t.get("key"), "value": t.get("value"), "effect": t.get("effect")} for t in raw_taints]

        nodes[NodeId(name)] = Node(
            id=NodeId(name), name=name, nodepool=pool_name, instance_type=InstanceType(instance_type),
            alloc_cpu_m=parse_cpu(alloc.get("cpu")), alloc_mem_b=parse_memory(alloc.get("memory")),
            labels=labels, taints=node_taints
        )

    # Pods
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
        
        owner_kind, owner_name = None, None
        if meta.get("ownerReferences"):
            owner_kind = meta["ownerReferences"][0].get("kind")
            owner_name = meta["ownerReferences"][0].get("name")
        
        is_ds = (owner_kind == "DaemonSet")
        namespace = meta.get("namespace")
        is_system = (namespace in ["kube-system", "monitoring", "logging", "ingress-nginx"])

        tols = [{"key": t.get("key"), "operator": t.get("operator"), "value": t.get("value"), "effect": t.get("effect")} for t in spec.get("tolerations", [])]
        
        # Metrics Mapping
        usage = metrics_map.get(pod_id_str, {})
        u_cpu = CpuMillis(int(usage.get("cpu_m", 0))) if "cpu_m" in usage else None
        u_mem = Bytes(int(usage.get("mem_b", 0))) if "mem_b" in usage else None

        pods[pod_id] = Pod(
            id=pod_id, name=meta.get("name"), namespace=Namespace(namespace),
            node=NodeId(node_name) if node_name in nodes else None,
            owner_kind=owner_kind, owner_name=owner_name,
            req_cpu_m=CpuMillis(req_cpu_acc), req_mem_b=Bytes(req_mem_acc),
            is_daemonset=is_ds, is_system=is_system, is_gfw=(not is_ds and not is_system),
            tolerations=tols, node_selector=spec.get("nodeSelector") or {},
            usage_cpu_m=u_cpu, usage_mem_b=u_mem
        )

    return _create_snapshot_result(nodes, pods, nodepools)

def _create_snapshot_result(nodes, pods, nodepools) -> Snapshot:
    # (Оставь как было)
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
            return _collect_via_client(k8s_context)
    else:
        return _collect_via_client(k8s_context)