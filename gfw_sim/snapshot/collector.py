# gfw_sim/snapshot/collector.py
from __future__ import annotations

import logging
import re
import json
import subprocess
from typing import Dict, List, Any

from kubernetes import client, config

from ..model.entities import Snapshot, Node, Pod, NodePool, InstancePrice, Schedule
from ..types import (
    NodeId, PodId, NodePoolName, InstanceType, Namespace, CpuMillis, Bytes, UsdPerHour
)

log = logging.getLogger(__name__)

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
    """Запускает kubectl и возвращает распаршенный JSON."""
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

def _collect_via_client(k8s_context: str | None) -> Snapshot:
    """Сбор данных через python-kubernetes client (медленный способ)."""
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
            log.info(f"Trying to fetch NodePools via karpenter.sh/{version}...")
            resp = cust.list_cluster_custom_object(
                group="karpenter.sh", 
                version=version, 
                plural="nodepools"
            )
            kp_pools_items = resp.get("items", [])
            if kp_pools_items:
                log.info(f"Found {len(kp_pools_items)} nodepools in {version}")
                break
        except client.exceptions.ApiException as e:
            if e.status == 404:
                continue
            else:
                log.warning(f"Error fetching NodePools ({version}): {e}")

    for item in kp_pools_items:
        metadata = item.get("metadata", {})
        spec = item.get("spec", {})
        template = spec.get("template", {})
        spec_meta = template.get("metadata", {})
        
        name_str = metadata.get("name")
        if not name_str: 
            continue
        
        name = NodePoolName(name_str)
        labels = spec_meta.get("labels", {})
        taints_raw = spec.get("template", {}).get("spec", {}).get("taints", [])
        
        taints = []
        for t in taints_raw:
            taints.append({
                "key": t.get("key"), 
                "value": t.get("value"), 
                "effect": t.get("effect")
            })

        is_keda = "keda" in name_str.lower() or "nightly" in name_str.lower()
        
        if is_keda:
            has_nightly_taint = any(t["key"] == "keda_nightly" for t in taints)
            if not has_nightly_taint:
                taints.append({
                    "key": "keda_nightly",
                    "value": "true",
                    "effect": "NoSchedule"
                })

        nodepools[name] = NodePool(
            name=name,
            labels=labels,
            taints=taints,
            is_keda=is_keda,
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
        capacity_type = labels.get("karpenter.sh/capacity-type") or "on_demand"
        
        alloc = status.allocatable or {}
        cpu_m = parse_cpu(alloc.get("cpu"))
        mem_b = parse_memory(alloc.get("memory"))

        node_taints = []
        if spec.taints:
            for t in spec.taints:
                node_taints.append({
                    "key": t.key,
                    "value": t.value,
                    "effect": t.effect
                })

        if pool_name not in nodepools:
            is_keda = "keda" in pool_name_str.lower() or "nightly" in pool_name_str.lower()
            inferred_taints = list(node_taints)
            if is_keda:
                has_nightly_taint = any(t["key"] == "keda_nightly" for t in inferred_taints)
                if not has_nightly_taint:
                     inferred_taints.append({
                        "key": "keda_nightly",
                        "value": "true",
                        "effect": "NoSchedule"
                    })
            nodepools[pool_name] = NodePool(
                name=pool_name,
                labels=labels,
                taints=inferred_taints,
                is_keda=is_keda,
                schedule_name="keda-weekdays-12h" if is_keda else "default"
            )

        nodes[NodeId(name)] = Node(
            id=NodeId(name),
            name=name,
            nodepool=pool_name,
            instance_type=InstanceType(instance_type_str),
            alloc_cpu_m=cpu_m,
            alloc_mem_b=mem_b,
            capacity_type=capacity_type,
            labels=labels,
            taints=node_taints,
            is_virtual=False
        )

    # --- 3. Pods ---
    pods: Dict[PodId, Pod] = {}
    for kp in k8s_pods:
        meta = kp.metadata
        spec = kp.spec
        
        if not spec.node_name:
            continue
            
        pod_id = PodId(f"{meta.namespace}/{meta.name}")
        
        req_cpu_acc = 0
        req_mem_acc = 0
        all_containers = (spec.containers or []) + (spec.init_containers or [])
        for c in all_containers:
            res = c.resources
            if res and res.requests:
                req_cpu_acc += int(parse_cpu(res.requests.get("cpu")))
                req_mem_acc += int(parse_memory(res.requests.get("memory")))
        
        owner_kind = None
        owner_name = None
        if meta.owner_references:
            ref = meta.owner_references[0]
            owner_kind = ref.kind
            owner_name = ref.name
            
        is_ds = (owner_kind == "DaemonSet")
        is_system = (meta.namespace in ["kube-system", "monitoring", "logging", "ingress-nginx"])
        is_gfw = (not is_ds) and (not is_system)

        pod_tols = []
        if spec.tolerations:
            for t in spec.tolerations:
                pod_tols.append({
                    "key": t.key,
                    "operator": t.operator,
                    "value": t.value,
                    "effect": t.effect
                })
        
        affinity_dict = {}
        if spec.affinity:
            try:
                affinity_dict = spec.affinity.to_dict()
            except AttributeError:
                pass

        pods[pod_id] = Pod(
            id=pod_id,
            name=meta.name,
            namespace=Namespace(meta.namespace),
            node=NodeId(spec.node_name) if spec.node_name in nodes else None,
            owner_kind=owner_kind,
            owner_name=owner_name,
            req_cpu_m=CpuMillis(req_cpu_acc),
            req_mem_b=Bytes(req_mem_acc),
            is_daemonset=is_ds,
            is_system=is_system,
            is_gfw=is_gfw,
            tolerations=pod_tols,
            node_selector=spec.node_selector or {},
            affinity=affinity_dict
        )

    return _create_snapshot_result(nodes, pods, nodepools)

def _collect_via_kubectl(context: str | None) -> Snapshot:
    """Сбор данных через вызов kubectl (быстрый способ)."""
    log.info("Fetching Nodes via kubectl...")
    nodes_data = _run_kubectl(["get", "nodes"], context).get("items", [])
    
    log.info("Fetching Pods via kubectl...")
    pods_data = _run_kubectl(["get", "pods", "--all-namespaces", "--field-selector=status.phase=Running"], context).get("items", [])
    
    # --- 1. NodePools ---
    nodepools: Dict[NodePoolName, NodePool] = {}
    kp_pools_items = []
    
    versions_to_try = ["v1", "v1beta1"]
    for version in versions_to_try:
        try:
            log.info(f"Trying to fetch NodePools via karpenter.sh/{version}...")
            # kubectl get nodepools.karpenter.sh
            # Мы используем generic имя CRD, kubectl сам разберется с версией, если она одна,
            # но если их несколько, лучше указывать.
            # Попробуем просто "nodepools.karpenter.sh"
            data = _run_kubectl(["get", "nodepools.karpenter.sh"], context)
            kp_pools_items = data.get("items", [])
            if kp_pools_items:
                log.info(f"Found {len(kp_pools_items)} nodepools")
                break
        except Exception:
            # Игнорируем ошибки (например CRD не найден)
            pass

    for item in kp_pools_items:
        metadata = item.get("metadata", {})
        spec = item.get("spec", {})
        template = spec.get("template", {})
        spec_meta = template.get("metadata", {})
        
        name_str = metadata.get("name")
        if not name_str: 
            continue
        
        name = NodePoolName(name_str)
        labels = spec_meta.get("labels", {})
        # В kubectl JSON структура такая же (dict)
        taints_raw = spec.get("template", {}).get("spec", {}).get("taints", [])
        
        taints = []
        for t in taints_raw:
            taints.append({
                "key": t.get("key"), 
                "value": t.get("value"), 
                "effect": t.get("effect")
            })

        is_keda = "keda" in name_str.lower() or "nightly" in name_str.lower()
        if is_keda:
            has_nightly_taint = any(t["key"] == "keda_nightly" for t in taints)
            if not has_nightly_taint:
                taints.append({
                    "key": "keda_nightly",
                    "value": "true",
                    "effect": "NoSchedule"
                })

        nodepools[name] = NodePool(
            name=name,
            labels=labels,
            taints=taints,
            is_keda=is_keda,
            schedule_name="keda-weekdays-12h" if is_keda else "default"
        )

    # --- 2. Nodes ---
    nodes: Dict[NodeId, Node] = {}
    for kn in nodes_data:
        meta = kn.get("metadata", {})
        spec = kn.get("spec", {})
        status = kn.get("status", {})
        labels = meta.get("labels", {})
        name = meta.get("name")
        
        pool_name_str = labels.get("karpenter.sh/nodepool") or labels.get("node.kubernetes.io/instance-group") or "default"
        pool_name = NodePoolName(pool_name_str)
        instance_type_str = labels.get("node.kubernetes.io/instance-type") or "unknown"
        capacity_type = labels.get("karpenter.sh/capacity-type") or "on_demand"
        
        alloc = status.get("allocatable", {})
        cpu_m = parse_cpu(alloc.get("cpu"))
        mem_b = parse_memory(alloc.get("memory"))

        node_taints = []
        raw_taints = spec.get("taints", [])
        for t in raw_taints:
            node_taints.append({
                "key": t.get("key"),
                "value": t.get("value"),
                "effect": t.get("effect")
            })

        if pool_name not in nodepools:
            is_keda = "keda" in pool_name_str.lower() or "nightly" in pool_name_str.lower()
            inferred_taints = list(node_taints)
            if is_keda:
                has_nightly_taint = any(t["key"] == "keda_nightly" for t in inferred_taints)
                if not has_nightly_taint:
                     inferred_taints.append({
                        "key": "keda_nightly",
                        "value": "true",
                        "effect": "NoSchedule"
                    })
            nodepools[pool_name] = NodePool(
                name=pool_name,
                labels=labels,
                taints=inferred_taints,
                is_keda=is_keda,
                schedule_name="keda-weekdays-12h" if is_keda else "default"
            )

        nodes[NodeId(name)] = Node(
            id=NodeId(name),
            name=name,
            nodepool=pool_name,
            instance_type=InstanceType(instance_type_str),
            alloc_cpu_m=cpu_m,
            alloc_mem_b=mem_b,
            capacity_type=capacity_type,
            labels=labels,
            taints=node_taints,
            is_virtual=False
        )

    # --- 3. Pods ---
    pods: Dict[PodId, Pod] = {}
    for kp in pods_data:
        meta = kp.get("metadata", {})
        spec = kp.get("spec", {})
        
        node_name = spec.get("nodeName")
        if not node_name:
            continue
            
        pod_id = PodId(f"{meta.get('namespace')}/{meta.get('name')}")
        
        req_cpu_acc = 0
        req_mem_acc = 0
        all_containers = (spec.get("containers") or []) + (spec.get("initContainers") or [])
        for c in all_containers:
            res = c.get("resources", {})
            reqs = res.get("requests", {})
            if reqs:
                req_cpu_acc += int(parse_cpu(reqs.get("cpu")))
                req_mem_acc += int(parse_memory(reqs.get("memory")))
        
        owner_kind = None
        owner_name = None
        owner_refs = meta.get("ownerReferences", [])
        if owner_refs:
            ref = owner_refs[0]
            owner_kind = ref.get("kind")
            owner_name = ref.get("name")
            
        is_ds = (owner_kind == "DaemonSet")
        namespace = meta.get("namespace")
        is_system = (namespace in ["kube-system", "monitoring", "logging", "ingress-nginx"])
        is_gfw = (not is_ds) and (not is_system)

        pod_tols = []
        raw_tols = spec.get("tolerations", [])
        for t in raw_tols:
            pod_tols.append({
                "key": t.get("key"),
                "operator": t.get("operator"),
                "value": t.get("value"),
                "effect": t.get("effect")
            })
            
        affinity_dict = spec.get("affinity") or {}

        pods[pod_id] = Pod(
            id=pod_id,
            name=meta.get("name"),
            namespace=Namespace(namespace),
            node=NodeId(node_name) if node_name in nodes else None,
            owner_kind=owner_kind,
            owner_name=owner_name,
            req_cpu_m=CpuMillis(req_cpu_acc),
            req_mem_b=Bytes(req_mem_acc),
            is_daemonset=is_ds,
            is_system=is_system,
            is_gfw=is_gfw,
            tolerations=pod_tols,
            node_selector=spec.get("nodeSelector") or {},
            affinity=affinity_dict
        )

    return _create_snapshot_result(nodes, pods, nodepools)

def _create_snapshot_result(nodes, pods, nodepools) -> Snapshot:
    """Общий метод сборки финального объекта Snapshot."""
    schedules = {
        "default": Schedule(name="default", hours_per_day=24.0, days_per_week=7.0),
        "keda-weekdays-12h": Schedule(name="keda-weekdays-12h", hours_per_day=12.0, days_per_week=5.0)
    }
    prices = {}
    return Snapshot(
        nodes=nodes,
        pods=pods,
        nodepools=nodepools,
        prices=prices,
        schedules=schedules,
        keda_pool_name=NodePoolName("keda-nightly-al2023-private-c")
    )

def collect_k8s_snapshot(k8s_context: str | None = None, method: str = "kubectl") -> Snapshot:
    """
    Собирает текущее состояние кластера в Snapshot.
    
    Args:
        k8s_context: Имя контекста Kubernetes.
        method: Метод сбора данных ('kubectl' или 'client'). 
                'kubectl' - использует вызов бинарника kubectl (быстрее).
                'client' - использует python-kubernetes (медленнее, но не требует бинарника).
    """
    if method == "kubectl":
        try:
            return _collect_via_kubectl(k8s_context)
        except FileNotFoundError:
            log.error("kubectl binary not found. Falling back to client method.")
            return _collect_via_client(k8s_context)
        except Exception as e:
            log.error(f"kubectl method failed: {e}. Falling back to client method.")
            return _collect_via_client(k8s_context)
    else:
        return _collect_via_client(k8s_context)