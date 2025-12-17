from __future__ import annotations

from typing import Any, Dict, List


# ---------------------------------------------------------------------------
# Нормализация taints / tolerations
# ---------------------------------------------------------------------------


def _normalize_taints(taints_raw: Any) -> List[Dict[str, Any]]:
    """Приводим taints к списку dict-ов с ключами key/value/effect."""
    if not taints_raw:
        return []
    result: List[Dict[str, Any]] = []
    for t in taints_raw:
        if isinstance(t, dict):
            result.append(
                {
                    "key": t.get("key"),
                    "value": t.get("value"),
                    "effect": t.get("effect"),
                }
            )
        else:
            # на случай, если внутри объект с атрибутами
            result.append(
                {
                    "key": getattr(t, "key", None),
                    "value": getattr(t, "value", None),
                    "effect": getattr(t, "effect", None),
                }
            )
    return result


def _normalize_tolerations(tols_raw: Any) -> List[Dict[str, Any]]:
    """Приводим tolerations к списку dict-ов с key/operator/value/effect."""
    if not tols_raw:
        return []
    result: List[Dict[str, Any]] = []
    for tol in tols_raw:
        if isinstance(tol, dict):
            result.append(
                {
                    "key": tol.get("key"),
                    "operator": tol.get("operator"),
                    "value": tol.get("value"),
                    "effect": tol.get("effect"),
                }
            )
        else:
            result.append(
                {
                    "key": getattr(tol, "key", None),
                    "operator": getattr(tol, "operator", None),
                    "value": getattr(tol, "value", None),
                    "effect": getattr(tol, "effect", None),
                }
            )
    return result


# ---------------------------------------------------------------------------
# Проверка taints / tolerations
# ---------------------------------------------------------------------------


def _taint_tolerated(
    key: str | None, value: str | None, effect: str | None, tolerations: List[Dict[str, Any]]
) -> bool:
    """
    Грубая модель соответствия taint/toleration:
      - оператор Equal (дефолт): key совпадает, value совпадает (или taint.value пустой),
        effect совпадает или в toleration не задан;
      - оператор Exists: key совпадает, effect совпадает или не задан.
    """
    for tol in tolerations:
        t_key = tol.get("key")
        if not t_key or t_key != key:
            continue

        op = (tol.get("operator") or "Equal").capitalize()
        t_val = tol.get("value")
        t_eff = tol.get("effect")

        if t_eff and effect and t_eff != effect:
            continue

        if op == "Exists":
            return True

        # Equal / всё остальное трактуем как Equal
        if op == "Equal":
            if value is None:
                # taint без value, достаточно совпадения key
                return True
            
            # --- FIX: Robust comparison (str/bool/case-insensitive) ---
            v1 = str(t_val).lower() if t_val is not None else ""
            v2 = str(value).lower() if value is not None else ""
            
            if v1 == v2:
                return True

    return False


def _check_taints_and_tolerations(pod, node) -> List[str]:
    reasons: List[str] = []

    taints = _normalize_taints(getattr(node, "taints", None))
    tolerations = _normalize_tolerations(getattr(pod, "tolerations", None))

    if not taints:
        return reasons

    for t in taints:
        key = t.get("key")
        value = t.get("value")
        effect = t.get("effect") or "NoSchedule"

        # интересны только жесткие эффекты
        if effect not in ("NoSchedule", "NoExecute"):
            continue

        if not _taint_tolerated(key, value, effect, tolerations):
            reasons.append(
                f"taint '{key}={value}' with effect '{effect}' is not tolerated by pod"
            )

    return reasons


# ---------------------------------------------------------------------------
# nodeSelector
# ---------------------------------------------------------------------------


def _check_node_selector(pod, node) -> List[str]:
    reasons: List[str] = []

    selector = getattr(pod, "node_selector", None) or {}
    labels = getattr(node, "labels", None) or {}

    for key, expected in selector.items():
        actual = labels.get(key)
        if actual is None:
            reasons.append(f"nodeSelector: missing label '{key}={expected}' on node")
        elif str(actual) != str(expected):
            reasons.append(
                f"nodeSelector: node label '{key}={actual}' != expected '{expected}'"
            )

    return reasons


# ---------------------------------------------------------------------------
# nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution
# ---------------------------------------------------------------------------


def _match_node_selector_expression(expr: Dict[str, Any], labels: Dict[str, str]) -> bool:
    key = expr.get("key")
    op = expr.get("operator") or "In"
    values = expr.get("values") or []

    val = labels.get(key)

    if op == "In":
        return val in values
    if op == "NotIn":
        return val is not None and val not in values
    if op == "Exists":
        return key in labels
    if op == "DoesNotExist":
        return key not in labels
    if op in ("Gt", "Lt"):
        # очень грубо: сравниваем как int, если не получилось — считаем несовпадением
        try:
            v_int = int(val) if val is not None else None
            cmp = int(values[0]) if values else None
        except Exception:
            return False
        if v_int is None or cmp is None:
            return False
        if op == "Gt":
            return v_int > cmp
        else:
            return v_int < cmp

    # неизвестный оператор — считаем, что не удовлетворяет
    return False


def _match_node_selector_term(term: Dict[str, Any], labels: Dict[str, str]) -> bool:
    """
    NodeSelectorTerm: набор выражений AND в рамках терма, термы объединяются по OR.
    """
    exprs = term.get("matchExpressions") or []
    # matchFields игнорируем, чтобы не усложнять — редко используется для наших кейсов
    for expr in exprs:
        if not _match_node_selector_expression(expr, labels):
            return False
    return True


def _check_node_affinity(pod, node) -> List[str]:
    reasons: List[str] = []

    affinity = getattr(pod, "affinity", None) or {}
    node_aff = affinity.get("nodeAffinity") or {}
    required = node_aff.get("requiredDuringSchedulingIgnoredDuringExecution") or {}

    terms = required.get("nodeSelectorTerms") or []
    if not terms:
        return reasons

    labels = getattr(node, "labels", None) or {}

    matches = any(_match_node_selector_term(term, labels) for term in terms)
    if not matches:
        reasons.append("nodeAffinity.requiredDuringScheduling is not satisfied by node")

    return reasons


# ---------------------------------------------------------------------------
# Основная проверка pod → node
# ---------------------------------------------------------------------------


def check_pod_on_node(pod, node) -> List[str]:
    """
    Проверка соблюдения основных правил назначения пода на ноду:
      - nodeSelector
      - taints / tolerations
      - nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution

    podAffinity / podAntiAffinity здесь сознательно не проверяем, т.к. нужен
    полноценный контекст по другим подам и селекторам; можно будет добавить позже.
    """
    reasons: List[str] = []

    reasons.extend(_check_node_selector(pod, node))
    reasons.extend(_check_taints_and_tolerations(pod, node))
    reasons.extend(_check_node_affinity(pod, node))

    return reasons


# ---------------------------------------------------------------------------
# Расчёт нарушений по всему снапшоту
# ---------------------------------------------------------------------------


def compute_violations(snapshot) -> Dict[str, List[Dict[str, Any]]]:
    """
    Строит карту нарушений:
      {
        "<node-name>": [
          {
            "pod_id": "<ns/name>",
            "reasons": ["...", "..."]
          },
          ...
        ],
        ...
      }

    Используется на уровне API для поля `violations` в /simulate.
    """
    nodes = getattr(snapshot, "nodes", None) or {}
    pods = getattr(snapshot, "pods", None) or {}

    result: Dict[str, List[Dict[str, Any]]] = {}

    for node_name, node in nodes.items():
        node_violations: List[Dict[str, Any]] = []

        for pod_id, pod in pods.items():
            if getattr(pod, "node", None) != node_name:
                continue

            reasons = check_pod_on_node(pod, node)
            if reasons:
                node_violations.append(
                    {
                        "pod_id": pod_id,
                        "reasons": reasons,
                    }
                )

        if node_violations:
            result[node_name] = node_violations

    return result


# ---------------------------------------------------------------------------
# Обратная совместимость
# ---------------------------------------------------------------------------


def check_all_placements(snapshot) -> Dict[str, List[Dict[str, Any]]]:
    """
    Обратная совместимость для старого API constraints.check_all_placements().

    Возвращает тот же формат, что и compute_violations(snapshot):
      dict[node_name] = [{pod_id, reasons}, ...]
    """
    return compute_violations(snapshot)