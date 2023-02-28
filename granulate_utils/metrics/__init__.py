from typing import Any, Dict


def set_individual_metric(
    collected_metrics: Dict[str, Dict[str, Any]], name: str, value: Any, labels: Dict[str, str]
) -> None:
    """
    Add a metric to collected_metrics with labels in {name, value, labels} format.
    Metric is only added if the value is not None.
    """
    if name not in collected_metrics and value is not None:
        collected_metrics[name] = {
            "name": name,
            "value": value,
            "labels": labels,
        }


def set_metrics_from_json(
    collected_metrics: Dict[str, Dict[str, Any]],
    labels: Dict[str, str],
    metrics_json: Dict[Any, Any],
    metrics: Dict[str, str],
) -> None:
    """
    Extract metrics values from JSON response and add to collected_metrics in {name, value, labels} format.
    """
    if metrics_json is None:
        return

    for field_name, metric_name in metrics.items():
        metric_value = metrics_json.get(field_name)
        set_individual_metric(collected_metrics, metric_name, metric_value, labels)
