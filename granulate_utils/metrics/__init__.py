#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
# (C) Datadog, Inc. 2018-present. All rights reserved.
# Licensed under a 3-clause BSD style license (see LICENSE.bsd3).
#
from typing import Any, Dict
from urllib.parse import urljoin

import requests


def rest_request_to_json(address: str, object_path: str, *args: Any, **kwargs: Any) -> Any:
    """
    Query the given URL and return the JSON response
    """
    return rest_request(address, object_path, *args, **kwargs).json()


def rest_request(url: str, object_path: str, *args: Any, **kwargs: Any) -> requests.Response:
    """
    Query the given URL and return the response
    """
    if object_path:
        url = join_url_dir(url, object_path)

    # Add args to the url
    if args:
        for directory in args:
            url = join_url_dir(url, directory)

    response = requests.get(url, params={k: v for k, v in kwargs.items() if v is not None}, timeout=3)
    response.raise_for_status()
    return response


def join_url_dir(url: str, *args: Any) -> str:
    """
    Join a URL with multiple directories
    """
    for path in args:
        url = url.rstrip("/") + "/"
        url = urljoin(url, path.lstrip("/"))
    return url


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
