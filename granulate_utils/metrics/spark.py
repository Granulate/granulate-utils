#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
# (C) Datadog, Inc. 2018-present. All rights reserved.
# Licensed under a 3-clause BSD style license (see LICENSE.bsd3).
#

from typing import Any, Dict, Iterable, List, Tuple, Union
from bs4 import BeautifulSoup

from granulate_utils.metrics import rest_request_to_json, get_request_url, samples_from_json, Sample
from granulate_utils.metrics.metrics import SPARK_APPLICATION_GAUGE_METRICS, SPARK_APPLICATION_DIFF_METRICS, \
    SPARK_AGGREGATED_STAGE_METRICS, SPARK_EXECUTORS_METRICS, SPARK_RUNNING_APPS_COUNT_METRIC

SPARK_APPS_PATH = "api/v1/applications"
MESOS_MASTER_APP_PATH = "/frameworks"
YARN_APPS_PATH = "ws/v1/cluster/apps"
YARN_CLUSTER_PATH = "ws/v1/cluster/metrics"
YARN_NODES_PATH = "ws/v1/cluster/nodes"

SPARK_MASTER_STATE_PATH = "/json"
SPARK_MASTER_APP_PATH = "/app/"


class SparkApplicationMetricsDiscover:
    """
    This class sets the relevant data for the spark application metrics collection.
    """

    def __init__(self):
        self.spark_cluster = None


class SparkApplicationMetricsCollector:
    def __init__(self, master_address: str, running_apps: Dict[str, Tuple[str, str]], logger: Any) -> None:
        self.master_address = master_address
        self.running_apps = running_apps
        self.logger = logger
        self._last_iteration_app_job_metrics: Dict[str, Dict[str, Any]] = {}

    def collect(self) -> Iterable[Dict[str, Union[str, int, float]]]:
        try:
            yield from self._spark_application_metrics()
            yield from self._spark_stage_metrics()
            yield from self._spark_executor_metrics()
            yield from self._running_applications_count_metric()
        except Exception as e:
            self.logger.exception("Failed to collect Spark metrics", exception=e)

    def _spark_application_metrics(self) -> Iterable[Sample]:
        """
        Get metrics for each Spark job.
        """
        iteration_metrics: Dict[str, Dict[str, Any]] = {}
        for app_id, (app_name, tracking_url) in self.running_apps.items():
            try:
                base_url = get_request_url(self.master_address, tracking_url)
                response = rest_request_to_json(base_url, SPARK_APPS_PATH, app_id, "jobs")
                application_diff_aggregated_metrics = dict.fromkeys(SPARK_APPLICATION_DIFF_METRICS.keys(), 0)
                application_gauge_aggregated_metrics = dict.fromkeys(SPARK_APPLICATION_GAUGE_METRICS.keys(), 0)
                iteration_metrics[app_id] = {}
                for job in response:
                    iteration_metrics[app_id][job["jobId"]] = job
                    first_time_seen_job = job["jobId"] not in self._last_iteration_app_job_metrics.get(app_id, {})
                    # In order to keep track of an application's metrics, we want to accumulate the values across all
                    # jobs. If the values are numActiveTasks or numActiveStages - there's no problem as they're
                    # always up-to-date and can just be summed. In case of completed jobs, only the last 1000
                    # (configurable - spark.ui.retainedJobs) jobs will be saved and only their metrics will be sent to
                    # us. Older jobs will be deleted , hence we can get into a situation when an old job is deleted,
                    # and then the accumulated metric will get lower, and the diff will be negative. In order to solve
                    # that, we only accumulate the value of newly seen jobs or the diff from the last time the value
                    # was seen.
                    for metric in SPARK_APPLICATION_DIFF_METRICS.keys():
                        if first_time_seen_job:
                            application_diff_aggregated_metrics[metric] += int(job[metric])
                        else:
                            application_diff_aggregated_metrics[metric] += int(job[metric]) - int(
                                self._last_iteration_app_job_metrics[app_id][job["jobId"]][metric]
                            )

                    for metric in SPARK_APPLICATION_GAUGE_METRICS.keys():
                        application_gauge_aggregated_metrics[metric] += int(job[metric])

                labels = {"app_name": app_name, "app_id": app_id}
                yield from samples_from_json(
                    labels, application_diff_aggregated_metrics, SPARK_APPLICATION_DIFF_METRICS
                )
                yield from samples_from_json(
                    labels, application_gauge_aggregated_metrics, SPARK_APPLICATION_GAUGE_METRICS
                )

            except Exception:
                self.logger.exception("Could not gather spark jobs metrics")
        self._last_iteration_app_job_metrics = iteration_metrics

    def _spark_stage_metrics(self) -> Iterable[Sample]:
        """
        Get metrics for each Spark stage.
        """
        for app_id, (app_name, tracking_url) in self.running_apps.items():
            labels = {"app_name": str(app_name), "app_id": str(app_id)}
            self.logger.debug("Gathering stage metrics for app", app_id=app_id)
            try:
                base_url = get_request_url(self.master_address, tracking_url)
                response = rest_request_to_json(base_url, SPARK_APPS_PATH, app_id, "stages")
                self.logger.debug("Got response for stage metrics for app %s", app_id)
            except Exception as e:
                self.logger.exception("Exception occurred while trying to retrieve stage metrics", extra={"exception": e})
                return

            aggregated_metrics = dict.fromkeys(SPARK_AGGREGATED_STAGE_METRICS.keys(), 0)
            for stage in response:
                curr_stage_status = stage["status"]
                aggregated_metrics["failed_tasks"] += stage["numFailedTasks"]
                if curr_stage_status == "PENDING":
                    aggregated_metrics["pending_stages"] += 1
                elif curr_stage_status == "ACTIVE":
                    aggregated_metrics["active_tasks"] += stage["numActiveTasks"]
                    aggregated_metrics["active_stages"] += 1
                elif curr_stage_status == "FAILED":
                    aggregated_metrics["failed_stages"] += 1
            yield from samples_from_json(labels, aggregated_metrics, SPARK_AGGREGATED_STAGE_METRICS)

    def _spark_executor_metrics(self) -> Iterable[Sample]:
        """
        Get metrics for each Spark executor.
        """
        for app_id, (app_name, tracking_url) in self.running_apps.items():
            try:
                base_url = get_request_url(self.master_address, tracking_url)
                executors = rest_request_to_json(base_url, SPARK_APPS_PATH, app_id, "executors")
                labels = {"app_name": app_name, "app_id": app_id}
                yield from samples_from_json(
                    labels,
                    {
                        "count": len(executors) - 1,  # Spark reports the driver as an executor, we discount it.
                        "activeCount": len([executor for executor in executors if executor["activeTasks"] > 0]),
                    },
                    SPARK_EXECUTORS_METRICS,
                )
            except Exception:
                self.logger.exception("Could not gather spark executors metrics")

    def _running_applications_count_metric(self) -> Iterable[Sample]:
        yield Sample(name=SPARK_RUNNING_APPS_COUNT_METRIC, value=len(self.running_apps), labels={})

