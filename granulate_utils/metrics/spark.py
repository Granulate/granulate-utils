#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
# (C) Datadog, Inc. 2018-present. All rights reserved.
# Licensed under a 3-clause BSD style license (see LICENSE.bsd3).
#
import logging
from typing import Any, Dict, Iterable, Tuple

from bs4 import BeautifulSoup
from requests import HTTPError

from granulate_utils.metrics import (
    Collector,
    Sample,
    get_request_url,
    rest_request_raw,
    rest_request_to_json,
    samples_from_json,
)
from granulate_utils.metrics.metrics import (
    SPARK_AGGREGATED_STAGE_METRICS,
    SPARK_APPLICATION_DIFF_METRICS,
    SPARK_APPLICATION_GAUGE_METRICS,
    SPARK_EXECUTORS_METRICS,
    SPARK_RUNNING_APPS_COUNT_METRIC,
)
from granulate_utils.metrics.mode import SPARK_MESOS_MODE, SPARK_STANDALONE_MODE, SPARK_YARN_MODE
from granulate_utils.metrics.yarn import YARN_RUNNING_APPLICATION_SPECIFIER, YARN_SPARK_APPLICATION_SPECIFIER

SPARK_APPS_PATH = "api/v1/applications"
MESOS_MASTER_APP_PATH = "/frameworks"
YARN_APPS_PATH = "ws/v1/cluster/apps"
YARN_CLUSTER_PATH = "ws/v1/cluster/metrics"
YARN_NODES_PATH = "ws/v1/cluster/nodes"

SPARK_MASTER_STATE_PATH = "/json"
SPARK_MASTER_APP_PATH = "/app/"


class SparkRunningApps:
    def __init__(self, cluster_mode: str, master_address: str, logger: logging.LoggerAdapter) -> None:
        self._master_address = master_address
        self._cluster_mode = cluster_mode
        self._logger = logger

    def get_running_apps(self) -> Dict[str, Tuple[str, str]]:
        """
        Determine what mode was specified
        """
        if self._cluster_mode == SPARK_YARN_MODE:
            running_apps = self._yarn_init()
            return self._get_spark_app_ids(running_apps)
        elif self._cluster_mode == SPARK_STANDALONE_MODE:
            return self._standalone_init()
        elif self._cluster_mode == SPARK_MESOS_MODE:
            return self._mesos_init()
        else:
            raise ValueError(f"Invalid cluster mode {self._cluster_mode!r}")

    def _get_spark_app_ids(self, running_apps: Dict[str, Tuple[str, str]]) -> Dict[str, Tuple[str, str]]:
        """
        Traverses the Spark application master in YARN to get a Spark application ID.
        Return a dictionary of {app_id: (app_name, tracking_url)} for Spark applications
        """
        spark_apps = {}
        for app_id, (app_name, tracking_url) in running_apps.items():
            try:
                response = rest_request_to_json(tracking_url, SPARK_APPS_PATH)

                for app in response:
                    app_id = app.get("id")
                    app_name = app.get("name")

                    if app_id and app_name:
                        spark_apps[app_id] = (app_name, tracking_url)
            except Exception:
                self._logger.exception("Could not fetch data from url", url=tracking_url)

        return spark_apps

    def _yarn_init(self) -> Dict[str, Tuple[str, str]]:
        """
        Return a dictionary of {app_id: (app_name, tracking_url)} for running Spark applications.
        """
        return self._yarn_get_spark_apps(
            states=YARN_RUNNING_APPLICATION_SPECIFIER, applicationTypes=YARN_SPARK_APPLICATION_SPECIFIER
        )

    def _yarn_get_spark_apps(self, *args: Any, **kwargs: Any) -> Dict[str, Tuple[str, str]]:
        metrics_json = rest_request_to_json(self._master_address, YARN_APPS_PATH, *args, **kwargs)

        running_apps = {}

        if metrics_json.get("apps"):
            if metrics_json["apps"].get("app") is not None:
                for app_json in metrics_json["apps"]["app"]:
                    app_id = app_json.get("id")
                    tracking_url = app_json.get("trackingUrl")
                    app_name = app_json.get("name")

                    if app_id and tracking_url and app_name:
                        running_apps[app_id] = (app_name, tracking_url)

        return running_apps

    def _mesos_init(self) -> Dict[str, Tuple[str, str]]:
        running_apps = {}
        metrics_json = rest_request_to_json(self._master_address, MESOS_MASTER_APP_PATH)
        for app_json in metrics_json.get("frameworks", []):
            app_id = app_json.get("id")
            tracking_url = app_json.get("webui_url")
            app_name = app_json.get("name")
            if app_id and tracking_url and app_name:
                running_apps[app_id] = (app_name, tracking_url)
        return running_apps

    def _standalone_init(self) -> Dict[str, Tuple[str, str]]:
        """
        Return a dictionary of {app_id: (app_name, tracking_url)} for the running Spark applications
        """
        # Parsing the master address json object:
        # https://github.com/apache/spark/blob/67a254c7ed8c5c3321e8bed06294bc2c9a2603de/core/src/main/scala/org/apache/spark/deploy/JsonProtocol.scala#L202
        metrics_json = rest_request_to_json(self._master_address, SPARK_MASTER_STATE_PATH)
        running_apps = {}

        for app in metrics_json.get("activeapps", []):
            try:
                app_id = app["id"]
                app_name = app["name"]

                # Parse through the HTML to grab the application driver's link
                app_url = self._get_standalone_app_url(app_id)
                self._logger.debug("Retrieved standalone app URL", app_url=app_url)

                if app_id and app_name and app_url:
                    running_apps[app_id] = (app_name, app_url)
                    self._logger.debug("Added app to running apps", app_id=app_id, app_name=app_name, app_url=app_url)
            except HTTPError as e:
                if e.response.status_code == 404:
                    # it's possible for the requests to fail if the job
                    # completed since we got the list of apps.  Just continue
                    pass
                else:
                    self._logger.exception("HTTP error was found while iterating applications.")
            except Exception:
                self._logger.exception("Error was found while iterating applications.")

        return running_apps

    def _get_standalone_app_url(self, app_id: str) -> Any:
        """
        Return the application URL from the app info page on the Spark master.
        Due to a bug, we need to parse the HTML manually because we cannot
        fetch JSON data from HTTP interface.
        Hence, we decided to carry logic from Datadog's Spark integration.
        """
        app_page = rest_request_raw(self._master_address, SPARK_MASTER_APP_PATH, appId=app_id)

        dom = BeautifulSoup(app_page.text, "html.parser")

        app_detail_ui_links = dom.find_all("a", string="Application Detail UI")

        if app_detail_ui_links and len(app_detail_ui_links) == 1:
            return app_detail_ui_links[0].attrs["href"]


class SparkApplicationMetricsCollector(Collector):
    def __init__(self, cluster_mode: str, master_address: str, logger: logging.LoggerAdapter) -> None:
        self.master_address = master_address
        self._cluster_mode = cluster_mode
        self.logger = logger
        self.running_apps_helper = SparkRunningApps(cluster_mode, master_address, logger)
        self._last_iteration_app_job_metrics: Dict[str, Dict[str, Any]] = {}

    def collect(self) -> Iterable[Sample]:
        running_apps = self.running_apps_helper.get_running_apps()
        try:
            yield from self._spark_application_metrics(running_apps)
            yield from self._spark_stage_metrics(running_apps)
            yield from self._spark_executor_metrics(running_apps)
            yield from self._running_applications_count_metric(running_apps)
        except Exception as e:
            self.logger.exception("Failed to collect Spark metrics", exception=e)

    def _spark_application_metrics(self, running_apps: Dict[str, Tuple[str, str]]) -> Iterable[Sample]:
        """
        Get metrics for each Spark job.
        """
        iteration_metrics: Dict[str, Dict[str, Any]] = {}
        for app_id, (app_name, tracking_url) in running_apps.items():
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

    def _spark_stage_metrics(self, running_apps: Dict[str, Tuple[str, str]]) -> Iterable[Sample]:
        """
        Get metrics for each Spark stage.
        """
        for app_id, (app_name, tracking_url) in running_apps.items():
            labels = {"app_name": str(app_name), "app_id": str(app_id)}
            self.logger.debug("Gathering stage metrics for app", app_id=app_id)
            try:
                base_url = get_request_url(self.master_address, tracking_url)
                response = rest_request_to_json(base_url, SPARK_APPS_PATH, app_id, "stages")
                self.logger.debug("Got response for stage metrics for app %s", app_id)
            except Exception as e:
                self.logger.exception(
                    "Exception occurred while trying to retrieve stage metrics", extra={"exception": e}
                )
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

    def _spark_executor_metrics(self, running_apps: Dict[str, Tuple[str, str]]) -> Iterable[Sample]:
        """
        Get metrics for each Spark executor.
        """
        for app_id, (app_name, tracking_url) in running_apps.items():
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

    def _running_applications_count_metric(self, running_apps: Dict[str, Tuple[str, str]]) -> Iterable[Sample]:
        yield Sample(name=SPARK_RUNNING_APPS_COUNT_METRIC, value=len(running_apps), labels={})
