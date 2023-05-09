#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

import json
import logging
import os
import time
from typing import Optional

import requests

from granulate_utils.exceptions import DatabricksJobNameDiscoverException
from granulate_utils.metadata import Metadata

HOST_KEY_NAME = "*.sink.ganglia.host"
DATABRICKS_METRICS_PROP_PATH = "/databricks/spark/conf/metrics.properties"
CLUSTER_TAGS_KEY = "spark.databricks.clusterUsageTags.clusterAllTags"
SPARKUI_APPS_URL = "http://{}/api/v1/applications"
REQUEST_TIMEOUT = 5
DEFAULT_WEBUI_PORT = 40001
MAX_RETRIES = 300
RETRY_INTERVAL = 1


class DatabricksClient:
    def __init__(self, logger: logging.LoggerAdapter) -> None:
        self.logger = logger
        self.logger.debug("Getting Databricks job name")
        self.job_name = self.get_job_name()
        if self.job_name is None:
            self.logger.warning(
                "Failed initializing Databricks client. Databricks job name will not be included in ephemeral clusters."
            )
        else:
            self.logger.debug(f"Got Databricks job name: {self.job_name}")

    def _request_get(self, url: str) -> requests.Response:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp

    @staticmethod
    def get_webui_address() -> str:
        with open(DATABRICKS_METRICS_PROP_PATH) as f:
            properties = f.read()
        host = dict([line.split("=", 1) for line in properties.splitlines()])[HOST_KEY_NAME]
        return f"{host}:{DEFAULT_WEBUI_PORT}"

    def get_job_name(self) -> Optional[str]:
        # Retry in case of a connection error, as the metrics server might not be up yet.
        retries = 0
        while retries < MAX_RETRIES:
            retries += 1
            try:
                if cluster_metadata := self._cluster_all_tags_metadata():
                    # Got the job name, no need to retry.
                    name = self._get_name_from_metadata(cluster_metadata)
                    if name:
                        self.logger.debug("Found name in metadata", job_name=name, cluster_metadata=cluster_metadata)
                        return name
                    else:
                        self.logger.debug("Failed to extract name from metadata", cluster_metadata=cluster_metadata)
                        return None
                else:
                    # No job name yet, retry.
                    time.sleep(RETRY_INTERVAL)
            except DatabricksJobNameDiscoverException as e:
                self.logger.exception("Failed to get Databricks job name", exception=e)
                return None
            except Exception as e:
                self.logger.exception("Generic exception was raise during spark job name discovery", exception=e)
                return None
        self.logger.info("Databricks get job name timeout, continuing...")
        return None

    @staticmethod
    def _get_name_from_metadata(metadata: Metadata) -> Optional[str]:
        if "RunName" in metadata:
            return str(metadata["RunName"]).replace(" ", "-").lower()
        return None

    def _cluster_all_tags_metadata(self) -> Optional[Metadata]:
        """
        Returns `includes spark.databricks.clusterUsageTags.clusterAllTags` tags as `Dict`.
        """
        if not os.path.isfile(DATABRICKS_METRICS_PROP_PATH):
            # We want to retry in case the cluster is still initializing, and the file is not yet deployed.
            return None
        webui = self.get_webui_address()
        # The API used: https://spark.apache.org/docs/latest/monitoring.html#rest-api
        apps_url = SPARKUI_APPS_URL.format(webui)
        self.logger.debug(f"Databricks SparkUI address: {apps_url}")
        try:
            response = self._request_get(apps_url)
        except requests.exceptions.RequestException:
            # Request might fail in cases where the cluster is still initializing, retrying.
            return None
        try:
            apps = response.json()
        except Exception as e:
            raise DatabricksJobNameDiscoverException(f"Failed to parse apps url response, query {response=}") from e
        if len(apps) == 0:
            # apps might be empty because of initialization, retrying.
            self.logger.debug("No apps yet, retrying.")
            return None

        env_url = f"{apps_url}/{apps[0].get('id')}/environment"
        try:
            response = self._request_get(env_url)
        except Exception as e:
            # No reason for any exception, `environment` uri should be accessible if we have running apps.
            raise DatabricksJobNameDiscoverException(f"Environment request failed {env_url=}") from e
        try:
            env = response.json()
        except Exception as e:
            raise DatabricksJobNameDiscoverException(f"Environment request failed {response=}") from e
        props = env.get("sparkProperties", [])
        if not props:
            raise DatabricksJobNameDiscoverException(f"sparkProperties was not found in {env=}")
        for prop in props:
            if prop[0] == CLUSTER_TAGS_KEY:
                try:
                    all_tags_value = json.loads(prop[1])
                except Exception as e:
                    raise DatabricksJobNameDiscoverException(f"Failed to parse {prop=}") from e
                return {clusterUsageTag["key"]: clusterUsageTag["value"] for clusterUsageTag in all_tags_value}
        return None
