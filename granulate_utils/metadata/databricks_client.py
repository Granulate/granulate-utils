#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

import json
import logging
import os
import re
import time
from typing import Any, Dict, Optional

import requests

from granulate_utils.exceptions import (
    DatabricksJobNameDiscoverException,
    DatabricksMetadataFetchException,
    DatabricksTagsExtractionException,
    SparkAPIException,
    SparkAppsURLDiscoveryException,
    SparkNotReadyException,
)

HOST_KEY_NAME = "*.sink.ganglia.host"
DATABRICKS_METRICS_PROP_PATH = "/databricks/spark/conf/metrics.properties"
CLUSTER_USAGE_ALL_TAGS_PROP = "spark.databricks.clusterUsageTags.clusterAllTags"
CLUSTER_USAGE_CLUSTER_NAME_PROP = "spark.databricks.clusterUsageTags.clusterName"
CLUSTER_USAGE_RELEVANT_TAGS_PROPS = [
    "spark.databricks.clusterUsageTags.cloudProvider",
    "spark.databricks.clusterUsageTags.clusterAvailability",
    "spark.databricks.clusterUsageTags.clusterCreator",
    "spark.databricks.clusterUsageTags.clusterFirstOnDemand",
    "spark.databricks.clusterUsageTags.clusterMaxWorkers",
    "spark.databricks.clusterUsageTags.clusterMinWorkers",
    "spark.databricks.clusterUsageTags.clusterNodeType",
    "spark.databricks.clusterUsageTags.clusterScalingType",
    "spark.databricks.clusterUsageTags.clusterSizeType",
    "spark.databricks.clusterUsageTags.clusterSku",
    "spark.databricks.clusterUsageTags.clusterSpotBidMaxPrice",
    "spark.databricks.clusterUsageTags.clusterTargetWorkers",
    "spark.databricks.clusterUsageTags.clusterWorkers",
    "spark.databricks.clusterUsageTags.driverNodeType",
]
DATABRICKS_REDACTED_STR = "redacted"
SPARKUI_APPS_URL_FORMAT = "http://{}/api/v1/applications"
DEFAULT_REQUEST_TIMEOUT = 15
DEFAULT_REQUEST_VERIFY_SSL = False

JOB_NAME_KEY = "RunName"
CLUSTER_NAME_KEY = "ClusterName"
DEFAULT_WEBUI_PORT = 40001
DATABRICKS_JOBNAME_TIMEOUT_S = 2 * 60
RETRY_INTERVAL_S = 1
RUN_ID_REGEX = "-run-\\d+"


class DBXWebUIEnvWrapper:
    def __init__(
        self,
        logger: logging.LoggerAdapter,
        enable_retries: bool = True,
        http_request_timeout: int = DEFAULT_REQUEST_TIMEOUT,
        raise_on_failure: bool = False,
        verify_request_ssl: bool = DEFAULT_REQUEST_VERIFY_SSL,
    ) -> None:
        if raise_on_failure:
            assert not enable_retries, "enable_retries and raise_on_failure can't be both True"

        self.logger = logger
        self.enable_retries = enable_retries
        self.http_request_timeout = http_request_timeout
        self.raise_on_failure = raise_on_failure
        self.verify_request_ssl = verify_request_ssl
        self._apps_url: Optional[str] = None
        self.logger.debug("Getting DBX environment properties")
        self.all_props_dict: Optional[Dict[str, str]] = self.extract_relevant_metadata()
        if self.all_props_dict is None:
            self.logger.warning(
                "DBXWebUIEnvWrapper failed to get relevant metadata, service name will not include metadata from DBX"
            )

    def _request_get(self, url: str) -> requests.Response:
        try:
            resp = requests.get(
                url, timeout=self.http_request_timeout, verify=self.verify_request_ssl, allow_redirects=True
            )
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            if e.response is not None:
                raise SparkAPIException(
                    f"Failed to perform REST HTTP GET on {url=} response_text={e.response.text},"
                    f"code={e.response.status_code}"
                ) from e
            else:
                raise SparkAPIException(f"An error occurred: {str(e)}") from e

    @staticmethod
    def get_webui_address() -> Optional[str]:
        with open(DATABRICKS_METRICS_PROP_PATH) as f:
            properties = f.read()
        try:
            # Ignore line without `=` declaration
            properties_values = dict(line.split("=", 1) for line in properties.splitlines() if "=" in line)
            host = properties_values[HOST_KEY_NAME]
        except KeyError as e:
            if e.args[0] == HOST_KEY_NAME:
                # Might happen while provisioning the cluster, retry.
                return None
            raise DatabricksJobNameDiscoverException(f"Failed to get Databricks webui address {properties=}") from e
        except Exception as e:
            raise DatabricksJobNameDiscoverException(f"Failed to get Databricks webui address {properties=}") from e
        return f"{host}:{DEFAULT_WEBUI_PORT}"

    def extract_relevant_metadata(self) -> Optional[Dict[str, str]]:
        # Retry in case of a connection error, as the metrics server might not be up yet.
        start_time = time.monotonic()
        while time.monotonic() - start_time < DATABRICKS_JOBNAME_TIMEOUT_S:
            try:
                return self._fetch_cluster_all_tags()

            except Exception as e:
                self.logger.exception("Exception was raised while getting DBX environment metadata")
                if self.raise_on_failure:
                    raise e

            if not self.enable_retries:
                break

            # No environment metadata yet, retry.
            time.sleep(RETRY_INTERVAL_S)

        if self.raise_on_failure:
            raise DatabricksMetadataFetchException(
                f"Timed out getting DBX environment metadata after {DATABRICKS_JOBNAME_TIMEOUT_S}s"
            )
        self.logger.info("Databricks get DBX environment metadata timeout reached")
        return None

    def _discover_apps_url(self) -> bool:
        """
        Discovers the SparkUI apps url, and setting it to `self._apps_url`.
        Returns `True` if the url was discovered, `False` otherwise.
        """
        if self._apps_url is not None:  # Checks if the url was already discovered.
            return True
        else:
            if (web_ui_address := self.get_webui_address()) is None:
                return False
            self._apps_url = SPARKUI_APPS_URL_FORMAT.format(web_ui_address)
            self.logger.debug("Databricks SparkUI address", apps_url=self._apps_url)
            return True

    def _fetch_spark_apps(self) -> list:
        assert self._apps_url, "SparkUI apps url was not discovered"
        response = self._request_get(self._apps_url)
        try:
            apps = response.json()
        except Exception as e:
            if "Spark is starting up. Please wait a while until it's ready" in response.text:
                # Spark is still initializing, retrying.
                # https://github.com/apache/spark/blob/38c41c/core/src/main/scala/org/apache/spark/ui/SparkUI.scala#L64
                raise SparkNotReadyException("Spark is still initializing") from e
            else:
                raise SparkAPIException(f"Failed to parse apps url response, query {response.text=}") from e
        return apps

    def _fetch_spark_app_environment(self, app_id: str) -> Dict[str, Any]:
        assert self._apps_url is not None, "SparkUI apps url was not discovered"
        env_url = f"{self._apps_url}/{app_id}/environment"
        response = self._request_get(env_url)
        return response.json()

    def _fetch_cluster_all_tags(self) -> Dict[str, str]:
        """
        Returns `includes spark.databricks.clusterUsageTags.clusterAllTags` tags as `dict`.
        In any case this function returns `None`, a retry is required.
        """
        if not os.path.isfile(DATABRICKS_METRICS_PROP_PATH):
            self.logger.warning(f"{DATABRICKS_METRICS_PROP_PATH} was not found.")
            # We want to retry in case the cluster is still initializing, and the file is not yet deployed.
            raise SparkAppsURLDiscoveryException(f"{DATABRICKS_METRICS_PROP_PATH} was not found.")
        # Discovering SparkUI apps url.
        if self._discover_apps_url() is False:
            # SparkUI apps url was not discovered, retrying.
            self.logger.warning("SparkUI apps url was not discovered")
            raise SparkAppsURLDiscoveryException(f"{DATABRICKS_METRICS_PROP_PATH} couldn't be discovered.")

        # Getting spark apps in JSON format.
        apps = self._fetch_spark_apps()

        if len(apps) == 0:
            # apps might be empty because of initialization, retrying.
            raise DatabricksMetadataFetchException("Spark apps list is empty")

        if len(apps) > 1:
            raise DatabricksMetadataFetchException("Spark apps list contains more than one app")

        # Extracting for the first app the "sparkProperties" table of the application environment.
        full_spark_app_env = self._fetch_spark_app_environment(apps[0]["id"])
        spark_properties = full_spark_app_env.get("sparkProperties")
        if spark_properties is None:
            raise DatabricksMetadataFetchException(f"sparkProperties was not found in {full_spark_app_env=}")

        # Convert from [[key, val], [key, val]] to {key: val, key: val}
        try:
            spark_properties = dict(spark_properties)
        except Exception as e:
            raise DatabricksMetadataFetchException(f"Failed to parse as dict {full_spark_app_env=}") from e

        # First, trying to extract `CLUSTER_USAGE_ALL_TAGS_PROP` property, in case not redacted.
        result: Dict[str, str] = {}
        if (
            cluster_all_tags_value := spark_properties.get(CLUSTER_USAGE_ALL_TAGS_PROP)
        ) is not None and DATABRICKS_REDACTED_STR not in cluster_all_tags_value:
            try:
                cluster_all_tags_value_json = json.loads(cluster_all_tags_value)
            except Exception as e:
                raise DatabricksTagsExtractionException(f"Failed to parse {cluster_all_tags_value}") from e

            result.update(
                {cluster_all_tag["key"]: cluster_all_tag["value"] for cluster_all_tag in cluster_all_tags_value_json}
            )
        # As a fallback, trying to extract `CLUSTER_USAGE_CLUSTER_NAME_PROP` property.
        elif (cluster_name_value := spark_properties.get(CLUSTER_USAGE_CLUSTER_NAME_PROP)) is not None:
            self.logger.info(
                "Falling back to get cluster name from available/not redacted property",
            )
            result[CLUSTER_NAME_KEY] = cluster_name_value

        else:
            # We expect at least one of the properties to be present.
            raise DatabricksTagsExtractionException(
                f"Failed to extract {CLUSTER_USAGE_ALL_TAGS_PROP} or "
                f"{CLUSTER_USAGE_CLUSTER_NAME_PROP} from {spark_properties=}"
            )

        # Now add additional interesting data to the metadata
        for key in spark_properties:
            if key in CLUSTER_USAGE_RELEVANT_TAGS_PROPS:
                val = spark_properties[key]
                if DATABRICKS_REDACTED_STR not in val:
                    result[key] = val

        final_metadata = self._apply_pattern(result)
        self.logger.info(
            "Successfully got relevant cluster tags metadata",
            cluster_all_props=final_metadata,
        )
        return final_metadata

    @staticmethod
    def _apply_pattern(metadata: Dict[str, str]) -> Dict[str, str]:
        """
        Applies certain patterns on the metadata values.
        We mostly use the metadata values as service names, so we want to make sure the metadata values
        match some service name requirements.

        e.g.: Job Name might include spaces, we want to replace them with dashes.
        """
        if JOB_NAME_KEY in metadata:
            metadata[JOB_NAME_KEY] = metadata[JOB_NAME_KEY].replace(" ", "-").lower()
        if CLUSTER_NAME_KEY in metadata:
            # We've tackled cases where the cluster name includes Run ID, we want to remove it.
            metadata[CLUSTER_NAME_KEY] = re.sub(RUN_ID_REGEX, "", metadata[CLUSTER_NAME_KEY])
            metadata[CLUSTER_NAME_KEY] = metadata[CLUSTER_NAME_KEY].replace(" ", "-").lower()
        return metadata


def get_name_from_metadata(metadata: Dict[str, str]) -> Optional[str]:
    assert metadata is not None, "all_props_dict is None, can't get name from metadata"
    if job_name := metadata.get(JOB_NAME_KEY):
        return f"job-{job_name}"
    elif cluster_name := metadata.get(CLUSTER_NAME_KEY):
        return cluster_name
    return None
