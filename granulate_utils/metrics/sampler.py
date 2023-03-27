#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
# (C) Datadog, Inc. 2018-present. All rights reserved.
# Licensed under a 3-clause BSD style license (see LICENSE.bsd3).
#
import logging
import os
from datetime import datetime, timezone
from typing import List, Optional, Tuple
from xml.etree import ElementTree as ET

import psutil

from granulate_utils.exceptions import MissingExePath
from granulate_utils.linux.ns import resolve_host_path
from granulate_utils.linux.process import process_exe, search_for_process
from granulate_utils.metrics import Collector, MetricsSnapshot
from granulate_utils.metrics.mode import SPARK_MESOS_MODE, SPARK_STANDALONE_MODE, SPARK_YARN_MODE
from granulate_utils.metrics.spark import SparkApplicationMetricsCollector
from granulate_utils.metrics.yarn import YarnCollector

SPARK_MASTER_STATE_PATH = "/json"
SPARK_MASTER_APP_PATH = "/app/"

# COMMON urls
YARN_APPS_PATH = "ws/v1/cluster/apps"
YARN_CLUSTER_PATH = "ws/v1/cluster/metrics"
YARN_NODES_PATH = "ws/v1/cluster/nodes"
SPARK_APPS_PATH = "api/v1/applications"
MESOS_MASTER_APP_PATH = "/frameworks"

FIND_CLUSTER_TIMEOUT_SECS = 10 * 60


class BigDataSampler:
    """
    Spark cluster metrics sampler
    """

    def __init__(
        self,
        logger: logging.LoggerAdapter,
        hostname: str,
        master_address: Optional[str],
        cluster_mode: Optional[str],
        applications_metrics: Optional[bool] = False,
    ):
        self._logger = logger
        self._hostname = hostname
        self._applications_metrics = applications_metrics
        self._spark_samplers: List[Collector] = []

        if (cluster_mode is not None) and (master_address is not None):
            # No need to guess cluster mode and master address
            self._cluster_mode = cluster_mode
            self._master_address = f"http://{master_address}"
        elif (cluster_mode is None) and (master_address is None):
            # Guess cluster mode and master address
            cluster_conf = self._guess_cluster_mode()
            if cluster_conf is not None:
                self._master_address, self._cluster_mode = cluster_conf

        # In Standalone and Mesos we'd use applications metrics
        if self._cluster_mode in (SPARK_STANDALONE_MODE, SPARK_MESOS_MODE):
            self._applications_metrics = True

    def _get_yarn_config_path(self, process: psutil.Process) -> str:
        env = process.environ()
        if "HADOOP_CONF_DIR" in env:
            path = env["HADOOP_CONF_DIR"]
            self._logger.debug("Found HADOOP_CONF_DIR variable", hadoop_conf_dir=path)
        else:
            path = "/etc/hadoop/conf/"
            self._logger.info("Could not find HADOOP_CONF_DIR variable, using default path", hadoop_conf_dir=path)
        return os.path.join(path, "yarn-site.xml")

    def _get_yarn_config(self, process: psutil.Process) -> Optional[ET.Element]:
        config_path = self._get_yarn_config_path(process)

        self._logger.debug("Trying to open yarn config file for reading", config_path=config_path)
        try:
            # resolve config path against process' filesystem root
            process_relative_config_path = resolve_host_path(process, self._get_yarn_config_path(process))
            with open(process_relative_config_path, "rb") as conf_file:
                config_xml_string = conf_file.read()
            return ET.fromstring(config_xml_string)
        except FileNotFoundError:
            return None

    def _get_yarn_config_property(
        self, process: psutil.Process, requested_property: str, default: Optional[str] = None
    ) -> Optional[str]:
        config = self._get_yarn_config(process)
        if config is not None:
            for config_property in config.iter("property"):
                name_property = config_property.find("name")
                if name_property is not None and name_property.text == requested_property:
                    value_property = config_property.find("value")
                    if value_property is not None:
                        return value_property.text
        return default

    def _guess_standalone_master_webapp_address(self, process: psutil.Process) -> str:
        """
        Selects the master address for a standalone cluster.
        Uses master_address if given.
        """
        if self._master_address:
            return self._master_address
        else:
            master_ip = self._get_master_process_arg_value(process, "--host")
            master_port = self._get_master_process_arg_value(process, "--webui-port")
            return f"{master_ip}:{master_port}"

    def _get_master_process_arg_value(self, process: psutil.Process, arg_name: str) -> Optional[str]:
        process_args = process.cmdline()
        if arg_name in process_args:
            try:
                return process_args[process_args.index(arg_name) + 1]
            except IndexError:
                self._logger.exception("Could not find value for argument", arg_name=arg_name)
        return None

    def _guess_yarn_resource_manager_webapp_address(self, resource_manager_process: psutil.Process) -> str:
        config = self._get_yarn_config(resource_manager_process)

        if config is not None:
            for config_property in config.iter("property"):
                name_property = config_property.find("name")
                if (
                    name_property is not None
                    and name_property.text is not None
                    and name_property.text.startswith("yarn.resourcemanager.webapp.address")
                ):
                    value_property = config_property.find("value")
                    if value_property is not None and value_property.text is not None:
                        return value_property.text

        if self._master_address is not None:
            return self._master_address
        else:
            host_name = self._get_yarn_host_name(resource_manager_process)
            return host_name + ":8088"

    def _guess_mesos_master_webapp_address(self, process: psutil.Process) -> str:
        """
        Selects the master address for a mesos-master running on this node. Uses master_address if given, or defaults
        to my hostname.
        """
        if self._master_address:
            return self._master_address
        else:
            host_name = self._hostname
            return host_name + ":5050"

    def _get_yarn_host_name(self, resource_manager_process: psutil.Process) -> str:
        """
        Selects the master adderss for a ResourceManager running on this node - this parses the YARN config to
        get the hostname, and if not found, defaults to my hostname.
        """
        hostname = self._get_yarn_config_property(resource_manager_process, "yarn.resourcemanager.hostname")
        if hostname is not None:
            self._logger.debug(
                "Selected hostname from yarn.resourcemanager.hostname config", resourcemanager_hostname=hostname
            )
        else:
            hostname = self._hostname
            self._logger.debug("Selected hostname from my hostname", resourcemanager_hostname=hostname)
        return hostname

    def _is_yarn_master_collector(self, resource_manager_process: psutil.Process) -> bool:
        """
        yarn lists the addresses of the other masters in order communicate with
        other masters, so we can choose one of them (like rm1) and run the
        collection only on it so we won't get the same metrics for the cluster
        multiple times the rm1 hostname is in both EMR and Azure using the internal
        DNS and it's starts with the host name.

        For example, in AWS EMR:
        rm1 = 'ip-10-79-63-183.us-east-2.compute.internal:8025'
        where the hostname is 'ip-10-79-63-183'.

        In Azure:
        'rm1 = hn0-nrt-hb.3e3rqto3nr5evmsjbqz0pkrj4g.tx.internal.cloudapp.net:8050'
        where the hostname is 'hn0-nrt-hb.3e3rqto3nr5evmsjbqz0pkrj4g'
        """
        rm1_address = self._get_yarn_config_property(resource_manager_process, "yarn.resourcemanager.address.rm1", None)
        host_name = self._get_yarn_host_name(resource_manager_process)

        if rm1_address is None:
            self._logger.info(
                "yarn.resourcemanager.address.rm1 is not defined in config, so it's a single master deployment,"
                " enabling Spark collector"
            )
            return True
        elif rm1_address.startswith(host_name):
            self._logger.info(
                f"This is the collector master, because rm1: {rm1_address!r}"
                f" starts with my host name: {host_name!r}, enabling Spark collector"
            )
            return True
        else:
            self._logger.info(
                f"This is not the collector master, because rm1: {rm1_address!r}"
                f" does not start with my host name: {host_name!r}, skipping Spark collector on this YARN master"
            )
            return False

    def _get_spark_manager_process(self) -> Optional[psutil.Process]:
        def is_master_process(process: psutil.Process) -> bool:
            try:
                return (
                    "org.apache.hadoop.yarn.server.resourcemanager.ResourceManager" in process.cmdline()
                    or "org.apache.spark.deploy.master.Master" in process.cmdline()
                    or "mesos-master" in process_exe(process)
                )
            except MissingExePath:
                return False

        try:
            return next(search_for_process(is_master_process))
        except StopIteration:
            return None

    def _guess_cluster_mode(self) -> Optional[Tuple[str, str]]:
        """
        Guess the cluster mode and master address
        Enables `applications_metrics` in case of Standalone or Mesos
        returns (master address, cluster mode)
        """
        spark_master_process = self._get_spark_manager_process()
        spark_cluster_mode = "unknown"
        webapp_url = None

        if spark_master_process is None:
            self._logger.debug("Could not find any spark master process (resource manager or spark master)")
            return None

        if "org.apache.hadoop.yarn.server.resourcemanager.ResourceManager" in spark_master_process.cmdline():
            if not self._is_yarn_master_collector(spark_master_process):
                return None
            spark_cluster_mode = SPARK_YARN_MODE
            webapp_url = self._guess_yarn_resource_manager_webapp_address(spark_master_process)
        elif "org.apache.spark.deploy.master.Master" in spark_master_process.cmdline():
            spark_cluster_mode = SPARK_STANDALONE_MODE
            webapp_url = self._guess_standalone_master_webapp_address(spark_master_process)
        elif "mesos-master" in process_exe(spark_master_process):
            spark_cluster_mode = SPARK_MESOS_MODE
            webapp_url = self._guess_mesos_master_webapp_address(spark_master_process)

        if spark_master_process is None or webapp_url is None or spark_cluster_mode == "unknown":
            self._logger.warning("Could not get proper Spark cluster configuration")
            return None

        self._logger.info("Guessed settings are", cluster_mode=spark_cluster_mode, webapp_url=webapp_url)

        return webapp_url, spark_cluster_mode

    def _init_collectors(self):
        """
        This function fills in self._spark_samplers with the appropriate collectors.
        """
        if self._cluster_mode == SPARK_YARN_MODE:
            self._spark_samplers.append(YarnCollector(self._master_address, self._logger))
        elif self._cluster_mode == SPARK_STANDALONE_MODE or self._cluster_mode == SPARK_MESOS_MODE:
            self._spark_samplers.append(
                SparkApplicationMetricsCollector(self._cluster_mode, self._master_address, self._logger)
            )

    def discover(self) -> Optional[bool]:
        """
        I guess every sampler should have this method, so TODO is to make it abstract in a base class.
        return a boolean so the caller can check if the discovery was successful or not, and set it's own timeout.

        For every collector we need to know:
        1. the cluster mode (yarn, standalone, mesos)
        2. the master address

        returns True if we have these configurations, False otherwise
        """
        have_conf = False
        if self._master_address is None or self._cluster_mode is None:
            self._logger.debug("Trying to guess cluster mode and master address")
            cluster_conf = self._guess_cluster_mode()
            if cluster_conf is not None:
                self._master_address, self._cluster_mode = cluster_conf
                self._logger.info(
                    "Guessed cluster mode and master address",
                    cluster_mode=self._cluster_mode,
                    master_address=self._master_address,
                )
                have_conf = True
        else:
            self._logger.info(
                "We already know cluster mode and master address",
                cluster_mode=self._cluster_mode,
                master_address=self._master_address,
            )
            have_conf = True

        if have_conf and self._spark_samplers == []:
            self._init_collectors()

        return have_conf

    def collect_loop_helper(self) -> Optional[MetricsSnapshot]:
        """
        This function will be used in a collector loop.
        It will take care of all the logic to collect metrics from Spark, without any backend communication.
        It will return MetricsSnapshot object.
        """
        if self._spark_samplers:
            collected = []
            for collector in self._spark_samplers:
                for sample in collector.collect():
                    collected.append(sample)
            # No need to submit samples that don't actually have a value:
            samples = tuple(filter(lambda s: s.value is not None, collected))
            snapshot = MetricsSnapshot(datetime.now(tz=timezone.utc), samples)
            return snapshot

        # If we don't have any samplers, we don't have any metrics to collect:
        return None
