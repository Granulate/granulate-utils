#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
# (C) Datadog, Inc. 2018-present. All rights reserved.
# Licensed under a 3-clause BSD style license (see LICENSE.bsd3).
#
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import psutil

from granulate_utils.exceptions import MissingExePath
from granulate_utils.linux.ns import resolve_host_path
from granulate_utils.linux.process import process_exe, search_for_process
from granulate_utils.metrics import Collector, MetricsSnapshot, Sample
from granulate_utils.metrics.mode import SPARK_MESOS_MODE, SPARK_STANDALONE_MODE, SPARK_YARN_MODE
from granulate_utils.metrics.spark import SparkApplicationMetricsCollector
from granulate_utils.metrics.yarn import YarnCollector, YarnNodeInfo, get_yarn_node_info
from granulate_utils.metrics.yarn.utils import parse_config_xml

FIND_CLUSTER_TIMEOUT_SECS = 10 * 60


class Sampler(ABC):
    @abstractmethod
    def discover(self) -> bool:
        """
        The collector's main loop will use this function to determine what Collector's to enabled, if to enable at all.
        returns True if we have these configurations, False otherwise

        returns True if discover succeeded, False otherwise
        """

        pass

    @abstractmethod
    def snapshot(self) -> Optional[MetricsSnapshot]:
        """
        This function will be used in a collector loop.
        It will take care of all the logic to collect metrics from Spark, without any backend communication.

        returns Optional[MetricsSnapshot].
        """
        pass


class BigDataSampler(Sampler):
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
        self._collectors: List[Collector] = []
        self._master_address: Optional[str] = None
        self._cluster_mode: Optional[str] = None
        self._yarn_node_info: Optional[YarnNodeInfo] = None

        assert (cluster_mode is None) == (
            master_address is None
        ), "cluster_mode and master_address must be configured together, or not at all"

        if (cluster_mode is not None) and (master_address is not None):
            # No need to guess cluster mode and master address
            self._cluster_mode = cluster_mode
            self._master_address = f"http://{master_address}"

    def _get_yarn_config_path(self, process: psutil.Process) -> str:
        env = process.environ()
        if "HADOOP_CONF_DIR" in env:
            path = env["HADOOP_CONF_DIR"]
            self._logger.debug("Found HADOOP_CONF_DIR variable", hadoop_conf_dir=path)
        else:
            path = "/etc/hadoop/conf/"
            self._logger.info("Could not find HADOOP_CONF_DIR variable, using default path", hadoop_conf_dir=path)
        return os.path.join(path, "yarn-site.xml")

    def _get_yarn_config(self, process: psutil.Process) -> Optional[Dict[str, str]]:
        config_path = self._get_yarn_config_path(process)

        self._logger.debug("Trying to open yarn config file for reading", config_path=config_path)
        try:
            # resolve config path against process' filesystem root
            process_relative_config_path = resolve_host_path(process, self._get_yarn_config_path(process))
            with open(process_relative_config_path, "r") as conf_file:
                return parse_config_xml(conf_file.read())
        except FileNotFoundError:
            return None

    def _guess_standalone_master_webapp_address(self, process: psutil.Process) -> str:
        """
        Selects the master address for a standalone cluster.
        Uses master_address if given.
        """
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

    def _guess_mesos_master_webapp_address(self, process: psutil.Process) -> str:
        """
        Selects the master address for a mesos-master running on this node. Uses master_address if given, or defaults
        to my hostname.
        """
        return self._hostname + ":5050"

    def _is_yarn_master_collector(self) -> bool:
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
        if self._yarn_node_info is None or self._yarn_node_info.resource_manager_index is None:
            return False

        rm_addresses = self._yarn_node_info.resource_manager_webapp_addresses
        rm1_address = rm_addresses[0]

        if len(rm_addresses) == 1:
            self._logger.info(
                "yarn.resourcemanager.address.rm1 is not defined in config, so it's a single master deployment,"
                " enabling Spark collector"
            )
            return True

        if self._yarn_node_info.is_first_resource_manager:
            self._logger.info(
                f"This is the collector master, because rm1: {rm1_address!r}"
                f" starts with my host name: {self._hostname!r}, enabling Spark collector"
            )
            return True

        self._logger.info(
            f"This is not the collector master, because rm1: {rm1_address!r}"
            f" does not start with my host name: {self._hostname!r}, skipping Spark collector on this YARN master"
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
        Guess the cluster mode and master address, depends on cluster mode.

        returns (master address, cluster mode)
        """
        spark_master_process = self._get_spark_manager_process()
        spark_cluster_mode = "unknown"
        webapp_url = None

        if spark_master_process is None:
            return None

        if "org.apache.hadoop.yarn.server.resourcemanager.ResourceManager" in spark_master_process.cmdline():
            if (yarn_config := self._get_yarn_config(spark_master_process)) is None:
                return None
            self._yarn_node_info = get_yarn_node_info(logger=self._logger, yarn_config=yarn_config)
            if self._yarn_node_info is None:
                return None
            if not self._is_yarn_master_collector():
                return None
            spark_cluster_mode = SPARK_YARN_MODE
            webapp_url = self._yarn_node_info.resource_manager_webapp_addresses[0]
        elif "org.apache.spark.deploy.master.Master" in spark_master_process.cmdline():
            spark_cluster_mode = SPARK_STANDALONE_MODE
            webapp_url = self._guess_standalone_master_webapp_address(spark_master_process)
        elif "mesos-master" in process_exe(spark_master_process):
            spark_cluster_mode = SPARK_MESOS_MODE
            webapp_url = self._guess_mesos_master_webapp_address(spark_master_process)

        if spark_master_process is None or webapp_url is None or spark_cluster_mode == "unknown":
            self._logger.warning("Could not get proper Spark cluster configuration")
            return None

        self._logger.info("Guessed settings", cluster_mode=spark_cluster_mode, webapp_url=webapp_url)

        return webapp_url, spark_cluster_mode

    def _init_collectors(self):
        """
        This function fills in self._spark_samplers with the appropriate collectors.
        """
        if self._cluster_mode == SPARK_YARN_MODE:
            self._collectors.append(YarnCollector(self._master_address, self._logger))

        # In Standalone and Mesos we'd use applications metrics
        if self._cluster_mode in (SPARK_STANDALONE_MODE, SPARK_MESOS_MODE):
            self._applications_metrics = True

        if self._applications_metrics:
            self._collectors.append(
                SparkApplicationMetricsCollector(self._cluster_mode, self._master_address, self._logger)
            )

    def _validate_manual_configuration(self) -> bool:
        """
        Validates the manual configuration of master_address and cluster_mode.
        """
        if self._cluster_mode == SPARK_YARN_MODE and self._yarn_node_info is None:
            self._yarn_node_info = get_yarn_node_info(logger=self._logger)
            if self._yarn_node_info is None:
                self._logger.debug("YARN not detected")
                return False
            if not self._yarn_node_info.is_resource_manager:
                self._logger.debug("This is not a YARN ResourceManager node")
                return False
            if not self._yarn_node_info.is_first_resource_manager:
                self._logger.debug("This is not the first YARN ResourceManager node")
                return False
            rm1_address = self._yarn_node_info.resource_manager_webapp_addresses[0]
            if self._master_address != rm1_address:
                self._logger.debug(
                    f"YARN ResourceManager address {rm1_address!r} does not match"
                    f" manually configured address {self._master_address!r}"
                )
                return False

        return True

    def discover(self) -> bool:
        """
        Discovers:
        1. the cluster mode (yarn, standalone, mesos)
        2. the master address

        returns True if we have these configurations, False otherwise
        """
        assert self._collectors == [], "discover() should only be called once"

        have_conf = False

        if self._master_address is not None and self._cluster_mode is not None:
            if self._validate_manual_configuration():
                # No need to guess, manually configured
                self._logger.debug(
                    "No need to guess cluster mode and master address, manually configured",
                    cluster_mode=self._cluster_mode,
                    master_address=self._master_address,
                )
                have_conf = True
            else:
                self._logger.error("Manually configured cluster mode and master address are invalid, skipping sampler")
        else:
            cluster_conf = self._guess_cluster_mode()
            if cluster_conf is not None:
                master_address, self._cluster_mode = cluster_conf
                self._master_address = f"http://{master_address}"
                have_conf = True

        if have_conf:
            self._init_collectors()

        return have_conf

    def snapshot(self) -> Optional[MetricsSnapshot]:
        """
        Returns a MetricsSnapshot with the collected metrics.
        """
        if self._collectors:
            collected: List[Sample] = []
            for collector in self._collectors:
                collected.extend(collector.collect())
            # No need to submit samples that don't actually have a value:
            samples = tuple(filter(lambda s: s.value is not None, collected))
            return MetricsSnapshot(datetime.now(tz=timezone.utc), samples)

        # If we don't have any samplers, we don't have any metrics to collect:
        return None
