import json
import shutil
import subprocess
from typing import Dict, Optional


def _get_instance_data() -> Optional[Dict]:
    try:
        with open("/mnt/var/lib/info/extraInstanceData.json", "r") as f:
            obj = json.loads(f.read())
            if isinstance(obj, dict):
                return obj
    except FileNotFoundError:
        pass
    return None


def _get_job_flow() -> Optional[Dict]:
    try:
        with open("/mnt/var/lib/info/job-flow.json", "r") as f:
            obj = json.loads(f.read())
            if isinstance(obj, dict):
                return obj
    except FileNotFoundError:
        pass
    return None


def _get_emr_cluster_tags() -> Optional[Dict]:
    instance_data = _get_instance_data()
    if not instance_data:
        return None
    job_flow = _get_job_flow()
    if not job_flow:
        return None

    region = instance_data.get("region")
    cluster_id = job_flow.get("jobFlowId")
    proc = None
    timeout_counter = 0

    if not shutil.which("aws"):
        raise Exception("AWS CLI is not installed")

    while timeout_counter < 3:
        try:
            proc = subprocess.run(
                f"aws emr describe-cluster --cluster-id {cluster_id} --region {region} --query 'Cluster.Tags'",
                shell=True, capture_output=True, text=True, timeout=5)
            if proc.returncode == 0:
                return {item['Key']: item['Value'] for item in json.loads(proc.stdout)}
            else:
                raise Exception("EMR Describe Cluster return code is non zero: {}", proc.stderr)
        except subprocess.TimeoutExpired:
            timeout_counter += 1
    if timeout_counter >= 5:
        raise Exception("EMR Describe Cluster timed out")
    if not proc:
        raise Exception("EMR Describe Cluster failed")
    return None


def get_emr_metadata() -> Optional[Dict]:
    emr_cluster_tags = _get_emr_cluster_tags()
    if emr_cluster_tags is None:
        return None
    return {"tags": _get_emr_cluster_tags()}


def get_emr_version() -> Optional[str]:
    if (data := _get_instance_data()) is not None:
        release = data.get("releaseLabel")
        if isinstance(release, str):
            return release
    return None
