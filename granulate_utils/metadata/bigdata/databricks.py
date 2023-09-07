from typing import Optional


def get_databricks_version() -> Optional[str]:
    try:
        with open("/databricks/DBR_VERSION", "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return None


def get_hadoop_version() -> Optional[str]:
    try:
        with open("/databricks/spark/HADOOP_VERSION", "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return None
