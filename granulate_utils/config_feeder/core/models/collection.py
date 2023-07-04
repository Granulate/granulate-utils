from enum import Enum


class CollectorType(str, Enum):
    UNKNOWN = "unknown"
    SAGENT = "sagent"
    GPROFILER = "gprofiler"
