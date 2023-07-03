from enum import Enum


class CollectorType(str, Enum):
    UNKOWN = "unknown"
    SAGENT = "sagent"
    GPROFILER = "gprofiler"
