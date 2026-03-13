from enum import Enum


class EloadType(str, Enum):
    HISTORICAL = "HISTORICAL"
    INCREMENTAL = "INCREMENTAL"
