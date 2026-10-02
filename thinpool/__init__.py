"""thinpool: 精简单配置（thin-provisioned）持久卷存储池库。

卷数据存放在一个固定大小的本地数据文件中，元数据存放在 SQLite 中。
"""

from .errors import (
    CorruptPoolError,
    InvalidConfigError,
    OutOfBoundsError,
    OversubscriptionError,
    PoolClosedError,
    PoolError,
    PoolExistsError,
    PoolFullError,
    PoolNotFoundError,
    VolumeExistsError,
    VolumeNotFoundError,
)
from .pool import BLOCK_SIZE, StoragePool

__all__ = [
    "BLOCK_SIZE",
    "StoragePool",
    "CorruptPoolError",
    "InvalidConfigError",
    "OutOfBoundsError",
    "OversubscriptionError",
    "PoolClosedError",
    "PoolError",
    "PoolExistsError",
    "PoolFullError",
    "PoolNotFoundError",
    "VolumeExistsError",
    "VolumeNotFoundError",
]
