"""异常类型定义。"""


class PoolError(Exception):
    """存储池相关错误的基类。"""


class PoolExistsError(PoolError):
    """在已有池的位置重复建池。"""


class PoolNotFoundError(PoolError):
    """打开一个不存在的池。"""


class InvalidConfigError(PoolError):
    """建池参数非法（如容量不是块大小的整数倍）。"""


class VolumeExistsError(PoolError):
    """同名卷已存在。"""


class VolumeNotFoundError(PoolError):
    """卷不存在。"""


class OversubscriptionError(PoolError):
    """标称大小之和超出物理容量乘以超分比。"""


class PoolFullError(PoolError):
    """物理块用尽，需要新块的写入被整笔拒绝。"""


class OutOfBoundsError(PoolError):
    """读写范围超出卷的标称大小。"""


class PoolClosedError(PoolError):
    """池已关闭。"""


class CorruptPoolError(PoolError):
    """池文件损坏或一致性检查失败。"""
