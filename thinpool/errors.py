"""Exception types raised by the thinpool storage pool library."""


class PoolError(Exception):
    """Base class for all thinpool errors."""


class PoolExistsError(PoolError):
    """A pool already exists at the given paths."""


class PoolNotFoundError(PoolError):
    """No valid pool exists at the given paths."""


class InvalidPoolConfigError(PoolError):
    """Pool creation parameters are invalid."""


class VolumeExistsError(PoolError):
    """A volume with the same name already exists in the pool."""


class VolumeNotFoundError(PoolError):
    """The named volume does not exist in the pool."""


class QuotaExceededError(PoolError):
    """Nominal sizes would exceed capacity * oversubscription ratio."""


class PoolFullError(PoolError):
    """The write needs new physical blocks but the pool has none free."""


class OutOfBoundsError(PoolError):
    """The requested range falls outside the volume's nominal size."""
