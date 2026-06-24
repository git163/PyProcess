"""pyprocess package."""

from pyprocess.pool import Future, ProcessPool, TaskError
from pyprocess.pool_service import ProcessPoolService

__version__ = "0.1.0"

__all__ = ["Future", "ProcessPool", "TaskError", "ProcessPoolService", "__version__"]
