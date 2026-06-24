"""pyprocess package."""

from pyprocess.pool import Future, ProcessPool, TaskError
from pyprocess.pool_service import WorkerService

__version__ = "0.1.0"

__all__ = ["Future", "ProcessPool", "TaskError", "WorkerService", "__version__"]
