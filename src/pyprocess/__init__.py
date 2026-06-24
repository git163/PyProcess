"""pyprocess package."""

from pyprocess.pool import Future, ProcessPool, TaskError

__version__ = "0.1.0"

__all__ = ["Future", "ProcessPool", "TaskError", "__version__"]
