"""pyprocess package."""

# 注意：ProcessPool 为内部实现，仅供 ProcessPoolService 使用，不对外公开。
# 对外入口统一使用 ProcessPoolService；Future / TaskError 是其 submit/result 的契约类型。
from pyprocess.pool import Future, TaskError
from pyprocess.pool_service import ProcessPoolService

__version__ = "0.1.0"

__all__ = ["Future", "TaskError", "ProcessPoolService", "__version__"]
