"""长生命周期进程池服务封装。

`WorkerService` 把 `ProcessPool` 包装为可长期持有的服务对象，适合在 Web 服务、
后台任务队列等需要反复提交任务的场景中使用。
"""

from __future__ import annotations

import threading
from typing import Any, Callable, TypeVar

from pyprocess.pool import Future, ProcessPool

T = TypeVar("T")

__all__ = ["ProcessPoolService"]


class ProcessPoolService:
    """长生命周期进程池服务。

    对 `ProcessPool` 做了一层薄封装，提供线程安全的提交接口和统一的生命周期管理。
    """

    def __init__(self, max_workers: int | None = None) -> None:
        self._max_workers = max_workers
        self._pool = ProcessPool(max_workers=max_workers)
        self._lock = threading.Lock()
        self._started = False
        self._shutdown = False

    def start(self) -> None:
        """启动服务，内部进程池开始运行。"""
        with self._lock:
            if self._started:
                return
            if self._shutdown:
                # 已经关闭过的实例允许重新启动：创建新的底层进程池。
                self._pool = ProcessPool(max_workers=self._max_workers)
                self._shutdown = False
            self._pool.start()
            self._started = True

    def submit(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> Future[T]:
        """提交异步任务。

        Args:
            func: 可 pickle 的可调用对象。
            *args: 位置参数。
            **kwargs: 关键字参数。

        Returns:
            Future: 用于获取结果的句柄。

        Raises:
            RuntimeError: 服务未启动或已关闭。
        """
        self._ensure_started()
        return self._pool.submit(func, *args, **kwargs)

    def submit_no_wait(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        """异步提交任务，不返回 Future。

        Args:
            func: 可 pickle 的可调用对象。
            *args: 位置参数。
            **kwargs: 关键字参数。

        Raises:
            RuntimeError: 服务未启动或已关闭。
        """
        self._ensure_started()
        self._pool.submit_no_wait(func, *args, **kwargs)

    def shutdown(self, wait: bool = True, timeout: float | None = None) -> None:
        """关闭服务并释放进程池资源。"""
        with self._lock:
            if self._shutdown:
                return
            self._shutdown = True
            self._started = False
        self._pool.shutdown(wait=wait, timeout=timeout)

    @property
    def is_running(self) -> bool:
        """服务是否处于运行状态。"""
        with self._lock:
            return self._started and not self._shutdown

    @property
    def worker_pids(self) -> list[int]:
        """返回当前工作进程的 PID 列表。"""
        return self._pool.worker_pids

    def _ensure_started(self) -> None:
        """确保服务已启动；未启动时自动启动，已关闭则抛异常。"""
        with self._lock:
            if self._shutdown:
                raise RuntimeError("ProcessPoolService has been shut down")
            if not self._started:
                self._pool.start()
                self._started = True

    def __enter__(self) -> ProcessPoolService:
        self.start()
        return self

    def __exit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> None:
        self.shutdown(wait=True)
