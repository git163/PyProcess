"""异步进程池实现。

提供基于 multiprocessing 的进程池，支持提交异步任务、等待结果、
优雅/强制关闭，并在收到信号时自动清理所有子进程。
"""

from __future__ import annotations

import logging
import multiprocessing as mp
import os
import queue
import signal
import threading
import time
import traceback
import uuid
from typing import Any, Callable, Generic, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

__all__ = ["Future", "ProcessPool", "TaskError"]

# 进程池默认配置常量，便于统一调整。
DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT: float = 5.0
"""shutdown 时等待工作者优雅退出的默认秒数。"""

DEFAULT_TERMINATE_JOIN_TIMEOUT: float = 1.0
"""发出 terminate 后等待工作者退出的秒数。"""

DEFAULT_KILL_JOIN_TIMEOUT: float = 1.0
"""发出 kill 后等待工作者退出的秒数。"""

DEFAULT_WORKER_POLL_INTERVAL: float = 0.5
"""孤儿检测与健康监控轮询间隔（秒）。"""

DEFAULT_TERMINATE_JOIN_TIMEOUT_NO_WAIT: float = 0.2
"""wait=False 时发出 terminate 后等待工作者退出的秒数。"""

DEFAULT_RESULT_QUEUE_TIMEOUT: float = 0.2
"""结果收集线程从结果队列取数据的超时时间（秒）。"""

SIGNAL_CLEANUP_BUDGET: float = 5.0
"""信号触发后，整个清理流程（优雅等待 + terminate + kill）的总预算（秒）。"""

SIGNAL_TERMINATE_JOIN_TIMEOUT: float = 0.5
"""信号触发关闭时，terminate 后等待工作者退出的秒数。"""

SIGNAL_KILL_JOIN_TIMEOUT: float = 0.5
"""信号触发关闭时，kill 后等待工作者退出的秒数。"""

SIGNAL_GRACEFUL_TIMEOUT: float = (
    SIGNAL_CLEANUP_BUDGET - SIGNAL_TERMINATE_JOIN_TIMEOUT - SIGNAL_KILL_JOIN_TIMEOUT
)
"""SIGTERM 触发优雅关闭时，给工作者的最长等待时间（秒）。

由 SIGNAL_CLEANUP_BUDGET 扣除 terminate / kill 阶段的预留时间后自动推导，
避免手动调整时超时常数错配。
"""

# 模块加载时校验：信号清理总预算必须足够覆盖各阶段。
if SIGNAL_GRACEFUL_TIMEOUT <= 0:
    raise ValueError(
        "SIGNAL_CLEANUP_BUDGET must be greater than the sum of "
        "SIGNAL_TERMINATE_JOIN_TIMEOUT and SIGNAL_KILL_JOIN_TIMEOUT"
    )

SHUTDOWN_SENTINEL: Any = None
"""任务队列关闭哨兵。"""


class TaskError(Exception):
    """任务执行过程中抛出的异常包装。"""

    def __init__(self, message: str, cause: BaseException | None = None):
        super().__init__(message)
        self.cause = cause


class Future(Generic[T]):
    """异步任务结果句柄。"""

    def __init__(self) -> None:
        self._event = threading.Event()
        self._result: Any = None
        self._error: TaskError | None = None
        self._done = False
        self._lock = threading.Lock()

    def done(self) -> bool:
        """返回任务是否已完成。"""
        with self._lock:
            return self._done

    def wait(self, timeout: float | None = None) -> bool:
        """等待任务完成，返回是否在超时前完成。"""
        return self._event.wait(timeout)

    def result(self, timeout: float | None = None) -> T:
        """等待并返回任务结果。

        Args:
            timeout: 最长等待秒数，None 表示无限等待。

        Raises:
            TimeoutError: 超时仍未完成。
            TaskError: 任务执行中抛出异常。
        """
        if not self._event.wait(timeout):
            raise TimeoutError("Timeout waiting for task result")
        with self._lock:
            if self._error is not None:
                raise self._error
            return self._result

    def _set_result(self, value: Any) -> None:
        with self._lock:
            self._result = value
            self._done = True
            self._event.set()

    def _set_error(self, error: TaskError) -> None:
        with self._lock:
            self._error = error
            self._done = True
            self._event.set()


def _orphan_watcher(original_ppid: int, interval: float = DEFAULT_WORKER_POLL_INTERVAL) -> None:
    """在工作者进程中运行，检测到父进程变化时主动退出。

    当父进程被 SIGKILL 等不可捕获信号终止后，子进程会被系统中最近的存活祖先收养。
    该守护线程发现当前 ppid 与启动时记录的父进程不一致即退出，避免残留。
    """
    pid = os.getpid()
    while True:
        time.sleep(interval)
        current_ppid = os.getppid()
        if current_ppid != original_ppid:
            logger.warning(
                "Worker %s detected parent change (ppid %s -> %s), exiting.",
                pid,
                original_ppid,
                current_ppid,
            )
            os._exit(1)


def _worker_entry(parent_pid: int, task_queue: mp.Queue, result_queue: mp.Queue) -> None:
    """工作者进程入口。"""
    # 父进程在创建工作者时传入其 PID，作为孤儿检测的基准。
    # 若启动时父进程已经消失（ppid 发生变化），直接退出。
    if os.getppid() != parent_pid:
        logger.warning(
            "Worker %s started with changed parent (expected %s, got %s), exiting.",
            os.getpid(),
            parent_pid,
            os.getppid(),
        )
        os._exit(1)

    watcher = threading.Thread(
        target=_orphan_watcher, args=(parent_pid, DEFAULT_WORKER_POLL_INTERVAL), daemon=True
    )
    watcher.start()

    while True:
        try:
            task = task_queue.get()
        except (OSError, EOFError):
            logger.info("Worker %s task queue closed, exiting.", os.getpid())
            break

        if task is SHUTDOWN_SENTINEL:
            logger.debug("Worker %s received shutdown sentinel.", os.getpid())
            break

        task_id = task["id"]
        try:
            func = task["func"]
            args = task["args"]
            kwargs = task["kwargs"]
            value = func(*args, **kwargs)
            result_queue.put({"id": task_id, "status": "ok", "value": value})
        except Exception as exc:  # noqa: BLE001
            logger.exception("Worker %s task %s failed.", os.getpid(), task_id)
            result_queue.put(
                {
                    "id": task_id,
                    "status": "error",
                    "exc_type": type(exc).__name__,
                    "exc_msg": str(exc),
                    "traceback": traceback.format_exc(),
                }
            )


class ProcessPool:
    """异步进程池。"""

    def __init__(self, max_workers: int | None = None) -> None:
        self._max_workers = max_workers or os.cpu_count() or 1
        self._ctx = mp.get_context("spawn")
        self._task_queue: mp.Queue | None = None
        self._result_queue: mp.Queue | None = None
        self._workers: list[mp.Process] = []
        self._started = False
        self._shutdown = False
        self._lock = threading.Lock()
        self._futures: dict[str, Future[Any]] = {}
        self._collector: threading.Thread | None = None
        self._signal_installed = False
        self._shutdown_event = threading.Event()
        self._signal_thread: threading.Thread | None = None
        self._health_thread: threading.Thread | None = None

    def start(self) -> None:
        """启动进程池，显式创建并启动工作进程。"""
        with self._lock:
            if self._started:
                return
            if self._shutdown:
                raise RuntimeError("Pool has been shut down")

            self._task_queue = self._ctx.Queue()
            self._result_queue = self._ctx.Queue()
            parent_pid = os.getpid()
            self._workers = [
                self._ctx.Process(
                    target=_worker_entry,
                    args=(parent_pid, self._task_queue, self._result_queue),
                    daemon=False,
                )
                for _ in range(self._max_workers)
            ]
            for worker in self._workers:
                worker.start()

            self._collector = threading.Thread(target=self._collect_results, daemon=True)
            self._collector.start()

            self._signal_thread = threading.Thread(target=self._signal_watcher, daemon=True)
            self._signal_thread.start()

            self._health_thread = threading.Thread(target=self._health_watcher, daemon=True)
            self._health_thread.start()

            self._install_signal_handlers()
            self._started = True

    @property
    def worker_pids(self) -> list[int]:
        """返回当前工作进程的 PID 列表（便于测试和监控）。"""
        with self._lock:
            return [w.pid for w in self._workers if w.pid is not None]

    @property
    def is_running(self) -> bool:
        """进程池是否处于运行状态。"""
        with self._lock:
            return self._started and not self._shutdown

    def _install_signal_handlers(self) -> None:
        """注册 SIGTERM/SIGINT 处理函数。"""
        if self._signal_installed:
            return
        try:
            signal.signal(signal.SIGTERM, self._signal_handler)
            signal.signal(signal.SIGINT, self._signal_handler)
            self._signal_installed = True
        except (ValueError, OSError):
            logger.debug("Cannot install signal handlers in current context.")

    def _signal_handler(self, signum: int, _frame: Any) -> None:
        """信号处理函数，仅记录信号并设置事件，由守护线程执行关闭。"""
        logger.warning("Received signal %s, requesting pool shutdown.", signum)
        self._shutdown_signal = signum
        self._shutdown_event.set()

    def _signal_watcher(self) -> None:
        """等待关闭事件，然后执行非信号安全的关闭逻辑。"""
        self._shutdown_event.wait()
        signum = getattr(self, "_shutdown_signal", None)
        # SIGTERM 通常表示外部要求优雅退出，给工作者默认的 graceful 时间；
        # SIGINT（Ctrl+C）等其它信号则快速关闭，避免用户等待过长。
        # 信号触发的清理总预算控制在约 5 秒：
        #   SIGTERM: 4s 优雅 + 0.5s terminate join + 0.5s kill join
        #   SIGINT:  0.2s terminate join + 0.5s kill join
        graceful = signum == signal.SIGTERM
        try:
            self.shutdown(
                wait=graceful,
                timeout=SIGNAL_GRACEFUL_TIMEOUT,
                _terminate_join_timeout=SIGNAL_TERMINATE_JOIN_TIMEOUT,
                _kill_join_timeout=SIGNAL_KILL_JOIN_TIMEOUT,
                _no_wait_terminate_join_timeout=DEFAULT_TERMINATE_JOIN_TIMEOUT_NO_WAIT,
            )
        except Exception:  # noqa: BLE001
            logger.exception("Error during signal-triggered shutdown.")

    def _health_watcher(self, interval: float = DEFAULT_WORKER_POLL_INTERVAL) -> None:
        """监控工作进程健康状态，发现异常死亡时主动关闭进程池。"""
        while True:
            time.sleep(interval)
            with self._lock:
                if self._shutdown or not self._started:
                    break
                workers = list(self._workers)

            dead_workers = [w for w in workers if not w.is_alive()]
            if dead_workers:
                logger.warning(
                    "Detected dead worker(s): %s. Initiating pool shutdown.",
                    [w.pid for w in dead_workers],
                )
                try:
                    self.shutdown(wait=False)
                except Exception:  # noqa: BLE001
                    logger.exception("Error during health-triggered shutdown.")
                break

    def _collect_results(self) -> None:
        """后台线程：从结果队列读取结果并填充 Future。"""
        while True:
            try:
                result = self._result_queue.get(timeout=DEFAULT_RESULT_QUEUE_TIMEOUT)
            except (OSError, EOFError):
                break
            except queue.Empty:
                if self._shutdown:
                    break
                continue

            if result is SHUTDOWN_SENTINEL:
                break

            task_id = result["id"]
            future = self._futures.pop(task_id, None)
            if future is None:
                logger.warning("Received result for unknown task %s.", task_id)
                continue

            if result.get("status") == "error":
                exc_type = result.get("exc_type", "Exception")
                exc_msg = result.get("exc_msg", "unknown error")
                tb = result.get("traceback", "")
                cause = RuntimeError(f"{exc_type}: {exc_msg}")
                error = TaskError(
                    f"Task failed with {exc_type}: {exc_msg}\n{tb}",
                    cause=cause,
                )
                future._set_error(error)
            else:
                future._set_result(result.get("value"))

    def submit(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> Future[T]:
        """提交异步任务。

        Args:
            func: 可 pickle 的可调用对象。
            *args: 位置参数。
            **kwargs: 关键字参数。

        Returns:
            Future: 用于获取结果的句柄。

        Raises:
            RuntimeError: 进程池已关闭。
        """
        with self._lock:
            if not self._started:
                self.start()
            if self._shutdown:
                raise RuntimeError("Pool has been shut down")

            task_id = str(uuid.uuid4())
            future: Future[T] = Future()
            self._futures[task_id] = future
            self._task_queue.put(
                {
                    "id": task_id,
                    "func": func,
                    "args": args,
                    "kwargs": kwargs,
                }
            )
            return future

    def submit_no_wait(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        """异步提交任务，不返回 Future。

        适用于只关心任务被执行、不关心结果或异常的场景。
        任务完成后由结果收集线程自动清理内部状态。

        Args:
            func: 可 pickle 的可调用对象。
            *args: 位置参数。
            **kwargs: 关键字参数。

        Raises:
            RuntimeError: 进程池已关闭。
        """
        # 内部仍创建 Future，用于消费结果队列、避免队列堵塞；
        # 不返回给调用方，由结果收集线程负责释放。
        self.submit(func, *args, **kwargs)

    def shutdown(
        self,
        wait: bool = True,
        timeout: float | None = None,
        *,
        _terminate_join_timeout: float = DEFAULT_TERMINATE_JOIN_TIMEOUT,
        _kill_join_timeout: float = DEFAULT_KILL_JOIN_TIMEOUT,
        _no_wait_terminate_join_timeout: float = DEFAULT_TERMINATE_JOIN_TIMEOUT_NO_WAIT,
    ) -> None:
        """关闭进程池。

        Args:
            wait: 是否等待工作进程优雅退出。
            timeout: 优雅等待的最长秒数；None 时使用内部默认值 5 秒。
                如果需要给长任务更多退出时间，可传入更大的值（如 30.0）。
        """
        with self._lock:
            if self._shutdown:
                return
            self._shutdown = True

            # 高负载场景下任务队列可能堆积大量待处理任务。直接发送关闭哨兵会导致
            # 哨兵排在所有待处理任务之后，工作者需处理完堆积任务才能退出，从而
            # 延迟关闭并可能超出超时时间。这里先排空队列中尚未被取走的任务，
            # 并立即取消对应的 Future；正在执行的任务保留其 Future，等待结果收集。
            pending_task_ids: set[str] = set()
            if self._task_queue is not None:
                try:
                    while True:
                        task = self._task_queue.get_nowait()
                        if task is not SHUTDOWN_SENTINEL and isinstance(task, dict):
                            pending_task_ids.add(task.get("id"))
                except queue.Empty:
                    pass
                except Exception:
                    logger.exception("Failed to drain task queue during shutdown.")

            for task_id in pending_task_ids:
                future = self._futures.pop(task_id, None)
                if future is not None:
                    future._set_error(TaskError("Pool shut down before task completed"))

            if self._task_queue is not None:
                try:
                    for _ in self._workers:
                        self._task_queue.put_nowait(SHUTDOWN_SENTINEL)
                except Exception:
                    logger.exception("Failed to send shutdown sentinel.")

            if self._result_queue is not None:
                try:
                    self._result_queue.put_nowait(SHUTDOWN_SENTINEL)
                except Exception:
                    pass

        # 先尝试让工作者自己退出（收到哨兵后正常返回）。
        if wait:
            graceful_timeout = DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT if timeout is None else timeout
            deadline = time.monotonic() + graceful_timeout
            for worker in self._workers:
                remaining = max(0.0, deadline - time.monotonic())
                worker.join(timeout=remaining)

        # 强制终止尚未退出的工作者；wait=False 时也执行，避免程序退出时被 atexit 阻塞。
        for worker in self._workers:
            if worker.is_alive():
                logger.warning("Worker %s did not exit gracefully, terminating.", worker.pid)
                worker.terminate()

        # 给被 terminate 的进程极短的时间自行清理，避免 atexit 长时间阻塞。
        join_timeout = _terminate_join_timeout if wait else _no_wait_terminate_join_timeout
        for worker in self._workers:
            if worker.is_alive():
                worker.join(timeout=join_timeout)

        for worker in self._workers:
            if worker.is_alive():
                logger.warning("Worker %s did not terminate, killing.", worker.pid)
                worker.kill()
                worker.join(timeout=_kill_join_timeout)

        with self._lock:
            for future in list(self._futures.values()):
                future._set_error(TaskError("Pool shut down before task completed"))
            self._futures.clear()

        if self._signal_installed:
            try:
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                signal.signal(signal.SIGINT, signal.SIG_DFL)
            except Exception:
                pass
            self._signal_installed = False

    def __enter__(self) -> ProcessPool:
        self.start()
        return self

    def __exit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> None:
        self.shutdown(wait=True)
