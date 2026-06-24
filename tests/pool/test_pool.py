"""进程池功能单元测试。"""

import time

import pytest

from pyprocess.pool import ProcessPool, TaskError


def _add(a: int, b: int) -> int:
    return a + b


def _sleep_and_return(value: float, duration: float) -> float:
    time.sleep(duration)
    return value


def _raise_value_error(message: str) -> None:
    raise ValueError(message)


def test_submit_and_result():
    """提交简单任务并验证结果。"""
    pool = ProcessPool(max_workers=2)
    pool.start()
    try:
        future = pool.submit(_add, 1, 2)
        assert future.result(timeout=5) == 3
        assert future.done()
    finally:
        pool.shutdown(wait=True)


def test_submit_multiple():
    """并发提交多个任务并验证结果。"""
    with ProcessPool(max_workers=4) as pool:
        futures = [pool.submit(_add, i, i) for i in range(10)]
        results = [f.result(timeout=5) for f in futures]
        assert results == [i * 2 for i in range(10)]


def test_future_timeout():
    """Future 超时等待应抛出 TimeoutError。"""
    with ProcessPool(max_workers=2) as pool:
        future = pool.submit(time.sleep, 5)
        with pytest.raises(TimeoutError):
            future.result(timeout=0.1)
        assert not future.done()


def test_exception_propagation():
    """任务异常应被包装为 TaskError 抛出。"""
    with ProcessPool(max_workers=2) as pool:
        future = pool.submit(_raise_value_error, "boom")
        with pytest.raises(TaskError) as exc_info:
            future.result(timeout=5)
        assert "boom" in str(exc_info.value)
        assert isinstance(exc_info.value.cause, RuntimeError)


def test_context_manager():
    """上下文管理器自动启动和关闭。"""
    with ProcessPool(max_workers=2) as pool:
        future = pool.submit(_add, 3, 4)
        assert future.result(timeout=5) == 7


def test_shutdown_then_submit():
    """关闭后再提交任务应抛 RuntimeError。"""
    pool = ProcessPool(max_workers=2)
    pool.start()
    pool.shutdown(wait=True)
    with pytest.raises(RuntimeError, match="shut down"):
        pool.submit(_add, 1, 2)


def test_double_shutdown():
    """重复关闭不应报错。"""
    pool = ProcessPool(max_workers=2)
    pool.start()
    pool.shutdown(wait=True)
    pool.shutdown(wait=True)


def test_normal_shutdown_no_orphans():
    """正常 shutdown 后所有工作进程应已退出。"""
    pool = ProcessPool(max_workers=3)
    pool.start()
    workers = list(pool._workers)
    pool.shutdown(wait=True)
    for worker in workers:
        assert not worker.is_alive()


def test_wait_method():
    """Future.wait 应在任务完成时返回 True。"""
    with ProcessPool(max_workers=2) as pool:
        future = pool.submit(_sleep_and_return, 42, 0.2)
        assert future.wait(timeout=5)
        assert future.result(timeout=0) == 42


def test_wait_method_timeout():
    """Future.wait 超时返回 False。"""
    with ProcessPool(max_workers=2) as pool:
        future = pool.submit(time.sleep, 5)
        assert not future.wait(timeout=0.1)


def test_shutdown_cancels_pending_futures():
    """关闭时应取消未完成的 Future。"""
    pool = ProcessPool(max_workers=1)
    pool.start()
    try:
        future = pool.submit(time.sleep, 60)
    finally:
        pool.shutdown(wait=False)
    assert future.done()
    with pytest.raises(TaskError, match="shut down"):
        future.result(timeout=0)


def _returns_none() -> None:
    return None


def _return_unpicklable() -> object:
    """返回一个不可 pickle 的局部函数对象。"""

    def _local() -> None:
        pass

    return _local


def _exit_task() -> None:
    """任务中主动调用 sys.exit。"""
    import sys

    sys.exit(42)


def test_submit_no_wait():
    """fire-and-forget 提交不返回 Future，任务仍正常执行。"""
    with ProcessPool(max_workers=2) as pool:
        pool.submit_no_wait(_add, 3, 4)
        pool.submit_no_wait(_add, 5, 6)
        # 给任务一点执行时间
        time.sleep(0.3)


def test_submit_no_wait_exception_does_not_propagate():
    """fire-and-forget 的任务异常不会抛到调用方。"""
    with ProcessPool(max_workers=2) as pool:
        pool.submit_no_wait(_raise_value_error, "silent boom")
        # 立即提交下一个正常任务，验证池子未被阻塞
        future = pool.submit(_add, 1, 2)
        assert future.result(timeout=5) == 3


def test_submit_then_submit_no_wait():
    """混合使用 submit 和 submit_no_wait。"""
    with ProcessPool(max_workers=2) as pool:
        pool.submit_no_wait(_add, 1, 2)
        future = pool.submit(_add, 3, 4)
        assert future.result(timeout=5) == 7


def test_shutdown_then_submit_no_wait():
    """关闭后再 fire-and-forget 提交应抛 RuntimeError。"""
    pool = ProcessPool(max_workers=2)
    pool.start()
    pool.shutdown(wait=True)
    with pytest.raises(RuntimeError, match="shut down"):
        pool.submit_no_wait(_add, 1, 2)

    """任务返回 None 时应正常处理。"""
    with ProcessPool(max_workers=2) as pool:
        future = pool.submit(_returns_none)
        assert future.result(timeout=5) is None


def test_result_called_multiple_times():
    """Future.result 可多次调用，结果一致。"""
    with ProcessPool(max_workers=2) as pool:
        future = pool.submit(_add, 2, 3)
        assert future.result(timeout=5) == 5
        assert future.result(timeout=5) == 5
        assert future.done()


def test_multiple_result_calls_after_exception():
    """异常任务的 Future.result 多次调用应始终抛出相同 TaskError。"""
    with ProcessPool(max_workers=2) as pool:
        future = pool.submit(_raise_value_error, "consistent error")
        with pytest.raises(TaskError, match="consistent error"):
            future.result(timeout=5)
        with pytest.raises(TaskError, match="consistent error"):
            future.result(timeout=5)


def test_unpicklable_arguments():
    """提交不可 pickle 的参数时，对应 Future 应超时；进程池本身不被阻塞。"""
    with ProcessPool(max_workers=2) as pool:
        # lambda 不可 pickle，队列序列化会失败
        bad_future = pool.submit(_add, (lambda x: x), 1)  # noqa: E731
        with pytest.raises(TimeoutError):
            bad_future.result(timeout=0.5)

        # 其他正常任务应仍能执行
        future = pool.submit(_add, 2, 3)
        assert future.result(timeout=5) == 5


def test_unpicklable_return_value():
    """任务返回不可 pickle 的结果时，对应 Future 会超时；进程池本身不被阻塞。"""
    with ProcessPool(max_workers=2) as pool:
        bad_future = pool.submit(_return_unpicklable)
        with pytest.raises(TimeoutError):
            bad_future.result(timeout=0.5)

        # 其他正常任务应仍能执行
        future = pool.submit(_add, 2, 3)
        assert future.result(timeout=5) == 5


def test_worker_sys_exit_in_task():
    """任务中调用 sys.exit 会导致工作进程退出，健康监控触发 shutdown。"""
    pool = ProcessPool(max_workers=2)
    pool.start()
    try:
        bad = pool.submit(_exit_task)
        pool.submit(_add, 1, 2)

        # 等待健康监控触发 shutdown
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if pool._shutdown:
                break
            time.sleep(0.05)
        assert pool._shutdown, "Health watcher should trigger shutdown after sys.exit"
    finally:
        pool.shutdown(wait=True)

    # 被 sys.exit 的 worker 对应的 future 会被 shutdown 取消
    with pytest.raises(TaskError, match="shut down"):
        bad.result(timeout=0)
