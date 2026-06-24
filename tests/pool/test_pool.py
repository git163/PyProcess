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
