"""进程池压力与鲁棒性测试。"""

import os
import signal
import threading
import time
from concurrent.futures import TimeoutError as FutureTimeoutError

import pytest

from pyprocess.pool import ProcessPool, TaskError


def _pid_exists(pid: int) -> bool:
    """通过发送信号 0 判断进程是否存在。"""
    try:
        os.kill(pid, 0)
    except (OSError, ProcessLookupError):
        return False
    return True


def _identity(value: int) -> int:
    return value


def _add_one(value: int) -> int:
    return value + 1


def _maybe_raise(value: int) -> int:
    if value % 2 == 0:
        raise ValueError(f"even value: {value}")
    return value


def _sleep_and_return(value: float, duration: float) -> float:
    time.sleep(duration)
    return value


def test_mass_tasks():
    """提交大量任务并验证全部完成且结果正确。"""
    task_count = 1000
    with ProcessPool(max_workers=8) as pool:
        futures = [pool.submit(_identity, i) for i in range(task_count)]
        results = [f.result(timeout=30) for f in futures]
    assert results == list(range(task_count))


def test_concurrent_submission():
    """多线程并发提交任务。"""
    thread_count = 10
    tasks_per_thread = 100
    results_lock = threading.Lock()
    results: list[int] = []
    errors: list[BaseException] = []

    def submit_batch(start: int) -> None:
        try:
            batch = [pool.submit(_add_one, i) for i in range(start, start + tasks_per_thread)]
            for f in batch:
                val = f.result(timeout=30)
                with results_lock:
                    results.append(val)
        except BaseException as exc:  # noqa: BLE001
            with results_lock:
                errors.append(exc)

    with ProcessPool(max_workers=8) as pool:
        threads = [
            threading.Thread(target=submit_batch, args=(i * tasks_per_thread,))
            for i in range(thread_count)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    assert not errors, f"Concurrent submission encountered errors: {errors}"
    assert sorted(results) == [i + 1 for i in range(thread_count * tasks_per_thread)]


def test_mixed_fast_slow_tasks():
    """混合长短任务，验证快速任务不会被慢任务阻塞。"""
    with ProcessPool(max_workers=4) as pool:
        slow = [pool.submit(_sleep_and_return, i, 0.3) for i in range(4)]
        fast = [pool.submit(_identity, i) for i in range(20)]

        # 快速任务应优先完成
        fast_results = [f.result(timeout=5) for f in fast]
        assert fast_results == list(range(20))

        slow_results = [f.result(timeout=10) for f in slow]
        assert slow_results == list(range(4))


def test_exception_storm():
    """大量任务中混合异常，验证异常正确传播且不影响其他任务。"""
    task_count = 200
    with ProcessPool(max_workers=8) as pool:
        futures = [pool.submit(_maybe_raise, i) for i in range(task_count)]

        successes = 0
        failures = 0
        for i, f in enumerate(futures):
            try:
                result = f.result(timeout=10)
                assert result == i
                successes += 1
            except TaskError:
                failures += 1

    assert successes == task_count // 2
    assert failures == task_count // 2


def test_large_payload():
    """传输较大任务负载。"""
    size = 2 * 1024 * 1024  # 2MB
    data = list(range(size))

    with ProcessPool(max_workers=2) as pool:
        future = pool.submit(sum, data)
        assert future.result(timeout=30) == sum(data)


def test_rapid_start_shutdown():
    """频繁启动和关闭进程池。"""
    for _ in range(20):
        pool = ProcessPool(max_workers=2)
        pool.start()
        future = pool.submit(_identity, 42)
        assert future.result(timeout=5) == 42
        pool.shutdown(wait=True)


def test_sustained_load():
    """持续一段时间高负载提交。"""
    batch_size = 50
    total_batches = 20
    with ProcessPool(max_workers=8) as pool:
        all_results: list[int] = []
        for batch in range(total_batches):
            futures = [pool.submit(_identity, batch * batch_size + i) for i in range(batch_size)]
            batch_results = [f.result(timeout=30) for f in futures]
            all_results.extend(batch_results)

    expected = list(range(batch_size * total_batches))
    assert sorted(all_results) == expected


def test_result_timeout_under_load():
    """高负载下单个任务超时，其他任务正常完成。"""
    with ProcessPool(max_workers=4) as pool:
        slow_future = pool.submit(time.sleep, 10)
        fast_futures = [pool.submit(_identity, i) for i in range(20)]

        with pytest.raises(FutureTimeoutError):
            slow_future.result(timeout=0.1)

        fast_results = [f.result(timeout=5) for f in fast_futures]
        assert fast_results == list(range(20))

        pool.shutdown(wait=False)
        assert slow_future.done()


def test_submit_after_worker_death():
    """kill 一个空闲工作进程后，进程池仍能继续处理其他任务。"""
    with ProcessPool(max_workers=4) as pool:
        # 先让池子热身，确保所有工作者都已启动
        futures = [pool.submit(_identity, i) for i in range(4)]
        for f in futures:
            f.result(timeout=5)

        pids = pool.worker_pids
        assert len(pids) == 4

        # 杀死一个工作者（此时它应该空闲）
        victim = pids[0]
        os.kill(victim, signal.SIGKILL)

        # 等待系统确认进程已消失，并给其他工作者处理时间
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                os.kill(victim, 0)
            except (OSError, ProcessLookupError):
                break
            time.sleep(0.05)

        # 确认仍有 3 个工作者存活
        remaining_pids = [pid for pid in pids if pid != victim]
        for pid in remaining_pids:
            assert _pid_exists(pid), f"Worker {pid} died unexpectedly"

        # 剩余工作者应继续处理任务；由于容量减少，给更充裕的超时
        futures = [pool.submit(_add_one, i) for i in range(8)]
        results = sorted(f.result(timeout=30) for f in futures)
        assert results == [i + 1 for i in range(8)]


def test_pool_shutdown_after_worker_killed():
    """工作者被 kill 后，shutdown 仍能干净退出，不残留进程。"""
    pool = ProcessPool(max_workers=2)
    pool.start()
    try:
        # 提交一个长时间任务，确保工作者处于忙碌状态
        pool.submit(time.sleep, 60)
        time.sleep(0.3)  # 让任务被取走
        pids = pool.worker_pids
        assert len(pids) == 2

        # 随机杀死一个工作者
        os.kill(pids[0], signal.SIGKILL)
    finally:
        pool.shutdown(wait=True)

    # 被 kill 的工作者若变成僵尸，waitpid 会回收；仍存活则 assert 失败
    for pid in pids:
        try:
            os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            pass
        try:
            os.kill(pid, 0)
        except (OSError, ProcessLookupError):
            continue
        pytest.fail(f"Residual worker process detected: {pid}")


def test_many_short_tasks_with_few_workers():
    """少量工作者处理海量短任务，验证吞吐和正确性。"""
    task_count = 500
    with ProcessPool(max_workers=2) as pool:
        futures = [pool.submit(_add_one, i) for i in range(task_count)]
        results = sorted(f.result(timeout=30) for f in futures)
    assert results == [i + 1 for i in range(task_count)]


def test_reuse_after_context_manager():
    """多次使用上下文管理器创建不同池子。"""
    for i in range(5):
        with ProcessPool(max_workers=2) as pool:
            future = pool.submit(_identity, i)
            assert future.result(timeout=5) == i
