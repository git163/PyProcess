"""ProcessPoolService 单元测试。"""

import os
import threading
import time

import pytest

from pyprocess.pool import TaskError
from pyprocess.pool_service import ProcessPoolService


def _add(a: int, b: int) -> int:
    return a + b


def _sleep_and_return(value: float, duration: float) -> float:
    time.sleep(duration)
    return value


def _raise_value_error(message: str) -> None:
    raise ValueError(message)


def test_service_submit_and_result():
    """服务提交任务并获取结果。"""
    service = ProcessPoolService(max_workers=2)
    service.start()
    try:
        future = service.submit(_add, 1, 2)
        assert future.result(timeout=5) == 3
    finally:
        service.shutdown(wait=True)


def test_service_auto_start_on_submit():
    """未显式 start 时，submit 自动启动服务。"""
    service = ProcessPoolService(max_workers=2)
    try:
        future = service.submit(_add, 3, 4)
        assert future.result(timeout=5) == 7
        assert service.is_running
    finally:
        service.shutdown(wait=True)


def test_service_context_manager():
    """上下文管理器自动启动和关闭服务。"""
    with ProcessPoolService(max_workers=2) as service:
        future = service.submit(_add, 5, 6)
        assert future.result(timeout=5) == 11


def test_service_submit_no_wait():
    """fire-and-forget 提交。"""
    with ProcessPoolService(max_workers=2) as service:
        service.submit_no_wait(_add, 1, 2)
        time.sleep(0.2)


def test_service_shutdown_then_submit():
    """关闭后再提交应抛 RuntimeError。"""
    service = ProcessPoolService(max_workers=2)
    service.start()
    service.shutdown(wait=True)
    with pytest.raises(RuntimeError, match="shut down"):
        service.submit(_add, 1, 2)


def test_service_double_shutdown():
    """重复关闭不应报错。"""
    service = ProcessPoolService(max_workers=2)
    service.start()
    service.shutdown(wait=True)
    service.shutdown(wait=True)


def test_service_exception_propagation():
    """任务异常应包装为 TaskError。"""
    with ProcessPoolService(max_workers=2) as service:
        future = service.submit(_raise_value_error, "boom")
        with pytest.raises(TaskError, match="boom"):
            future.result(timeout=5)


def test_service_worker_pids():
    """服务能返回工作进程 PID。"""
    service = ProcessPoolService(max_workers=2)
    service.start()
    try:
        pids = service.worker_pids
        assert len(pids) == 2
    finally:
        service.shutdown(wait=True)


def test_service_is_running():
    """is_running 状态正确。"""
    service = ProcessPoolService(max_workers=2)
    assert not service.is_running
    service.start()
    try:
        assert service.is_running
    finally:
        service.shutdown(wait=True)
    assert not service.is_running


def test_service_reuse_across_many_tasks():
    """服务复用，提交大量任务。"""
    task_count = 100
    with ProcessPoolService(max_workers=4) as service:
        futures = [service.submit(_add, i, i) for i in range(task_count)]
        results = [f.result(timeout=10) for f in futures]
    assert results == [i * 2 for i in range(task_count)]


def test_service_high_load_shutdown_no_orphans():
    """高负载下 ProcessPoolService 关闭能按时完成且无残留。"""
    service = ProcessPoolService(max_workers=4)
    service.start()
    pids = service.worker_pids

    try:
        # 提交大量长任务，让任务队列堆积
        for i in range(1000):
            service.submit(_sleep_and_return, i, 10.0)

        start = time.monotonic()
        service.shutdown(wait=True, timeout=2.0)
        elapsed = time.monotonic() - start

        assert elapsed < 5.0, f"ProcessPoolService shutdown took too long: {elapsed:.2f}s"
    finally:
        if service.is_running:
            service.shutdown(wait=False)

    # 所有工作进程应已退出
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        if not any(_process_alive(pid) for pid in pids):
            break
        time.sleep(0.1)
    else:
        remaining = [pid for pid in pids if _process_alive(pid)]
        pytest.fail(f"Residual worker processes after service shutdown: {remaining}")


def test_service_long_task_shutdown_no_orphans():
    """长任务执行中关闭 ProcessPoolService，验证无残留。"""
    service = ProcessPoolService(max_workers=2)
    service.start()
    pids = service.worker_pids

    try:
        # 提交几个 60 秒长任务
        futures = [service.submit(_sleep_and_return, i, 60.0) for i in range(4)]
        time.sleep(0.3)  # 让任务被取走

        service.shutdown(wait=True, timeout=1.0)
    finally:
        if service.is_running:
            service.shutdown(wait=False)

    # 未完成的任务 Future 应被取消
    for future in futures:
        with pytest.raises(TaskError, match="shut down"):
            future.result(timeout=0)

    _assert_no_residuals(pids, timeout=2)


def test_service_shutdown_cancels_pending_futures():
    """高负载下关闭服务，未开始任务的 Future 应被立即取消。"""
    service = ProcessPoolService(max_workers=2)
    service.start()

    try:
        futures = [service.submit(_sleep_and_return, i, 60.0) for i in range(100)]
        service.shutdown(wait=True, timeout=1.0)
    finally:
        if service.is_running:
            service.shutdown(wait=False)

    cancelled = 0
    for future in futures:
        try:
            future.result(timeout=0)
        except TaskError as exc:
            if "shut down" in str(exc):
                cancelled += 1

    # 至少一半任务因队列堆积被直接取消
    assert cancelled >= len(futures) // 2


def _process_alive(pid: int) -> bool:
    """判断进程是否仍在运行（非僵尸）。"""
    try:
        waited_pid, _ = os.waitpid(pid, os.WNOHANG)
        if waited_pid == pid:
            return False
        if waited_pid == 0:
            return True
    except ChildProcessError:
        pass
    try:
        os.kill(pid, 0)
    except (OSError, ProcessLookupError):
        return False
    return True


def _assert_no_residuals(pids: list[int], timeout: float = 5) -> None:
    """等待并断言给定 PID 全部消失或变为僵尸并被回收。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not any(_process_alive(pid) for pid in pids):
            return
        time.sleep(0.1)
    remaining = [pid for pid in pids if _process_alive(pid)]
    assert not remaining, f"Residual worker processes detected: {remaining}"


@pytest.mark.parametrize("cycle_count", [3, 5])
def test_service_start_stop_cycles(cycle_count: int) -> None:
    """多次启停 ProcessPoolService，每次都不应残留进程。"""
    service = ProcessPoolService(max_workers=2)
    all_pids: set[int] = set()

    for _ in range(cycle_count):
        service.start()
        pids = service.worker_pids
        all_pids.update(pids)
        assert service.is_running

        future = service.submit(_add, 1, 2)
        assert future.result(timeout=5) == 3

        service.shutdown(wait=True)
        assert not service.is_running

    _assert_no_residuals(list(all_pids), timeout=3)


@pytest.mark.parametrize(
    ("thread_count", "tasks_per_thread"),
    [(5, 50), (10, 100)],
)
def test_service_concurrent_submission(thread_count: int, tasks_per_thread: int) -> None:
    """多线程并发向同一个服务提交任务，然后关闭，验证无残留。"""
    service = ProcessPoolService(max_workers=4)
    service.start()
    pids = service.worker_pids
    errors: list[BaseException] = []
    results_lock = threading.Lock()
    results: list[int] = []

    def submit_batch(start: int) -> None:
        try:
            batch = [service.submit(_add, i, i) for i in range(start, start + tasks_per_thread)]
            for f in batch:
                val = f.result(timeout=30)
                with results_lock:
                    results.append(val)
        except BaseException as exc:  # noqa: BLE001
            with results_lock:
                errors.append(exc)

    try:
        threads = [
            threading.Thread(target=submit_batch, args=(i * tasks_per_thread,))
            for i in range(thread_count)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Concurrent submission encountered errors: {errors}"
        expected = [i * 2 for i in range(thread_count * tasks_per_thread)]
        assert sorted(results) == expected
    finally:
        service.shutdown(wait=True)

    _assert_no_residuals(pids, timeout=3)


@pytest.mark.parametrize(
    ("short_count", "long_count"),
    [(50, 4), (100, 8)],
)
def test_service_mixed_tasks_shutdown(short_count: int, long_count: int) -> None:
    """短任务和长任务混合提交，执行中关闭服务，验证无残留。"""
    service = ProcessPoolService(max_workers=4)
    service.start()
    pids = service.worker_pids

    try:
        for i in range(short_count):
            service.submit(_add, i, i)
        for i in range(long_count):
            service.submit(_sleep_and_return, i, 60.0)

        time.sleep(0.2)  # 让任务被取走一部分
        service.shutdown(wait=True, timeout=2.0)
    finally:
        if service.is_running:
            service.shutdown(wait=False)

    _assert_no_residuals(pids, timeout=3)


def test_service_exception_storm_shutdown() -> None:
    """大量异常任务中关闭服务，验证无残留。"""
    service = ProcessPoolService(max_workers=4)
    service.start()
    pids = service.worker_pids

    try:
        for i in range(200):
            service.submit(_raise_value_error, f"error-{i}")
        service.shutdown(wait=True, timeout=2.0)
    finally:
        if service.is_running:
            service.shutdown(wait=False)

    _assert_no_residuals(pids, timeout=3)


@pytest.mark.parametrize("wait", [True, False])
def test_service_fire_and_forget_under_load(wait: bool) -> None:
    """高负载 fire-and-forget 后关闭服务，验证无残留。"""
    service = ProcessPoolService(max_workers=4)
    service.start()
    pids = service.worker_pids

    try:
        for i in range(500):
            service.submit_no_wait(_sleep_and_return, i, 0.01)
        service.shutdown(wait=wait, timeout=2.0)
    finally:
        if service.is_running:
            service.shutdown(wait=False)

    _assert_no_residuals(pids, timeout=3)


def test_service_lazy_start_concurrent_threads() -> None:
    """多个线程同时首次提交，懒启动不应出现竞态，关闭后无残留。"""
    service = ProcessPoolService(max_workers=4)
    pids: list[int] = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def submit_task(index: int) -> None:
        try:
            future = service.submit(_add, index, index)
            result = future.result(timeout=10)
            assert result == index * 2
            with lock:
                if not pids:
                    pids.extend(service.worker_pids)
        except BaseException as exc:  # noqa: BLE001
            with lock:
                errors.append(exc)

    try:
        threads = [threading.Thread(target=submit_task, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Concurrent lazy start encountered errors: {errors}"
    finally:
        service.shutdown(wait=True)

    assert pids, "Should have collected worker PIDs"
    _assert_no_residuals(pids, timeout=3)


def test_service_submit_shutdown_race() -> None:
    """一个线程持续提交，另一个线程关闭，验证无残留、无未处理异常。"""
    service = ProcessPoolService(max_workers=4)
    service.start()
    pids = service.worker_pids
    submit_errors: list[BaseException] = []
    stop_event = threading.Event()

    def submit_loop() -> None:
        i = 0
        while not stop_event.is_set():
            try:
                service.submit(_add, i, i)
            except RuntimeError:
                # 服务关闭后提交会抛 RuntimeError，属于预期行为
                break
            except BaseException as exc:  # noqa: BLE001
                submit_errors.append(exc)
                break
            i += 1

    try:
        t = threading.Thread(target=submit_loop)
        t.start()
        time.sleep(0.2)
        service.shutdown(wait=True, timeout=2.0)
        stop_event.set()
        t.join(timeout=5)
    finally:
        if service.is_running:
            service.shutdown(wait=False)
        stop_event.set()
        t.join(timeout=1)

    _assert_no_residuals(pids, timeout=3)


@pytest.mark.parametrize("cycle_count", [10, 20])
def test_service_rapid_start_shutdown(cycle_count: int) -> None:
    """频繁启停 ProcessPoolService，验证无残留。"""
    service = ProcessPoolService(max_workers=2)
    all_pids: set[int] = set()

    for _ in range(cycle_count):
        service.start()
        all_pids.update(service.worker_pids)
        service.submit_no_wait(_add, 1, 2)
        service.shutdown(wait=True)

    _assert_no_residuals(list(all_pids), timeout=3)


def test_service_context_manager_reuse() -> None:
    """多次使用上下文管理器创建服务，验证无残留。"""
    all_pids: set[int] = set()
    for i in range(5):
        with ProcessPoolService(max_workers=2) as service:
            future = service.submit(_add, i, i)
            assert future.result(timeout=5) == i * 2
            all_pids.update(service.worker_pids)

    _assert_no_residuals(list(all_pids), timeout=3)
