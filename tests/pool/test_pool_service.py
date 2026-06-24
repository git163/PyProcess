"""ProcessPoolService 单元测试。"""

import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

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


def _return_unpicklable() -> object:
    """返回一个不可 pickle 的局部函数对象。"""

    def _local() -> None:
        pass

    return _local


def _exit_task() -> None:
    """任务中主动调用 sys.exit。"""
    import sys

    sys.exit(42)


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


def test_service_wait_method() -> None:
    """ProcessPoolService 的 Future.wait 应在任务完成时返回 True。"""
    with ProcessPoolService(max_workers=2) as service:
        future = service.submit(_sleep_and_return, 42, 0.2)
        assert future.wait(timeout=5)
        assert future.result(timeout=0) == 42


def test_service_wait_method_timeout() -> None:
    """ProcessPoolService 的 Future.wait 超时应返回 False。"""
    with ProcessPoolService(max_workers=2) as service:
        future = service.submit(time.sleep, 5)
        assert not future.wait(timeout=0.1)


def test_service_mass_tasks() -> None:
    """ProcessPoolService 提交大量任务并验证结果正确。"""
    task_count = 1000
    with ProcessPoolService(max_workers=8) as service:
        futures = [service.submit(_add, i, i) for i in range(task_count)]
        results = [f.result(timeout=30) for f in futures]
    assert results == [i * 2 for i in range(task_count)]


def test_service_large_payload() -> None:
    """ProcessPoolService 传输较大任务负载。"""
    size = 2 * 1024 * 1024  # 2MB
    data = list(range(size))

    with ProcessPoolService(max_workers=2) as service:
        future = service.submit(sum, data)
        assert future.result(timeout=30) == sum(data)


def test_service_worker_killed_triggers_shutdown() -> None:
    """工作进程被 kill 后，ProcessPoolService 应触发 shutdown 且不残留。"""
    service = ProcessPoolService(max_workers=2)
    service.start()
    try:
        service.submit(time.sleep, 60)
        time.sleep(0.3)  # 让任务被取走
        pids = service.worker_pids
        assert len(pids) == 2

        os.kill(pids[0], signal.SIGKILL)

        # 等待健康监控触发 shutdown
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if not service.is_running:
                break
            time.sleep(0.05)
        assert not service.is_running, "Health watcher should trigger service shutdown"
    finally:
        if service.is_running:
            service.shutdown(wait=True)

    _assert_no_residuals(pids, timeout=3)


_SERVICE_SIGNAL_HELPER = Path(__file__).with_name("_service_signal_helper.py")


def _start_service_signal_helper() -> tuple[subprocess.Popen, list[int]]:
    """启动 ProcessPoolService 信号测试辅助脚本并解析工作进程 PID。"""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).parents[2] / "src")
    proc = subprocess.Popen(
        [sys.executable, str(_SERVICE_SIGNAL_HELPER)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )

    pids: list[int] = []
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        line = proc.stdout.readline()
        if not line:
            time.sleep(0.05)
            continue
        if line.startswith("WORKERS"):
            pids = [int(x) for x in line.strip().split()[1].split(",")]
            break

    if not pids:
        stderr = proc.stderr.read()
        proc.kill()
        proc.wait(timeout=5)
        raise RuntimeError(f"Failed to collect worker PIDs from helper. stderr: {stderr}")

    return proc, pids


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signal semantics only")
def test_service_sigterm_no_orphan_workers() -> None:
    """SIGTERM ProcessPoolService 主进程后，所有工作进程应被清理。"""
    proc, pids = _start_service_signal_helper()
    try:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=10)
        assert proc.returncode is not None
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
    _assert_no_residuals(pids, timeout=6)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signal semantics only")
def test_service_sigkill_no_orphan_workers() -> None:
    """SIGKILL ProcessPoolService 主进程后，工作进程应通过孤儿检测退出。"""
    proc, pids = _start_service_signal_helper()
    try:
        proc.kill()
        proc.wait(timeout=10)
        assert proc.returncode is not None
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
    _assert_no_residuals(pids, timeout=6)


def test_service_result_timeout() -> None:
    """ProcessPoolService 的 Future.result 超时等待应抛出 TimeoutError。"""
    with ProcessPoolService(max_workers=2) as service:
        future = service.submit(time.sleep, 5)
        with pytest.raises(TimeoutError):
            future.result(timeout=0.1)
        assert not future.done()


def test_service_normal_shutdown_no_orphans() -> None:
    """ProcessPoolService 正常 shutdown 后所有工作进程应已退出。"""
    service = ProcessPoolService(max_workers=3)
    service.start()
    pids = service.worker_pids
    service.shutdown(wait=True)
    _assert_no_residuals(pids, timeout=2)


def test_service_unpicklable_arguments() -> None:
    """ProcessPoolService 提交不可 pickle 的参数时，对应 Future 应超时；服务不被阻塞。"""
    with ProcessPoolService(max_workers=2) as service:
        bad_future = service.submit(_add, (lambda x: x), 1)  # noqa: E731
        with pytest.raises(TimeoutError):
            bad_future.result(timeout=0.5)

        future = service.submit(_add, 2, 3)
        assert future.result(timeout=5) == 5


def test_service_unpicklable_return_value() -> None:
    """ProcessPoolService 任务返回不可 pickle 的结果时，对应 Future 会超时；服务不被阻塞。"""
    with ProcessPoolService(max_workers=2) as service:
        bad_future = service.submit(_return_unpicklable)
        with pytest.raises(TimeoutError):
            bad_future.result(timeout=0.5)

        future = service.submit(_add, 2, 3)
        assert future.result(timeout=5) == 5


def test_service_worker_sys_exit_in_task() -> None:
    """任务中调用 sys.exit 会导致工作进程退出，ProcessPoolService 健康监控触发 shutdown。"""
    service = ProcessPoolService(max_workers=2)
    service.start()
    try:
        bad = service.submit(_exit_task)
        service.submit(_add, 1, 2)

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if not service.is_running:
                break
            time.sleep(0.05)
        assert not service.is_running, (
            "Health watcher should trigger service shutdown after sys.exit"
        )
    finally:
        if service.is_running:
            service.shutdown(wait=True)

    with pytest.raises(TaskError, match="shut down"):
        bad.result(timeout=0)


def test_service_sustained_load() -> None:
    """ProcessPoolService 持续一段时间高负载提交。"""
    batch_size = 50
    total_batches = 20
    with ProcessPoolService(max_workers=8) as service:
        all_results: list[int] = []
        for batch in range(total_batches):
            futures = [service.submit(_add, batch * batch_size + i, i) for i in range(batch_size)]
            batch_results = [f.result(timeout=30) for f in futures]
            all_results.extend(batch_results)

    expected = [
        batch * batch_size + i + i for batch in range(total_batches) for i in range(batch_size)
    ]
    assert sorted(all_results) == sorted(expected)
