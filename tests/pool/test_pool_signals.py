"""进程池信号处理与残留进程测试。

这些测试验证：当用户通过信号（SIGTERM/SIGKILL）终止主程序时，
进程池创建的工作进程不会残留。
"""

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

_HELPER = Path(__file__).with_name("_signal_helper.py")


def _pid_exists(pid: int) -> bool:
    """判断进程是否仍存在（包括僵尸进程）。"""
    try:
        os.kill(pid, 0)
    except (OSError, ProcessLookupError):
        return False
    return True


def _process_alive(pid: int) -> bool:
    """判断进程是否仍在运行（非僵尸）。

    对当前进程的子进程使用 waitpid 区分运行中/已退出；
    对其他进程回退到 kill(0)。
    """
    try:
        waited_pid, _ = os.waitpid(pid, os.WNOHANG)
        if waited_pid == pid:
            return False
        if waited_pid == 0:
            return True
    except ChildProcessError:
        # 不是当前进程的子进程
        pass
    return _pid_exists(pid)


def _start_helper() -> tuple[subprocess.Popen, list[int]]:
    """启动辅助脚本并解析工作进程 PID。"""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).parents[2] / "src")
    proc = subprocess.Popen(
        [sys.executable, str(_HELPER)],
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


def _assert_no_residuals(pids: list[int], timeout: float = 5) -> None:
    """等待并断言给定 PID 全部消失或变为僵尸并被回收。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not any(_process_alive(pid) for pid in pids):
            return
        time.sleep(0.1)
    remaining = [pid for pid in pids if _process_alive(pid)]
    assert not remaining, f"Residual worker processes detected: {remaining}"


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signal semantics only")
def test_sigterm_no_orphan_workers():
    """SIGTERM 主进程后，所有工作进程应被清理。"""
    proc, pids = _start_helper()
    try:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=10)
        assert proc.returncode is not None
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
    _assert_no_residuals(pids)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signal semantics only")
def test_sigkill_no_orphan_workers():
    """SIGKILL 主进程后，工作进程应通过孤儿检测主动退出。"""
    proc, pids = _start_helper()
    try:
        proc.kill()
        proc.wait(timeout=10)
        assert proc.returncode is not None
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
    # 孤儿检测线程约 0.5s 检查一次，给足够余量
    _assert_no_residuals(pids, timeout=5)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signal semantics only")
def test_normal_shutdown_no_orphans():
    """正常 shutdown 后不应残留工作进程。"""
    from pyprocess.pool import ProcessPool

    pool = ProcessPool(max_workers=2)
    pool.start()
    pids = pool.worker_pids
    pool.shutdown(wait=True)
    _assert_no_residuals(pids, timeout=2)
