"""随机信号压力测试辅助脚本。

启动进程池并持续提交任务，打印工作进程 PID 后阻塞，便于外部测试
在任意时刻发送信号并验证无残留进程。
"""

import random
import sys
import time

from pyprocess.pool import ProcessPool, TaskError


def _configurable_task(seed: int, duration: float) -> int:
    """执行一段指定耗时的任务，模拟长任务或中长任务。"""
    deadline = time.monotonic() + duration
    total = 0
    while time.monotonic() < deadline:
        total += seed
        seed = (seed * 1103515245 + 12345) & 0x7FFFFFFF
    return total


def main() -> None:
    max_workers = int(sys.argv[1]) if len(sys.argv) > 1 else 4
    duration = float(sys.argv[2]) if len(sys.argv) > 2 else 0.5
    multiplier = int(sys.argv[3]) if len(sys.argv) > 3 else 4

    pool = ProcessPool(max_workers=max_workers)
    pool.start()
    pids = pool.worker_pids
    print("WORKERS " + ",".join(str(pid) for pid in pids), flush=True)

    # 持续提交一批任务，让工作者保持忙碌
    random.seed()
    futures = [
        pool.submit(_configurable_task, random.randint(0, 10000), duration)
        for _ in range(max_workers * multiplier)
    ]

    try:
        for future in futures:
            future.result(timeout=300)
    except TaskError:
        pass
    finally:
        pool.shutdown(wait=False)


if __name__ == "__main__":
    main()
