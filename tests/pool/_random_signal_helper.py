"""随机信号压力测试辅助脚本。

启动进程池并持续提交长任务，打印工作进程 PID 后阻塞，便于外部测试
在任意时刻发送信号并验证无残留进程。
"""

import random
import sys
import time

from pyprocess.pool import ProcessPool, TaskError


def _long_task(seed: int) -> int:
    """执行一段随机耗时的 CPU 计算，模拟长任务。"""
    duration = 0.1 + (seed % 50) / 100.0  # 0.1s ~ 0.6s
    deadline = time.monotonic() + duration
    total = 0
    while time.monotonic() < deadline:
        total += seed
        seed = (seed * 1103515245 + 12345) & 0x7FFFFFFF
    return total


def main() -> None:
    max_workers = int(sys.argv[1]) if len(sys.argv) > 1 else 4
    pool = ProcessPool(max_workers=max_workers)
    pool.start()
    pids = pool.worker_pids
    print("WORKERS " + ",".join(str(pid) for pid in pids), flush=True)

    # 持续提交一批长任务，让工作者保持忙碌
    random.seed()
    futures = [pool.submit(_long_task, random.randint(0, 10000)) for _ in range(max_workers * 4)]

    try:
        for future in futures:
            future.result(timeout=300)
    except TaskError:
        pass
    finally:
        pool.shutdown(wait=False)


if __name__ == "__main__":
    main()
