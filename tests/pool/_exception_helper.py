"""异常退出测试辅助脚本：手动 start 后抛未捕获异常，且不调用 shutdown。

模拟用户未使用 with / try-finally 的场景，用于验证 atexit 兜底能在主进程
异常退出时清理 worker，避免进程挂起与残留。
"""

import time

from pyprocess.pool_service import ProcessPoolService


def main() -> None:
    service = ProcessPoolService(max_workers=2)
    service.start()
    pids = service.worker_pids
    print("WORKERS " + ",".join(str(pid) for pid in pids), flush=True)

    # 提交一个长任务占住一个 worker，另一个 worker 空闲阻塞在 task_queue.get()。
    # 随后抛出未捕获异常，且不调用 shutdown、不使用 with：
    # 若无 atexit 兜底，非守护 worker 会令 multiprocessing 退出 join 永久挂起。
    service.submit(time.sleep, 300)
    time.sleep(0.2)
    raise RuntimeError("uncaught error in main without shutdown")


if __name__ == "__main__":
    main()
