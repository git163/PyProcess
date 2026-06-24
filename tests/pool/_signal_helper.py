"""信号测试辅助脚本：启动进程池并阻塞，便于外部发送信号。"""

import time

from pyprocess.pool import ProcessPool, TaskError


def main() -> None:
    pool = ProcessPool(max_workers=2)
    pool.start()
    pids = pool.worker_pids
    print("WORKERS " + ",".join(str(pid) for pid in pids), flush=True)

    future = pool.submit(time.sleep, 300)
    try:
        future.result(timeout=300)
    except TaskError:
        pass
    finally:
        pool.shutdown(wait=False)


if __name__ == "__main__":
    main()
