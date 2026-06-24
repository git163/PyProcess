"""ProcessPoolService 信号测试辅助脚本。

启动 ProcessPoolService 并阻塞，便于外部发送信号并验证无残留进程。
"""

import time

from pyprocess.pool import TaskError
from pyprocess.pool_service import ProcessPoolService


def main() -> None:
    service = ProcessPoolService(max_workers=2)
    service.start()
    pids = service.worker_pids
    print("WORKERS " + ",".join(str(pid) for pid in pids), flush=True)

    future = service.submit(time.sleep, 300)
    try:
        future.result(timeout=300)
    except TaskError:
        pass
    finally:
        service.shutdown(wait=False)


if __name__ == "__main__":
    main()
