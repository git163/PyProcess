"""pyprocess.pool 常用用法演示。

运行方式：
    PYTHONPATH=src python examples/demo_pool.py

涵盖以下场景：
1. 基础 submit / result
2. 批量提交任务
3. fire-and-forget（不关心结果）
4. 共享内存传递大数据
5. 异常处理
6. 超时控制
7. 主动关闭与资源释放
"""

from __future__ import annotations

import logging
import multiprocessing as mp
import struct
import time
from array import array
from multiprocessing import shared_memory

from pyprocess.pool import ProcessPool, TaskError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)


def heavy_compute(start: int, end: int) -> int:
    """模拟 CPU 密集型计算：计算 [start, end) 范围内所有整数平方和。"""
    total = 0
    for i in range(start, end):
        total += i * i
    return total


def may_fail(value: int) -> int:
    """模拟可能失败的任务。"""
    if value % 7 == 0:
        raise ValueError(f"unlucky number: {value}")
    return value * value


def send_notification(user_id: int) -> None:
    """模拟发送通知，fire-and-forget 场景。"""
    time.sleep(0.01)
    logger.info("Notification sent to user %s", user_id)


def worker_with_shared_memory(shm_name: str, count: int) -> float:
    """在子进程中 attach 共享内存并计算双精度浮点数之和。"""
    shm = shared_memory.SharedMemory(name=shm_name)
    try:
        buf = shm.buf[: count * 8]
        try:
            numbers = struct.unpack(f"{count}d", buf)
            return float(sum(numbers))
        finally:
            # 必须先释放 memoryview，否则 close() 会报 BufferError
            buf.release()
    finally:
        shm.close()


def demo_basic_submit() -> None:
    """演示 1：基础 submit / result。"""
    logger.info("=== Demo 1: basic submit / result ===")
    with ProcessPool(max_workers=4) as pool:
        future = pool.submit(heavy_compute, 1, 100_000)
        result = future.result(timeout=5)
        logger.info("Result: %s", result)


def demo_batch_submit() -> None:
    """演示 2：批量提交任务。"""
    logger.info("=== Demo 2: batch submit ===")
    with ProcessPool(max_workers=4) as pool:
        ranges = [(i * 10_000, (i + 1) * 10_000) for i in range(8)]
        futures = [pool.submit(heavy_compute, start, end) for start, end in ranges]
        results = [f.result(timeout=10) for f in futures]
        logger.info("Results: %s", results)


def demo_fire_and_forget() -> None:
    """演示 3：fire-and-forget，不关心返回值。"""
    logger.info("=== Demo 3: fire-and-forget ===")
    with ProcessPool(max_workers=4) as pool:
        for user_id in range(20):
            pool.submit_no_wait(send_notification, user_id)
        # 给任务一点时间执行
        time.sleep(0.5)
    logger.info("All notifications submitted")


def demo_shared_memory() -> None:
    """演示 4：通过共享内存传递大数据。"""
    logger.info("=== Demo 4: shared memory ===")
    count = 1_000_000
    data = array("d", (float(i) for i in range(count)))
    size = data.buffer_info()[1] * data.itemsize

    shm = shared_memory.SharedMemory(create=True, size=size)
    try:
        shm.buf[:size] = data.tobytes()

        with ProcessPool(max_workers=4) as pool:
            future = pool.submit(worker_with_shared_memory, shm.name, count)
            result = future.result(timeout=10)
            expected = sum(range(count))
            logger.info("Sum from shared memory: %s (expected: %s)", result, expected)
    finally:
        shm.close()
        shm.unlink()


def demo_exception_handling() -> None:
    """演示 5：异常处理。"""
    logger.info("=== Demo 5: exception handling ===")
    with ProcessPool(max_workers=4) as pool:
        futures = [pool.submit(may_fail, i) for i in range(1, 15)]
        for i, future in enumerate(futures, start=1):
            try:
                result = future.result(timeout=5)
                logger.info("Task %s succeeded: %s", i, result)
            except TaskError as exc:
                logger.warning("Task %s failed: %s", i, exc)


def demo_timeout() -> None:
    """演示 6：超时控制。"""
    logger.info("=== Demo 6: timeout ===")
    with ProcessPool(max_workers=2) as pool:
        future = pool.submit(time.sleep, 60)
        try:
            future.result(timeout=0.5)
        except TimeoutError:
            logger.info("Task did not finish within timeout")
        # 强制结束进程池，未完成的任务会被取消
        pool.shutdown(wait=False)


def demo_manual_lifecycle() -> None:
    """演示 7：手动管理进程池生命周期。"""
    logger.info("=== Demo 7: manual lifecycle ===")
    pool = ProcessPool(max_workers=2)
    pool.start()
    try:
        future = pool.submit(heavy_compute, 1, 50_000)
        logger.info("Manual pool result: %s", future.result(timeout=5))
    finally:
        pool.shutdown(wait=True)


def main() -> None:
    # 使用 spawn 上下文与进程池保持一致
    mp.set_start_method("spawn", force=True)

    demo_basic_submit()
    demo_batch_submit()
    demo_fire_and_forget()
    demo_shared_memory()
    demo_exception_handling()
    demo_timeout()
    demo_manual_lifecycle()

    logger.info("All demos finished")


if __name__ == "__main__":
    main()
