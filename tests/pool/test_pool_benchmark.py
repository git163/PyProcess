"""进程池性能基准测试。

对比单进程与 4/8/16 个工作进程在执行 CPU 密集型任务时的加速比。

默认不执行，需设置环境变量 RUN_BENCHMARK=1：
    RUN_BENCHMARK=1 .venv/bin/python -m pytest tests/pool/test_pool_benchmark.py -v -s
"""

import os
import time

import pytest

from pyprocess.pool import ProcessPool

_SKIP_BENCHMARK = pytest.mark.skipif(
    os.environ.get("RUN_BENCHMARK") != "1",
    reason="benchmark tests only run when RUN_BENCHMARK=1",
)


def _count_primes(upper: int) -> int:
    """统计 [2, upper) 范围内的素数个数，CPU 密集型。"""
    count = 0
    for i in range(2, upper):
        is_prime = True
        for j in range(2, int(i**0.5) + 1):
            if i % j == 0:
                is_prime = False
                break
        if is_prime:
            count += 1
    return count


@_SKIP_BENCHMARK
@pytest.mark.benchmark
@pytest.mark.parametrize("worker_count", [1, 4, 8, 16])
def test_worker_scaling(worker_count: int) -> None:
    """测试不同工作进程数量下的纯任务执行时间（不含进程启动耗时）。"""
    task_count = 8
    upper = 400000

    if worker_count == 1:
        # 单进程串行执行
        start = time.perf_counter()
        results = [_count_primes(upper) for _ in range(task_count)]
        elapsed = time.perf_counter() - start
    else:
        # 先启动并预热池子，不计入任务执行时间
        pool = ProcessPool(max_workers=worker_count)
        pool.start()
        warmup = pool.submit(_count_primes, upper)
        warmup.result(timeout=30)

        start = time.perf_counter()
        futures = [pool.submit(_count_primes, upper) for _ in range(task_count)]
        results = [f.result(timeout=120) for f in futures]
        elapsed = time.perf_counter() - start

        pool.shutdown(wait=True)

    assert len(results) == task_count
    assert all(r == results[0] for r in results)
    print(f"\nworkers={worker_count:2d}, tasks={task_count}, elapsed={elapsed:.3f}s")


@_SKIP_BENCHMARK
@pytest.mark.benchmark
def test_speedup_summary() -> None:
    """汇总单进程与多进程的纯任务执行时间并输出加速比（不含进程启动耗时）。"""
    task_count = 8
    upper = 400000

    # 单进程基线
    start = time.perf_counter()
    single_results = [_count_primes(upper) for _ in range(task_count)]
    single_time = time.perf_counter() - start

    expected = single_results[0]
    speedups: dict[int, float] = {}

    for worker_count in [4, 8, 16]:
        # 先启动并预热池子
        pool = ProcessPool(max_workers=worker_count)
        pool.start()
        warmup = pool.submit(_count_primes, upper)
        warmup.result(timeout=30)

        start = time.perf_counter()
        futures = [pool.submit(_count_primes, upper) for _ in range(task_count)]
        results = [f.result(timeout=120) for f in futures]
        elapsed = time.perf_counter() - start

        pool.shutdown(wait=True)

        assert all(r == expected for r in results)
        speedups[worker_count] = single_time / elapsed
        print(
            f"\nworkers={worker_count:2d}, elapsed={elapsed:.3f}s, "
            f"speedup={speedups[worker_count]:.2f}x"
        )

    print(f"\nBaseline (single process): {single_time:.3f}s")
    print("Speedup summary:")
    for worker_count, speedup in speedups.items():
        print(f"  {worker_count:2d} workers: {speedup:.2f}x")

    # 加速比应至少大于 1，避免在超线程/重载机器上误判
    assert speedups[4] > 1.0, "4 workers should be faster than single process"
