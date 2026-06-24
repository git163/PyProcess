# pyprocess 进程池使用指南

`pyprocess.pool` 提供了一个轻量级的异步进程池，适用于需要把 CPU 密集型或独立任务分发到多个进程执行的场景。

## 快速开始

### 上下文管理器（推荐）

```python
from pyprocess.pool import ProcessPool


def add(a: int, b: int) -> int:
    return a + b


with ProcessPool(max_workers=4) as pool:
    future = pool.submit(add, 1, 2)
    result = future.result(timeout=5)
    print(result)  # 3
```

上下文管理器会自动调用 `start()` 和 `shutdown(wait=True)`，确保进程池被正确关闭。

## 核心概念

### ProcessPool

`ProcessPool` 是进程池入口，负责管理工作进程、任务队列和结果队列。

```python
from pyprocess.pool import ProcessPool

# max_workers 默认为 CPU 核心数
pool = ProcessPool(max_workers=4)
pool.start()

try:
    future = pool.submit(sum, [1, 2, 3, 4])
    print(future.result(timeout=5))
finally:
    pool.shutdown(wait=True)
```

### Future

`submit()` 返回 `Future` 对象，用于等待和获取任务结果。

```python
future = pool.submit(pow, 2, 10)

# 等待任务完成，返回 bool
if future.wait(timeout=2):
    print("任务已完成")

# 获取结果，超时报 TimeoutError，任务抛异常时报 TaskError
result = future.result(timeout=5)

# 查询状态
print(future.done())
```

### 异常处理

任务中抛出的异常会被包装为 `TaskError`。

```python
from pyprocess.pool import ProcessPool, TaskError


def may_raise(value: int) -> int:
    if value < 0:
        raise ValueError("value must be non-negative")
    return value


with ProcessPool(max_workers=2) as pool:
    future = pool.submit(may_raise, -1)
    try:
        future.result(timeout=5)
    except TaskError as exc:
        print(f"任务失败: {exc}")
        print(f"原始异常: {exc.cause}")  # RuntimeError: ValueError: value must be non-negative
```

## fire-and-forget 提交

如果你只关心任务被异步执行，不关心返回值，可以使用 `submit_no_wait()`：

```python
def send_email(user_id: int) -> None:
    ...


with ProcessPool(max_workers=4) as pool:
    for user_id in user_ids:
        pool.submit_no_wait(send_email, user_id)
```

### 注意事项

- `submit_no_wait()` 不返回 `Future`，调用方无法直接获取结果或异常。
- 任务抛出的异常会被内部 `Future` 捕获，但不会被调用方感知；如果你需要错误处理，请使用 `submit()`。
- 任务完成后，内部状态会被结果收集线程自动清理，不会因忽略返回值而泄漏内存。

## 批量提交任务

```python
def process(item: int) -> int:
    return item * item


with ProcessPool(max_workers=4) as pool:
    futures = [pool.submit(process, i) for i in range(10)]
    results = [f.result(timeout=10) for f in futures]
    print(results)
```

## 超时控制

```python
import time

with ProcessPool(max_workers=2) as pool:
    future = pool.submit(time.sleep, 60)

    # 只等 0.5 秒，未完成则抛 TimeoutError
    try:
        future.result(timeout=0.5)
    except TimeoutError:
        print("任务还没完成")

    # 强制结束进程池，未完成的任务会被取消
    pool.shutdown(wait=False)
```

## 优雅关闭与强制终止

```python
pool = ProcessPool(max_workers=4)
pool.start()

# 发送关闭哨兵，等待工作者自然退出
pool.shutdown(wait=True)

# 或者不等，直接 terminate / kill
pool.shutdown(wait=False)
```

关闭流程：
1. 向任务队列发送关闭哨兵。
2. 等待工作者优雅退出（默认 5 秒，可通过 `timeout` 参数调整）。
3. 仍有存活进程则调用 `terminate()`。
4. 仍然存活则调用 `kill()`。

## 信号处理与不残留子进程

进程池启动后会注册 `SIGTERM` 和 `SIGINT` 处理函数：

- `SIGTERM`（15）：触发**优雅关闭**（`shutdown(wait=True, timeout=5.0)`），给正在执行的任务最多 5 秒完成当前任务后退出；超时仍未退出则强制终止。
- `SIGINT`（2，即 Ctrl+C）：触发**快速关闭**（`shutdown(wait=False)`），立即发送关闭哨兵并强制清理工作进程。
- `SIGKILL`（9）：无法被捕获，由工作进程内部的**孤儿检测线程**处理。当工作进程发现父进程 PID 发生变化（例如被 init 收养），会主动退出，避免残留。

```python
import time

with ProcessPool(max_workers=4) as pool:
    pool.submit(time.sleep, 300)
    # 按 Ctrl-C 快速退出；发送 SIGTERM 则优先优雅退出
```

## 可调参数

`src/pyprocess/pool.py` 顶部定义了若干常量，可按需调整：

| 常量 | 默认值 | 含义 |
|------|--------|------|
| `DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT` | 5.0 | 优雅关闭最长等待时间（秒） |
| `DEFAULT_TERMINATE_JOIN_TIMEOUT` | 1.0 | `terminate` 后等待时间（秒） |
| `DEFAULT_KILL_JOIN_TIMEOUT` | 1.0 | `kill` 后等待时间（秒） |
| `DEFAULT_WORKER_POLL_INTERVAL` | 0.5 | 孤儿检测/健康监控轮询间隔（秒） |
| `DEFAULT_RESULT_QUEUE_TIMEOUT` | 0.2 | 结果收集线程队列取数超时（秒） |
| `SIGNAL_GRACEFUL_TIMEOUT` | 4.0 | `SIGTERM` 触发时优雅等待时间（秒），保证总清理约 5 秒 |
| `SIGNAL_TERMINATE_JOIN_TIMEOUT` | 0.5 | 信号触发时 `terminate` 后等待时间（秒） |
| `SIGNAL_KILL_JOIN_TIMEOUT` | 0.5 | 信号触发时 `kill` 后等待时间（秒） |

## 使用限制

1. **任务函数和参数必须可 pickle**：进程池使用 `spawn` 上下文，所有传入 `submit()` 的函数、参数、返回值都需要能被 pickle 序列化。
2. **任务函数建议在模块级别定义**：局部函数、lambda、闭包等通常无法被 pickle。
3. **返回 `None` 是允许的**：`Future.result()` 会正常返回 `None`。

## 完整示例

```python
import time
from pyprocess.pool import ProcessPool, TaskError


def heavy_task(n: int) -> int:
    """模拟耗时计算。"""
    total = 0
    for i in range(n):
        total += i
    time.sleep(0.1)
    return total


def main() -> None:
    numbers = [100000, 200000, 300000, 400000]

    with ProcessPool(max_workers=4) as pool:
        futures = [pool.submit(heavy_task, n) for n in numbers]

        for n, future in zip(numbers, futures):
            try:
                result = future.result(timeout=10)
                print(f"heavy_task({n}) = {result}")
            except TaskError as exc:
                print(f"heavy_task({n}) 失败: {exc}")


if __name__ == "__main__":
    main()
```

## API 概览

| 方法/属性 | 说明 |
|-----------|------|
| `ProcessPool(max_workers=...)` | 创建进程池，`max_workers` 默认为 CPU 核心数 |
| `pool.start()` | 显式启动工作进程（上下文管理器会自动调用） |
| `pool.submit(func, *args, **kwargs)` | 提交任务，返回 `Future` |
| `pool.submit_no_wait(func, *args, **kwargs)` | 提交任务，不返回 `Future` |
| `pool.shutdown(wait=True, timeout=None)` | 关闭进程池 |
| `pool.worker_pids` | 获取当前工作进程 PID 列表 |
| `future.result(timeout=None)` | 等待并返回结果 |
| `future.wait(timeout=None)` | 等待任务完成，返回 `bool` |
| `future.done()` | 查询任务是否已完成 |

## submit 与 submit_no_wait 对比

| 特性 | `submit()` | `submit_no_wait()` |
|------|-----------|-------------------|
| 返回值 | `Future` | `None` |
| 获取结果 | ✅ 支持 | ❌ 不支持 |
| 感知异常 | ✅ 通过 `.result()` 抛出 `TaskError` | ❌ 静默处理 |
| 适用场景 | 需要结果或错误处理 | 纯异步触发，如日志、通知 |

## 获取工作进程 PID

```python
pool = ProcessPool(max_workers=4)
pool.start()
print(pool.worker_pids)  # [12345, 12346, 12347, 12348]
pool.shutdown(wait=True)
```

## 自定义关闭等待时间

```python
# 给长任务更多退出时间
pool.shutdown(wait=True, timeout=30.0)

# 立即强制关闭
pool.shutdown(wait=False)
```

## 适用场景建议

**适合使用进程池：**
- CPU 密集型计算（图像处理、数值计算、数据转换）
- 需要隔离内存、避免 GIL 限制的任务
- 可能崩溃但不能影响主进程稳定性的任务

**不建议使用进程池：**
- 主要是 IO 等待的任务（网络请求、文件读写）：考虑线程池或 asyncio
- 任务极小但数量极大：进程间通信开销可能抵消收益
- 函数/参数不可 pickle：需要改用线程或在提交前序列化

## 运行基准测试

项目提供了性能基准测试，用于对比不同工作进程数量下的加速比：

```bash
./run_benchmark.sh
```

默认跳过，脚本会自动设置 `RUN_BENCHMARK=1` 并执行相关测试。
