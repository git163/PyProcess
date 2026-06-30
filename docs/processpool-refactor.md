# ProcessPool 重构计划（不改变功能）

- 日期: 2026-06-30
- 作者: git163
- 状态: 已批准
- 关联: src/pyprocess/pool.py

## 背景

`ProcessPool.shutdown()` 单个方法约 85 行，串联了「排空任务队列 → 取消 pending future →
发送关闭哨兵 → 优雅 join → 强制 terminate → kill → 清理剩余 future → 恢复信号处理」
等 8 个职责，阅读和维护成本高。`start()` 内联启动了 3 个后台线程，可读性也偏弱。
本次重构在**完全不改变外部功能与行为**的前提下，把这些长方法按职责拆成小的私有辅助方法。

## 目标

- 把超长的 `shutdown()` 拆成多个职责单一的私有辅助方法，主流程一眼可读。
- 把 `start()` 中后台线程的创建抽成一个辅助方法。
- 保持所有公开 API、行为语义、日志（英文）与中文注释风格不变。
- 非目标：不引入新类、不拆分多文件、不调整线程/进程模型、不改性能特性。

## 方案

采用「方法级拆分」：保持 `ProcessPool` 单类不变，仅提取私有辅助方法。
已与用户确认：可调整**未被测试引用**的私有成员；`_workers`、`_shutdown` 必须保留原名。

### 关键设计点

将 `shutdown()` 主体拆为以下私有方法（均不改变调用顺序与时序参数）：

- `_drain_and_cancel_pending() -> None`：在持锁状态下排空任务队列、取消对应 pending future。
- `_send_shutdown_sentinels() -> None`：向任务队列发送 N 个哨兵、向结果队列发送 1 个哨兵。
- `_join_workers(timeout) -> None`：带 deadline 的优雅 join 循环。
- `_terminate_workers(join_timeout) -> None`：terminate 仍存活的 worker 并 join。
- `_kill_workers(join_timeout) -> None`：kill 仍存活的 worker 并 join。
- `_cancel_all_futures() -> None`：给剩余 future 设错误并清空 `_futures`。
- `_restore_signal_handlers() -> None`：恢复 SIGTERM/SIGINT 默认处理。

`shutdown()` 自身只保留：幂等判断 + 置位 `_shutdown` + 按原顺序调用上述方法。

将 `start()` 中三个后台线程的创建抽为：

- `_start_background_threads() -> None`：创建并启动 collector / signal / health 三个守护线程。

`shutdown()` 的下划线内部 timeout 参数（`_terminate_join_timeout` 等）保持签名不变，
因为 `_signal_watcher` 依赖它们传参；仅把方法体内部逻辑下沉到上面的辅助方法。

### 拆分前后对照（行为不变）

```
shutdown(wait, timeout, *, _terminate_join_timeout, _kill_join_timeout, _no_wait_terminate_join_timeout):
    with lock:
        if _shutdown: return
        _shutdown = True
        _drain_and_cancel_pending()
        _send_shutdown_sentinels()
    if wait:
        _join_workers(timeout or DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT)
    _terminate_workers(_terminate_join_timeout if wait else _no_wait_terminate_join_timeout)
    _kill_workers(_kill_join_timeout)
    _cancel_all_futures()
    _restore_signal_handlers()
```

## 影响范围

- 仅修改 `src/pyprocess/pool.py` 一个文件。
- 公开 API、`Future`、`TaskError`、`pool_service.py` 均不改动。
- 无兼容性影响；无性能影响（仅是同一逻辑的方法内联→提取）。

## 风险与对策

- 风险：提取过程中错改时序（join/terminate/kill 的超时分配与顺序）。
  - 对策：严格按现有顺序与超时参数搬运，逐段对照；改完跑全量测试与信号/压力测试。
- 风险：误改被测试引用的私有成员名。
  - 对策：`_workers`、`_shutdown` 保持原名；其余改名前确认测试未引用。

## 实施步骤

- [ ] 提取 `_drain_and_cancel_pending` / `_send_shutdown_sentinels`
- [ ] 提取 `_join_workers` / `_terminate_workers` / `_kill_workers`
- [ ] 提取 `_cancel_all_futures` / `_restore_signal_handlers`
- [ ] 重写 `shutdown()` 主流程为顺序调用
- [ ] 提取 `start()` 中的 `_start_background_threads`
- [ ] 运行全量测试 + 信号/压力测试回归

## 测试计划

- 单元测试：`PYTHONPATH=src python -m pytest tests/ -q`，须保持 87 passed / 5 skipped。
- 重点回归：信号处理（`test_pool_signals.py`）、健康监控、高负载 shutdown 排空
  （`test_pool_stress.py`、`test_pool_service.py`）——这些覆盖了被拆分的 shutdown 逻辑。
