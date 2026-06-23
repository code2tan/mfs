"""``WorkerScheduler`` — 队列 + 断路器 / Circuit Breaker（阶段 0 脚手架）。

详见 `docs/engine-redesign.md` §4.7。当前转发回 `Engine` 旧实现；阶段 4 迁入
`run_worker_once` / `run_worker_forever` / `_claim_*` / `_process_with_retry` /
`_run_job` / `_run_job_loop` / `_heartbeat_loop` / `_should_stop` /
`_reclaim_stale_jobs`，并抽出 `ErrorClassifier` / `CircuitBreaker` /
`BackoffPolicy` 值对象。

注：``ErrorClassifier.classify`` 只返回 `auth`/`quota`/`retryable`；`skipped` 是
`_process_with_retry` 在分类之上的另一条出口（源消失等），属 retry 层不属 classify 层。
"""

from __future__ import annotations


class WorkerScheduler:
    """阶段 0 脚手架：公共方法转发回 Engine 旧实现。

    方法命名对齐 §4.1 蓝图：``Engine.run_worker_forever`` → ``worker.run_forever``。
    """

    def __init__(self, engine):
        self._engine = engine

    async def run_forever(self, *args, **kwargs):
        return await self._engine.run_worker_forever(*args, **kwargs)
