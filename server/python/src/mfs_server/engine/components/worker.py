"""``WorkerScheduler`` — 队列 + 断路器 / Circuit Breaker（阶段 0 脚手架）+ 阶段 1 纯函数。

详见 `docs/engine-redesign.md` §4.7。阶段 1 已把以下**纯逻辑**从 ``Engine`` 迁入本模块：

- ``ErrorClass`` / ``ErrorClassifier`` —— ``Engine._classify_error``。嵌入/provider 错误
  分类：``auth`` / ``quota`` 全局不可重试，``retryable`` 按退避重试。
- ``BackoffPolicy`` —— ``_process_with_retry`` 内的指数退避计算（``min(initial*2**attempt, max)``）。

注：``ErrorClassifier.classify`` 只返回 `auth`/`quota`/`retryable`；`skipped` 是
`_process_with_retry` 在分类之上的另一条出口（源消失等），属 retry 层不属 classify 层。

阶段 4 再迁入 ``run_worker_once`` / ``run_worker_forever`` / ``_claim_*`` /
``_process_with_retry`` / ``_run_job`` / ``_run_job_loop`` / ``_heartbeat_loop`` /
``_should_stop`` / ``_reclaim_stale_jobs``，并抽出 ``CircuitBreaker``。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ErrorClass(str, Enum):
    """嵌入/provider 错误分类。``auth``/``quota`` 全局不可重试；``retryable`` 按退避重试。"""

    AUTH = "auth"
    QUOTA = "quota"
    RETRYABLE = "retryable"


class ErrorClassifier:
    """嵌入/provider 错误分类（原 ``Engine._classify_error``）。

    'auth' / 'quota' 是 GLOBAL 且不可重试：已知坏 key 或空余额对每个对象都同样失败，
    故调用方在首次出现时整 job abort（``embedding_auth_failed`` /
    ``embedding_quota_exceeded``），而非把每个对象磨一遍再伪装成 0-indexed succeeded。
    'retryable' = 瞬态（429 rate-limit / 5xx / timeout）。
    """

    @staticmethod
    def classify(e: Exception) -> ErrorClass:
        """Classify an embedding/provider error:
          'auth'      — bad/unauthorized key (OpenAI 401 / AuthenticationError)
          'quota'     — billing/quota exhausted (insufficient_quota / 402)
          'retryable' — transient (429 rate-limit / 5xx / timeout)
        'auth' and 'quota' are GLOBAL and non-retryable: a known-bad key or empty balance
        fails identically for every object, so the caller aborts the whole job with the
        documented embedding_auth_failed / embedding_quota_exceeded code instead of grinding
        each object (and masking the run as a 0-indexed 'succeeded')."""
        m = str(e).lower()
        nm = type(e).__name__.lower()
        auth_markers = (
            "invalid_api_key",
            "invalid x-api-key",
            "authentication",
            "unauthorized",
            "permission denied",
            "401",
        )
        if any(k in m for k in auth_markers) or "authentication" in nm or "permissiondenied" in nm:
            return ErrorClass.AUTH
        # quota exhausted is distinct from a transient 429 rate-limit (which stays retryable):
        # OpenAI signals it with insufficient_quota; 402 is payment-required.
        if "insufficient_quota" in m or "402" in m:
            return ErrorClass.QUOTA
        return ErrorClass.RETRYABLE


@dataclass(frozen=True)
class BackoffPolicy:
    """指数退避计算（原 ``_process_with_retry`` 内联的 ``min(initial*2**attempt, max)``）。

    封顶于 ``max_ms``：原先一个只睡 initial 的扁平 sleep 完全无视 ``backoff_max_ms``，
    以固定节拍猛撞被 rate-limit 的 provider。
    """

    initial_ms: int
    max_ms: int

    def delay_ms(self, attempt: int) -> int:
        return min(self.initial_ms * (2**attempt), self.max_ms)


class WorkerScheduler:
    """阶段 0 脚手架：公共方法转发回 Engine 旧实现。

    方法命名对齐 §4.1 蓝图：``Engine.run_worker_forever`` → ``worker.run_forever``。
    阶段 1 已迁入 ``ErrorClassifier`` / ``BackoffPolicy`` 纯逻辑；队列调度 / 断路器
    留待阶段 4。
    """

    def __init__(self, engine):
        self._engine = engine

    async def run_forever(self, *args, **kwargs):
        return await self._engine.run_worker_forever(*args, **kwargs)
