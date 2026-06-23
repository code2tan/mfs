"""``PipelineSupervisor`` — 进程单例 + Observer（阶段 0 脚手架）。

详见 `docs/engine-redesign.md` §4.5。per-`Engine` 实例的惰性单例（**非模块全局**），
收口 ``_chunks_q`` / ``_embed_consumer`` / ``_producer_ctx`` / ``_job_lane`` /
``_job_watcher`` / 两个并发 gate / ``_pending_finalize``，以及 startup 时的
``_gc_orphan_chunks`` + ``_recover_job_lane``。

核心不变量（§5）：一个 chunk 在 Milvus 中存在 ⇔ 一条已提交的 `objects` 行指向它
（`dir_summary` 是刻意的例外）。``_on_pipeline_object_indexed`` 是不可跨组件分割的
原子方法体（claim→`won`-check→delete-or-write），阶段 3 迁入时方法体必须整体保留。

阶段 0：`Engine` 仍直接持有上述可变字段并在 `_build_pipeline` 里惰性装配；本类仅
占位，不持有状态。
"""

from __future__ import annotations


class PipelineSupervisor:
    """阶段 0 空壳。阶段 3 收口进程单例字段与 `_pending_finalize`（三处 pop 一起迁）。"""

    def __init__(self, engine):
        self._engine = engine
