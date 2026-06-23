"""``IngestOrchestrator`` — sync job 写编排 / Template Method（阶段 0 脚手架）。

详见 `docs/engine-redesign.md` §4.6。当前转发回 `Engine` 旧实现；阶段 4 迁入
`add` / `_drain_job` / `_index_object` / `_finalize_job` / `cancel_job`，并把
`_index_object` 的 deleted/renamed/pipeline(`deferred`)/metadata-only 四条出口
拆成四个 `IndexHandler`。

**迁移注意**：当 `Engine.add` 改为 ``return await self.ingest.add(...)`` 时，必须
同步把本类的转发体替换为真正实现，否则形成 Engine ↔ IngestOrchestrator 递归。
同理 `cancel_job`。`IngestOrchestrator ↔ WorkerScheduler` 的循环依赖（§4.1 末注）
在阶段 4 用 `bind_worker` 回填解决。
"""

from __future__ import annotations


class IngestOrchestrator:
    """阶段 0 脚手架：公共方法转发回 Engine 旧实现。"""

    def __init__(self, engine):
        self._engine = engine

    async def add(self, *args, **kwargs):
        return await self._engine.add(*args, **kwargs)

    async def cancel_job(self, *args, **kwargs):
        return await self._engine.cancel_job(*args, **kwargs)
