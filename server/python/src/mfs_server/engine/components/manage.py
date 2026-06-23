"""``ConnectorManage`` — probe / estimate / inspect / remove（阶段 0 脚手架）。

详见 `docs/engine-redesign.md` §4.10。当前转发回 `Engine` 旧实现；阶段 6 迁入。
`remove_connector` 是写路径里第二复杂的方法（等 worker 退出 → 删 Milvus → 删
artifacts → 删元数据），其"等 `running` job 自然结束 / 心跳 stale 才接管"的不变量
保留为显式 `_await_worker_drained(cid)` 步骤。

方法命名对齐 §4.1 蓝图：``Engine.remove_connector`` → ``admin.remove``。
"""

from __future__ import annotations


class ConnectorManager:
    """阶段 0 脚手架：4 个管理方法转发回 Engine 旧实现。"""

    def __init__(self, engine):
        self._engine = engine

    async def probe(self, *args, **kwargs):
        return await self._engine.probe(*args, **kwargs)

    async def estimate(self, *args, **kwargs):
        return await self._engine.estimate(*args, **kwargs)

    async def inspect(self, *args, **kwargs):
        return await self._engine.inspect(*args, **kwargs)

    async def remove(self, *args, **kwargs):
        # 对应 Engine.remove_connector
        return await self._engine.remove_connector(*args, **kwargs)
