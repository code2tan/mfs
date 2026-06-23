"""``ReadService`` — 读路径 / 检索 / Strategy + Chain of Responsibility（阶段 0 脚手架）。

详见 `docs/engine-redesign.md` §4.8。当前转发回 `Engine` 旧实现；阶段 5 把
`cat` 的 locator/structured/binary/text/density 多分支替换为 `CatRouter` 策略表
（first-match），`grep` 的 pushdown/bm25/linear 三分支替换为 `GrepStrategy` 链
（每策略返回是否终态，链在终态停止）。`_density_view` / `_locator_matches` 等纯函数
移到 ``reads/text_views.py``。

`_open_path` / `_match_connector` 属 connector 定位，归 `ConnectorFactory`，读服务
只接收已定位的 `(cid, curi, rel, plugin)`。
"""

from __future__ import annotations


class ReadService:
    """阶段 0 脚手架：8 个读方法转发回 Engine 旧实现。"""

    def __init__(self, engine):
        self._engine = engine

    async def search(self, *args, **kwargs):
        return await self._engine.search(*args, **kwargs)

    async def resolve_connector_uri(self, *args, **kwargs):
        return await self._engine.resolve_connector_uri(*args, **kwargs)

    async def ls(self, *args, **kwargs):
        return await self._engine.ls(*args, **kwargs)

    async def cat(self, *args, **kwargs):
        return await self._engine.cat(*args, **kwargs)

    async def head(self, *args, **kwargs):
        return await self._engine.head(*args, **kwargs)

    async def tail(self, *args, **kwargs):
        return await self._engine.tail(*args, **kwargs)

    async def grep(self, *args, **kwargs):
        return await self._engine.grep(*args, **kwargs)

    async def export(self, *args, **kwargs):
        return await self._engine.export(*args, **kwargs)
