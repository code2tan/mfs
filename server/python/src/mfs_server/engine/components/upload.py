"""``UploadService`` — tar / manifest-diff 上传 / Command（阶段 0 脚手架）。

详见 `docs/engine-redesign.md` §4.9。当前转发回 `Engine` 旧实现；阶段 6 迁入
`ingest_upload` / `files_manifest` / `files_upload`，tar 校验抽成 `BundleValidator`
（非 tar / 空 / zip-slip / 链接 全部在此 raise），staging 定位抽成 `StagingLocator`。
两个上传流程的"校验 → 占位 connector → 占 sync slot → 落盘 → drain_job"骨架用
Template Method 共享。
"""

from __future__ import annotations


class UploadService:
    """阶段 0 脚手架：3 个上传方法转发回 Engine 旧实现。"""

    def __init__(self, engine):
        self._engine = engine

    async def ingest_upload(self, *args, **kwargs):
        return await self._engine.ingest_upload(*args, **kwargs)

    async def files_manifest(self, *args, **kwargs):
        return await self._engine.files_manifest(*args, **kwargs)

    async def files_upload(self, *args, **kwargs):
        return await self._engine.files_upload(*args, **kwargs)
