"""``ArtifactCacheService`` — artifact 缓存 LRU + freshness（阶段 0 脚手架）。

详见 `docs/engine-redesign.md` §4.1 表。当前 `Engine` 持有 ``_put_artifact`` /
``_drop_artifacts`` / ``_read_artifact`` / ``_read_artifact_fresh`` /
``_converted_md_stale`` / ``_evict_artifacts_if_needed``。阶段 6 上移到本服务。
底层 ``storage.artifact_cache`` 已存在，保持不变。
"""

from __future__ import annotations


class ArtifactCacheService:
    """阶段 0 空壳：转发回 Engine 上的 artifact 缓存方法。"""

    def __init__(self, engine):
        self._engine = engine
