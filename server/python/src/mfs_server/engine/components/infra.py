"""``InfraStack`` — 基础设施装配与生命周期（阶段 0 脚手架）。

详见 `docs/engine-redesign.md` §4.2。当前 `Engine` 仍在 `__init__` 里直接构建
meta/milvus/embed/converter/vlm/summary/artifact_cache/tx_cache 八个客户端，本类
仅持有 Engine 反向引用，不持有任何状态。阶段 3 把这八个客户端的构建 +
`startup`/`shutdown` 连接收口到此处，并提供 ``async with`` 生命周期。
"""

from __future__ import annotations


class InfraStack:
    """阶段 0 空壳：属性转发回 Engine，与 Engine 现有客户端同源。"""

    def __init__(self, engine):
        self._engine = engine

    @property
    def cfg(self):
        return self._engine.cfg

    @property
    def meta(self):
        return self._engine.meta

    @property
    def milvus(self):
        return self._engine.milvus

    @property
    def embed(self):
        return self._engine.embed

    @property
    def converter(self):
        return self._engine.converter

    @property
    def vlm(self):
        return self._engine.vlm

    @property
    def summary(self):
        return self._engine.summary

    @property
    def artifact_cache(self):
        return self._engine.artifact_cache

    @property
    def tx_cache(self):
        return self._engine.tx_cache
