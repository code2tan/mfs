"""``InfraStack`` — 基础设施装配与生命周期（阶段 3）。

详见 `docs/engine-redesign.md` §4.2。收口 ``Engine.__init__`` 里 meta / milvus /
embed / converter / vlm / summary / artifact_cache / tx_cache 八个客户端的**构建**，
以及 ``startup`` 的连接段（connect / init_schema / ensure_collection /
preload）与 ``shutdown`` 的关闭段（close）。``_preload_startup_models`` 随之迁入。

设计约束（阶段 3，行为零变化）：

- **Engine 保留普通属性别名**（``self.meta = self.infra.meta`` …，非 property）。多个
  E2E 测试在 ``Engine(cfg)`` 后 ``eng.milvus = _FakeMilvus()`` / ``eng.embed = _FakeEmbed()``
  重赋值再调 ``_build_pipeline()`` 等路径；别名保证可重赋值，``PipelineSupervisor`` 经
  ``self._engine.<client>`` 读取即拿到测试注入的 fake。
- ``InfraStack`` 自持 ``cfg`` + 8 客户端字段，**不持 engine 反向引用**；连接/关闭逻辑
  操作自身字段。生产路径（``__main__.py``）不重赋值客户端，故与别名一致。
"""

from __future__ import annotations

import asyncio

from ...common.converter import ConverterClient
from ...common.embedding import CachingEmbeddingClient
from ...common.summary import CachingSummaryClient
from ...common.vlm import CachingVlmClient
from ...config import ServerConfig
from ...storage.artifact_cache import make_artifact_cache
from ...storage.metadata import make_metadata_store
from ...storage.milvus import MilvusStore
from ...storage.transformation_cache import make_transformation_cache


class InfraStack:
    """八个客户端的构建 + connect/close 生命周期收口。"""

    def __init__(self, cfg: ServerConfig):
        self.cfg = cfg
        self.ns = cfg.namespace
        self.meta = make_metadata_store(cfg)
        self.milvus = MilvusStore(cfg)
        self.artifact_cache = make_artifact_cache(cfg)
        self.tx_cache = make_transformation_cache(cfg)
        self.embed = CachingEmbeddingClient(cfg, self.tx_cache)
        self.converter = ConverterClient(cfg)
        self.vlm = CachingVlmClient(cfg, self.tx_cache)
        self.summary = CachingSummaryClient(cfg, self.tx_cache)

    async def startup(self, *, preload_local_models: bool = False) -> None:
        await self.meta.connect()
        await self.meta.init_schema()
        await self.tx_cache.connect()
        self.milvus.connect()
        self.milvus.ensure_collection(self.ns)
        if preload_local_models:
            await self._preload_startup_models()

    async def _preload_startup_models(self) -> None:
        if not self.embed.should_preload_on_server_start():
            return
        print(
            f"mfs-server: preloading embedding provider "
            f"{self.embed.provider_name}/{self.embed.model}",
            flush=True,
        )
        await asyncio.to_thread(self.embed.preload_provider)
        print("mfs-server: embedding provider ready", flush=True)

    async def shutdown(self) -> None:
        await self.meta.close()
        await self.tx_cache.close()

    async def __aenter__(self) -> "InfraStack":
        await self.startup()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.shutdown()
