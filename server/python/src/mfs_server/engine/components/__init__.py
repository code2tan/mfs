"""Engine 协作组件脚手架（阶段 0）。

按 `docs/engine-redesign.md` §3.2/§6 拆分的职责单一组件。当前为**空壳**：各组件
持有 `Engine` 反向引用，其公共方法转发回 `Engine` 上的旧实现，**行为零变化**。
后续阶段（1–7）逐步把实现从 `Engine` 迁入组件，`Engine` 退化为纯 Facade（§4.1）。

阶段 0 公共契约基线（grep `eng()` / `eng.` 于 `api/app.py` + `server/__main__.py`
确认，作为 Facade 契约）：

- **18 个方法** — `add` / `cancel_job` / `probe` / `estimate` / `inspect` /
  `remove_connector` / `ingest_upload` / `files_manifest` / `files_upload` /
  `resolve_connector_uri` / `search` / `grep` / `ls` / `cat` / `head` / `tail` /
  `export` / `run_worker_forever`；
- **`meta` 属性** — `/connectors`、`/jobs` 路由直接 `eng().meta.fetchall/fetchone`
  （`api/app.py:614,622,631,640`），lifespan 读 `eng.meta.backend`（`app.py:213`），
  故 `meta` 必须作为 `Engine` 属性保留；
- **生命周期** — `startup` / `shutdown`（`__main__.py` worker 子命令直接构造
  `Engine(cfg)` 并调用）。
"""

from __future__ import annotations

from .manage import ConnectorManager
from .artifact_cache import ArtifactCacheService
from .connector_factory import (
    ConnectorFactory,
    CredentialRedactor,
    CredentialResolver,
    TargetResolution,
    TargetResolver,
)
from .ingest import IngestOrchestrator
from .infra import InfraStack
from .object_repository import ObjectRepository
from .pipeline_supervisor import PipelineSupervisor
from .reads import ReadService
from .upload import UploadService
from .worker import BackoffPolicy, ErrorClass, ErrorClassifier, WorkerScheduler

__all__ = [
    "ArtifactCacheService",
    "BackoffPolicy",
    "ConnectorManager",
    "ConnectorFactory",
    "CredentialRedactor",
    "CredentialResolver",
    "ErrorClass",
    "ErrorClassifier",
    "IngestOrchestrator",
    "InfraStack",
    "ObjectRepository",
    "PipelineSupervisor",
    "ReadService",
    "TargetResolution",
    "TargetResolver",
    "UploadService",
    "WorkerScheduler",
]
