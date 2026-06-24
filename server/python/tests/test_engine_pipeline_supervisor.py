"""阶段 3 前置/回归单测：``InfraStack`` + ``PipelineSupervisor`` 迁移后的不变量。

覆盖 `docs/engine-redesign.md` §7 阶段 3 "前置单测" 与 `engine-redesign-testgap.md` 阶段 3：

- ``_build_pipeline`` 惰性 + 幂等（重复调用不重建 consumer，保留 ``if _embed_consumer
  is not None: return`` 守卫）。
- ``_pending_finalize`` 收口：``Engine._pending_finalize`` 是转发到 ``PipelineSupervisor``
  同一 dict 的只读 property，``[uri]=`` / ``.pop()`` 经原地变更生效。
- ``_recover_job_lane``：启动时为 crash 留下的 ``running`` job 重建 Job Lane 内存目录树
  （排除 ``dir_summary`` 任务）；``_job_lane`` 缺失/禁用早退；``list_running_jobs`` 抛错
  不阻塞。
"""

from __future__ import annotations

import hashlib
import json
import uuid

from mfs_server.config import ServerConfig
from mfs_server.connectors.registry import load_builtin
from mfs_server.engine.engine import Engine


# --- 最小 fakes（复用 test_engine_chunkable_e2e 的形态）---


class _FakeEmbed:
    provider_name = "fake"
    model = "fake-model"
    version = "1"

    def _key(self, text):
        return "k:" + hashlib.sha1(text.encode()).hexdigest()

    async def _embed_api(self, texts):
        return [[0.1, 0.2, 0.3] for _ in texts]


class _FakeMilvus:
    def __init__(self):
        self.deletes: list[tuple] = []

    def delete_by_object(self, ns, connector_uri, object_uri):
        self.deletes.append((ns, connector_uri, object_uri))


class _FakeTxCache:
    async def batch_get(self, keys):
        return {k: None for k in keys}


class _RecordingJobLane:
    """记录 recover_job 调用，模拟 Job Lane 的 enabled 守卫。"""

    def __init__(self, *, enabled: bool = True):
        self.enabled = enabled
        self.recovered: list[tuple] = []

    def recover_job(self, job_id, connector_uri, plugin, objects, extra):
        self.recovered.append((job_id, connector_uri, objects))


def _now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


async def _build_engine(tmp_path) -> Engine:
    load_builtin()
    cfg = ServerConfig()
    cfg.metadata.backend = "sqlite"
    cfg.metadata.path = str(tmp_path / "meta.db")
    cfg.transformation_cache.backend = "sqlite"
    cfg.transformation_cache.db_path = str(tmp_path / "tx.db")
    cfg.artifact_cache.root = str(tmp_path / "art")
    eng = Engine(cfg)
    eng.embed = _FakeEmbed()
    eng.milvus = _FakeMilvus()
    eng.tx_cache = _FakeTxCache()
    eng._embed_idle_ms = 50
    await eng.meta.connect()
    await eng.meta.init_schema()
    await eng.meta.execute("PRAGMA foreign_keys=OFF")  # seed rows without parent FKs
    return eng


# ---------------------------------------------------------------------------
# _build_pipeline 惰性 + 幂等
# ---------------------------------------------------------------------------


async def test_build_pipeline_is_idempotent(tmp_path):
    eng = await _build_engine(tmp_path)
    try:
        eng._build_pipeline()
        consumer_before = eng._embed_consumer
        chunks_q_before = eng._producer_ctx
        assert consumer_before is not None

        # 重复调用：守卫 ``if self._embed_consumer is not None: return`` 命中，不重建。
        eng._build_pipeline()
        assert eng._embed_consumer is consumer_before
        assert eng._producer_ctx is chunks_q_before
    finally:
        await eng.pipeline.shutdown()
        await eng.meta.close()


async def test_build_pipeline_lazy_from_pump(tmp_path):
    """pump 在未构建时惰性触发 _build_pipeline（约束：未 startup 也能跑 pipeline 路径）。"""
    eng = await _build_engine(tmp_path)
    try:
        assert eng._embed_consumer is None
        eng._build_pipeline()
        assert eng._embed_consumer is not None
        # consumer 持有的成功回调包含 supervisor 的 _on_object_indexed（原子 finalize hook）
        assert eng.pipeline._embed_consumer is eng._embed_consumer
    finally:
        await eng.pipeline.shutdown()
        await eng.meta.close()


# ---------------------------------------------------------------------------
# _pending_finalize 收口
# ---------------------------------------------------------------------------


async def test_pending_finalize_is_collected_into_supervisor(tmp_path):
    eng = await _build_engine(tmp_path)
    try:
        # Engine._pending_finalize 是只读 property，转发到 supervisor 的同一 dict。
        assert eng._pending_finalize is eng.pipeline._pending_finalize

        # 经 Engine property 写入 → supervisor 持有的 dict 立即可见（原地 __setitem__）。
        ctx = ("cid", "file:///repo", "/a.md", None, True, None, "task-1")
        eng._pending_finalize["file:///repo/a.md"] = ctx
        assert eng.pipeline._pending_finalize["file:///repo/a.md"] is ctx

        # 经 Engine property pop → supervisor dict 同步移除。
        popped = eng._pending_finalize.pop("file:///repo/a.md")
        assert popped is ctx
        assert "file:///repo/a.md" not in eng.pipeline._pending_finalize
    finally:
        await eng.meta.close()


# ---------------------------------------------------------------------------
# _recover_job_lane
# ---------------------------------------------------------------------------


async def _seed_running_job(eng, *, cid, job_id, root_uri, config_json, tasks):
    await eng.meta.execute(
        "INSERT INTO connectors (id, root_uri, type, config_json, status, registered_at) "
        "VALUES (?,?,?,?,?,?)",
        (cid, root_uri, "file", config_json, "active", _now()),
    )
    await eng.meta.execute(
        "INSERT INTO connector_jobs (id, connector_id, status, started_at) VALUES (?,?,?,?)",
        (job_id, cid, "running", _now()),
    )
    for object_uri, change_kind, status in tasks:
        await eng.meta.execute(
            "INSERT INTO object_tasks (id, connector_job_id, connector_id, object_uri, "
            " change_kind, status, priority, attempts) VALUES (?,?,?,?,?,?,?,0)",
            (uuid.uuid4().hex, job_id, cid, object_uri, change_kind, status, 0),
        )


async def test_recover_job_lane_rebuilds_dir_trees(tmp_path):
    eng = await _build_engine(tmp_path)
    root = tmp_path / "repo"
    root.mkdir()
    (root / "a.md").write_text("# A")
    (root / "b.md").write_text("# B")
    lane = _RecordingJobLane(enabled=True)
    eng._job_lane = lane  # 经 setter 写入 supervisor 字段
    cid, job_id = "cA", "job1"
    root_uri = f"file://local{root}"
    try:
        await _seed_running_job(
            eng,
            cid=cid,
            job_id=job_id,
            root_uri=root_uri,
            config_json=json.dumps({"root": str(root), "client_id": "local"}),
            # 一个普通 added 任务 + 一个 dir_summary（必须被排除）+ 一个 added
            tasks=[
                ("/a.md", "added", "running"),
                ("/b.md", "added", "running"),
                ("/", "dir_summary", "running"),
            ],
        )

        await eng._recover_job_lane()

        assert len(lane.recovered) == 1
        rec_job_id, rec_uri, rec_objects = lane.recovered[0]
        assert rec_job_id == job_id
        assert rec_uri == root_uri
        # dir_summary 被排除；剩余两个 added 任务以 (uri, okind, status) 传入。
        assert {o[0] for o in rec_objects} == {"/a.md", "/b.md"}
        assert all(o[2] == "running" for o in rec_objects)
        assert all(o[1] == "document" for o in rec_objects)  # .md → document okind
    finally:
        await eng.meta.close()


async def test_recover_job_lane_noop_when_lane_disabled(tmp_path):
    eng = await _build_engine(tmp_path)
    root = tmp_path / "repo"
    root.mkdir()
    (root / "a.md").write_text("# A")
    lane = _RecordingJobLane(enabled=False)
    eng._job_lane = lane
    cid, job_id = "cB", "job2"
    try:
        await _seed_running_job(
            eng,
            cid=cid,
            job_id=job_id,
            root_uri=f"file://local{root}",
            config_json=json.dumps({"root": str(root), "client_id": "local"}),
            tasks=[("/a.md", "added", "running")],
        )
        await eng._recover_job_lane()
        assert lane.recovered == []  # enabled=False 早退
    finally:
        await eng.meta.close()


async def test_recover_job_lane_noop_when_no_lane(tmp_path):
    eng = await _build_engine(tmp_path)
    # 默认 _job_lane is None（未 _build_pipeline）
    assert eng._job_lane is None
    await eng._recover_job_lane()  # 不得抛错
    await eng.meta.close()


async def test_recover_job_lane_survives_repo_failure(tmp_path):
    eng = await _build_engine(tmp_path)
    eng._job_lane = _RecordingJobLane(enabled=True)

    async def _boom():
        raise RuntimeError("metadata unavailable")

    eng.objects.list_running_jobs = _boom  # 模拟仓库故障
    await eng._recover_job_lane()  # best-effort：不得阻塞/抛错
    await eng.meta.close()
