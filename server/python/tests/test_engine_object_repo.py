"""阶段 2 ``ObjectRepository`` + 状态机单测。

覆盖 `docs/engine-redesign-testgap.md` 阶段 2 前置单测要求：

- ``_TASK_TRANSITIONS`` / ``_JOB_TRANSITIONS`` 合法迁移参数化测试；
- ``advance_task`` 非法迁移抛错（绝不静默写脏）；
- ``advance_task`` 的 ``won == 0`` 并发取消场景（§5 不变量：chunk 存在 ⇔ 已提交
  objects 行；被并发取消时不 commit、调用方据此删孤儿 chunk）；
- 关键仓库方法行为：``open_sync_job`` 唯一约束 + ``sync_already_running`` 唯一出口、
  ``finalize_job`` 计数、``claim_tasks`` 并发安全。

构造方式同 ``test_engine_cancel_reconcile.py``：真实 sqlite meta + schema，直接走
``Engine.objects``（阶段 2 已迁入的仓库），断言三张表状态。
"""

from __future__ import annotations

import uuid

import pytest

from mfs_server.config import ServerConfig
from mfs_server.connectors.base import PathStat
from mfs_server.engine.components import JobStatus, TaskStatus
from mfs_server.engine.components.object_repository import (
    _JOB_TRANSITIONS,
    _TASK_TRANSITIONS,
)
from mfs_server.engine.engine import Engine


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


async def _build_engine(tmp_path) -> Engine:
    cfg = ServerConfig()
    cfg.metadata.backend = "sqlite"
    cfg.metadata.path = str(tmp_path / "meta.db")
    cfg.transformation_cache.backend = "sqlite"
    cfg.transformation_cache.db_path = str(tmp_path / "tx.db")
    cfg.artifact_cache.root = str(tmp_path / "art")
    eng = Engine(cfg)
    await eng.meta.connect()
    await eng.meta.init_schema()
    await eng.meta.execute("PRAGMA foreign_keys=OFF")  # seed object_tasks without parent job
    return eng


def _stat(rel: str) -> PathStat:
    return PathStat(
        path=rel,
        type="file",
        media_type="text/markdown",
        size_hint=10,
        fingerprint="fp:" + rel,
    )


async def _seed_connector(eng, *, cid="c1", connector_uri="file:///repo") -> str:
    await eng.meta.execute(
        "INSERT INTO connectors (id, namespace_id, root_uri, type, status, config_json, registered_at) "
        "VALUES (?,?,?,?,?,?,?)",
        (cid, eng.ns, connector_uri, "file", "active", "{}", "2026-01-01T00:00:00+00:00"),
    )
    return cid


async def _seed_task(eng, *, task_id, job_id, cid, object_uri, status, change_kind="added"):
    await eng.meta.execute(
        "INSERT INTO object_tasks (id, connector_job_id, connector_id, object_uri, old_uri, "
        " change_kind, status, priority, attempts) VALUES (?,?,?,?,?,?,?,?,0)",
        (task_id, job_id, cid, object_uri, None, change_kind, status, 0),
    )


async def _seed_job(eng, *, job_id, cid, status="running"):
    await eng.meta.execute(
        "INSERT INTO connector_jobs (id, namespace_id, connector_id, op_kind, trigger, status, "
        " started_at, heartbeat) VALUES (?,?,?,?,?,?,?,?)",
        (
            job_id,
            eng.ns,
            cid,
            "sync",
            "manual",
            status,
            "2026-01-01T00:00:00+00:00",
            "2026-01-01T00:00:00+00:00",
        ),
    )


# ---------------------------------------------------------------------------
# state machine: transitions table
# ---------------------------------------------------------------------------


class TestTransitionsTable:
    """``_TASK_TRANSITIONS`` / ``_JOB_TRANSITIONS`` 是纯数据，作为后续所有
    ``advance_task`` 调用的安全网（testgap §3 建议）。穷举合法/非法迁移。"""

    @pytest.mark.parametrize(
        "frm,to",
        [
            (TaskStatus.PENDING, TaskStatus.RUNNING),
            (TaskStatus.PENDING, TaskStatus.CANCELLED),
            (TaskStatus.RUNNING, TaskStatus.SUCCEEDED),
            (TaskStatus.RUNNING, TaskStatus.FAILED),
            (TaskStatus.RUNNING, TaskStatus.SKIPPED),
            (TaskStatus.RUNNING, TaskStatus.CANCELLED),
            (TaskStatus.RUNNING, TaskStatus.PENDING),  # reclaim
            (TaskStatus.FAILED, TaskStatus.PENDING),  # reopen retry
        ],
    )
    def test_legal_task_transitions(self, frm, to):
        assert (frm, to) in _TASK_TRANSITIONS

    @pytest.mark.parametrize(
        "frm,to",
        [
            # terminal states must never leave
            (TaskStatus.SUCCEEDED, TaskStatus.RUNNING),
            (TaskStatus.SUCCEEDED, TaskStatus.FAILED),
            (TaskStatus.FAILED, TaskStatus.SUCCEEDED),
            (TaskStatus.CANCELLED, TaskStatus.RUNNING),
            (TaskStatus.CANCELLED, TaskStatus.SUCCEEDED),
            (TaskStatus.SKIPPED, TaskStatus.RUNNING),
            # no skipping running
            (TaskStatus.PENDING, TaskStatus.SUCCEEDED),
            (TaskStatus.PENDING, TaskStatus.FAILED),
        ],
    )
    def test_illegal_task_transitions(self, frm, to):
        assert (frm, to) not in _TASK_TRANSITIONS

    @pytest.mark.parametrize(
        "frm,to",
        [
            (JobStatus.PREPARING, JobStatus.QUEUED),
            (JobStatus.PREPARING, JobStatus.FAILED),
            (JobStatus.QUEUED, JobStatus.RUNNING),
            (JobStatus.RUNNING, JobStatus.SUCCEEDED),
            (JobStatus.RUNNING, JobStatus.FAILED),
            (JobStatus.RUNNING, JobStatus.CANCELLED),
            (JobStatus.RUNNING, JobStatus.QUEUED),  # reclaim re-queue
        ],
    )
    def test_legal_job_transitions(self, frm, to):
        assert (frm, to) in _JOB_TRANSITIONS

    @pytest.mark.parametrize(
        "frm,to",
        [
            (JobStatus.SUCCEEDED, JobStatus.RUNNING),
            (JobStatus.FAILED, JobStatus.SUCCEEDED),
            (JobStatus.CANCELLED, JobStatus.RUNNING),
            (JobStatus.PREPARING, JobStatus.RUNNING),  # must go via queued
        ],
    )
    def test_illegal_job_transitions(self, frm, to):
        assert (frm, to) not in _JOB_TRANSITIONS


# ---------------------------------------------------------------------------
# advance_task: illegal -> raise (never silently write dirty)
# ---------------------------------------------------------------------------


class TestAdvanceTaskGuards:
    async def test_illegal_transition_raises(self, tmp_path):
        eng = await _build_engine(tmp_path)
        cid = await _seed_connector(eng)
        tid = uuid.uuid4().hex
        await _seed_task(
            eng, task_id=tid, job_id="j1", cid=cid, object_uri="/a.md", status="succeeded"
        )
        with pytest.raises(ValueError, match="illegal task transition"):
            await eng.objects.advance_task(
                tid, TaskStatus.RUNNING, from_status=TaskStatus.SUCCEEDED
            )
        # row untouched
        row = await eng.meta.fetchone("SELECT status FROM object_tasks WHERE id=?", (tid,))
        assert row["status"] == "succeeded"
        await eng.meta.close()

    async def test_legal_running_to_succeeded_flips_row(self, tmp_path):
        eng = await _build_engine(tmp_path)
        cid = await _seed_connector(eng)
        tid = uuid.uuid4().hex
        await _seed_task(
            eng, task_id=tid, job_id="j1", cid=cid, object_uri="/a.md", status="running"
        )
        won = await eng.objects.advance_task(
            tid, TaskStatus.SUCCEEDED, from_status=TaskStatus.RUNNING
        )
        assert won == 1
        row = await eng.meta.fetchone("SELECT status FROM object_tasks WHERE id=?", (tid,))
        assert row["status"] == "succeeded"
        await eng.meta.close()


# ---------------------------------------------------------------------------
# advance_task: won == 0 — concurrent cancel / orphan-chunk reconcile
# (§5 invariant: chunk exists iff committed objects row; cancelled task => no commit)
# ---------------------------------------------------------------------------


class TestConcurrentCancelWonZero:
    async def test_won_zero_when_task_no_longer_running(self, tmp_path):
        """Mirrors ``test_engine_cancel_reconcile``: a task cancelled out from under the
        shared EmbedConsumer loses the completion claim. ``advance_task`` returns 0 so the
        caller (``_on_pipeline_object_indexed``) deletes the orphan chunks instead of
        committing an objects row."""
        eng = await _build_engine(tmp_path)
        cid = await _seed_connector(eng)
        tid = uuid.uuid4().hex
        # task was cancelled while its chunks were embedding (no longer 'running')
        await _seed_task(
            eng, task_id=tid, job_id="j1", cid=cid, object_uri="/a.md", status="cancelled"
        )
        won = await eng.objects.advance_task(
            tid, TaskStatus.SUCCEEDED, from_status=TaskStatus.RUNNING
        )
        assert won == 0
        # no objects row should be committed by a caller honoring won==0
        row = await eng.meta.fetchone(
            "SELECT * FROM objects WHERE connector_id=? AND object_uri=?", (cid, "/a.md")
        )
        assert row is None
        # task stays cancelled (the guarded UPDATE matched nothing)
        t = await eng.meta.fetchone("SELECT status FROM object_tasks WHERE id=?", (tid,))
        assert t["status"] == "cancelled"
        await eng.meta.close()

    async def test_won_zero_then_pipeline_hook_purges_orphans(self, tmp_path):
        """End-to-end through the Engine hook that consumes ``won``: the EmbedConsumer
        success hook must delete orphan chunks when the claim is lost."""
        eng = await _build_engine(tmp_path)

        class _RecordingMilvus:
            def __init__(self):
                self.deletes: list[tuple] = []

            def delete_by_object(self, ns, connector_uri, object_uri):
                self.deletes.append((ns, connector_uri, object_uri))

        eng.milvus = _RecordingMilvus()
        cid, job_id, connector_uri = "c2", "job1", "file:///repo"
        await _seed_connector(eng, cid=cid, connector_uri=connector_uri)
        relpath, tid = "/a.md", uuid.uuid4().hex
        task_uri = connector_uri + relpath
        await _seed_task(
            eng, task_id=tid, job_id=job_id, cid=cid, object_uri=relpath, status="cancelled"
        )

        class _Plugin:
            async def on_object_indexed(self, rel):
                pass

        eng._pending_finalize[task_uri] = (
            cid,
            connector_uri,
            relpath,
            _stat(relpath),
            True,
            _Plugin(),
            tid,
        )
        await eng._on_pipeline_object_indexed(task_uri, job_id, chunk_count=3, partial=False)
        # the chunks it upserted are reconciled away, keyed by the full object uri
        assert eng.milvus.deletes == [(eng.ns, connector_uri, task_uri)]
        await eng.meta.close()


# ---------------------------------------------------------------------------
# open_sync_job: one running/queued job per connector
# ---------------------------------------------------------------------------


class TestOpenSyncJob:
    async def test_open_sync_job_returns_new_id(self, tmp_path):
        eng = await _build_engine(tmp_path)
        cid = await _seed_connector(eng)
        job_id = await eng.objects.open_sync_job(cid, process=True)
        assert job_id
        row = await eng.meta.fetchone("SELECT status FROM connector_jobs WHERE id=?", (job_id,))
        assert row["status"] == "running"
        await eng.meta.close()

    async def test_second_inflight_job_raises_sync_already_running(self, tmp_path):
        eng = await _build_engine(tmp_path)
        cid = await _seed_connector(eng)
        await eng.objects.open_sync_job(cid, process=True)
        with pytest.raises(ValueError, match="sync_already_running"):
            await eng.objects.open_sync_job(cid, process=True)
        await eng.meta.close()

    async def test_removing_connector_raises_connector_removing(self, tmp_path):
        eng = await _build_engine(tmp_path)
        cid = await _seed_connector(eng)
        await eng.objects.set_connector_removing(cid)
        with pytest.raises(ValueError, match="connector_removing"):
            await eng.objects.open_sync_job(cid, process=True)
        await eng.meta.close()

    async def test_open_reopens_leftover_failed_tasks(self, tmp_path):
        """A new sync job re-attaches a connector's leftover pending/failed tasks
        (bounded by max_retries)."""
        eng = await _build_engine(tmp_path)
        cid = await _seed_connector(eng)
        # leftover failed task from a previous run
        old_tid = uuid.uuid4().hex
        await _seed_task(
            eng, task_id=old_tid, job_id="oldjob", cid=cid, object_uri="/old.md", status="failed"
        )
        await eng.meta.execute(
            "UPDATE object_tasks SET attempts=? WHERE id=?",
            (eng.cfg.object_task.max_retries, old_tid),
        )
        # a pending task within retries is re-attached; the exhausted one is NOT
        pend_tid = uuid.uuid4().hex
        await _seed_task(
            eng, task_id=pend_tid, job_id="oldjob", cid=cid, object_uri="/pend.md", status="pending"
        )
        job_id = await eng.objects.open_sync_job(cid, process=True)
        row = await eng.meta.fetchone(
            "SELECT connector_job_id FROM object_tasks WHERE id=?", (pend_tid,)
        )
        assert row["connector_job_id"] == job_id
        await eng.meta.close()


# ---------------------------------------------------------------------------
# finalize_job: per-status object counts
# ---------------------------------------------------------------------------


class TestFinalizeJob:
    async def test_succeeded_counts(self, tmp_path):
        eng = await _build_engine(tmp_path)
        cid = await _seed_connector(eng)
        job_id = "j1"
        await _seed_job(eng, job_id=job_id, cid=cid, status="running")
        for i, st in enumerate(("succeeded", "succeeded", "failed", "cancelled")):
            await _seed_task(
                eng, task_id=f"t{i}", job_id=job_id, cid=cid, object_uri=f"/{i}.md", status=st
            )
        status = await eng.objects.finalize_job(job_id, aborted=None)
        assert status == "succeeded"
        row = await eng.meta.fetchone(
            "SELECT total_objects, succeeded_objects, failed_objects, cancelled_objects "
            "FROM connector_jobs WHERE id=?",
            (job_id,),
        )
        assert row["total_objects"] == 4
        assert row["succeeded_objects"] == 2
        assert row["failed_objects"] == 1
        assert row["cancelled_objects"] == 1
        await eng.meta.close()

    async def test_aborted_marks_failed(self, tmp_path):
        eng = await _build_engine(tmp_path)
        cid = await _seed_connector(eng)
        job_id = "j1"
        await _seed_job(eng, job_id=job_id, cid=cid, status="running")
        status = await eng.objects.finalize_job(job_id, aborted="sync_error: boom")
        assert status == "failed"
        row = await eng.meta.fetchone(
            "SELECT status, error FROM connector_jobs WHERE id=?", (job_id,)
        )
        assert row["status"] == "failed"
        assert "boom" in row["error"]
        await eng.meta.close()

    async def test_cancelled_job_stays_cancelled_even_if_aborted(self, tmp_path):
        """If the job row was already flipped to 'cancelled' (e.g. by ``cancel_job``),
        finalize must NOT overwrite it to 'failed'."""
        eng = await _build_engine(tmp_path)
        cid = await _seed_connector(eng)
        job_id = "j1"
        await _seed_job(eng, job_id=job_id, cid=cid, status="cancelled")
        status = await eng.objects.finalize_job(job_id, aborted="sync_error: boom")
        assert status == "cancelled"
        await eng.meta.close()


# ---------------------------------------------------------------------------
# claim_tasks: multi-worker safe (conditional UPDATE, rowcount == 1)
# ---------------------------------------------------------------------------


class TestClaimTasks:
    async def test_claim_returns_only_flipped_rows(self, tmp_path):
        eng = await _build_engine(tmp_path)
        cid = await _seed_connector(eng)
        job_id = "j1"
        tids = [uuid.uuid4().hex for _ in range(3)]
        for t in tids:
            await _seed_task(
                eng, task_id=t, job_id=job_id, cid=cid, object_uri=f"/{t}.md", status="pending"
            )
        claimed = await eng.objects.claim_tasks(cid, limit=64)
        assert {r["id"] for r in claimed} == set(tids)
        rows = await eng.meta.fetchall(
            "SELECT status FROM object_tasks WHERE connector_job_id=?", (job_id,)
        )
        assert all(r["status"] == "running" for r in rows)
        await eng.meta.close()

    async def test_claim_excludes_dir_summary(self, tmp_path):
        """dir_summary is never an object_task (the Job Lane owns it); claim defensively
        excludes a stray row."""
        eng = await _build_engine(tmp_path)
        cid = await _seed_connector(eng)
        job_id = "j1"
        t_normal = uuid.uuid4().hex
        t_dir = uuid.uuid4().hex
        await _seed_task(
            eng, task_id=t_normal, job_id=job_id, cid=cid, object_uri="/a.md", status="pending"
        )
        await _seed_task(
            eng,
            task_id=t_dir,
            job_id=job_id,
            cid=cid,
            object_uri="/dir",
            status="pending",
            change_kind="dir_summary",
        )
        claimed = await eng.objects.claim_tasks(cid, limit=64)
        assert [r["id"] for r in claimed] == [t_normal]
        await eng.meta.close()

    async def test_claim_skips_already_running(self, tmp_path):
        """A task already flipped to 'running' by a concurrent worker is not re-claimed:
        the conditional UPDATE guarded on status='pending' matches nothing (won == 0)."""
        eng = await _build_engine(tmp_path)
        cid = await _seed_connector(eng)
        job_id = "j1"
        t_running = uuid.uuid4().hex
        t_pending = uuid.uuid4().hex
        await _seed_task(
            eng, task_id=t_running, job_id=job_id, cid=cid, object_uri="/r.md", status="running"
        )
        await _seed_task(
            eng, task_id=t_pending, job_id=job_id, cid=cid, object_uri="/p.md", status="pending"
        )
        claimed = await eng.objects.claim_tasks(cid, limit=64)
        assert [r["id"] for r in claimed] == [t_pending]
        await eng.meta.close()


# ---------------------------------------------------------------------------
# write_object_row / delete_object_row: objects registry UPSERT
# ---------------------------------------------------------------------------


class TestObjectsRow:
    async def test_write_then_update_upserts(self, tmp_path):
        eng = await _build_engine(tmp_path)
        cid = await _seed_connector(eng)
        await eng.objects.write_object_row(cid, "/a.md", _stat("/a.md"), True, "indexed", 2)
        await eng.objects.write_object_row(cid, "/a.md", _stat("/a.md"), True, "partial", 1)
        row = await eng.meta.fetchone(
            "SELECT search_status, chunk_count FROM objects WHERE connector_id=? AND object_uri=?",
            (cid, "/a.md"),
        )
        assert row["search_status"] == "partial"
        assert row["chunk_count"] == 1
        await eng.meta.close()

    async def test_delete_object_row(self, tmp_path):
        eng = await _build_engine(tmp_path)
        cid = await _seed_connector(eng)
        await eng.objects.write_object_row(cid, "/a.md", _stat("/a.md"), True, "indexed", 2)
        await eng.objects.delete_object_row(cid, "/a.md")
        row = await eng.meta.fetchone(
            "SELECT 1 FROM objects WHERE connector_id=? AND object_uri=?", (cid, "/a.md")
        )
        assert row is None
        await eng.meta.close()
