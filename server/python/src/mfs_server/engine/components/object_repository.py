"""``ObjectRepository`` + 状态机（阶段 0 脚手架）。

详见 `docs/engine-redesign.md` §4.4。当前 `objects` / `object_tasks` /
`connector_jobs` 三张表的 SQL 散落在 `Engine` 的 15+ 处方法里。阶段 2 把这些 SQL
逐方法迁入本仓库，并把 `pending→running→succeeded/failed/skipped/cancelled`
等合法迁移收敛到 ``_TRANSITIONS`` 表 + ``advance_task`` 守卫。

不变量（§5）：一条 running/queued job per connector，由 ``open_sync_job`` 唯一
约束 + `sync_already_running` 唯一出口保证。
"""

from __future__ import annotations


class ObjectRepository:
    """阶段 0 空壳：转发回 Engine 上的表读写方法。"""

    def __init__(self, engine):
        self._engine = engine
