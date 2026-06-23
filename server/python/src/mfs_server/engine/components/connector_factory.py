"""``ConnectorFactory`` — 工厂 + 凭证（阶段 0 脚手架）。

详见 `docs/engine-redesign.md` §4.3。当前 `Engine` 仍持有 `_resolve_target` /
`_redact_config` / `_resolve_ref` / `_build_plugin` / `_match_object_config` /
`_match_connector` / `_open_path` 等方法。阶段 1 迁出凭证安全逻辑
（``CredentialRedactor`` / ``CredentialResolver``），阶段 3/4 迁入 target 解析
（``TargetResolver`` strategy table）与 plugin 构建（``BuiltPlugin`` 值对象）。
"""

from __future__ import annotations


class ConnectorFactory:
    """阶段 0 空壳：转发回 Engine 上的工厂/凭证方法。"""

    def __init__(self, engine):
        self._engine = engine
