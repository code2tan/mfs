"""``ConnectorFactory`` — 工厂 + 凭证（阶段 0 脚手架）+ 阶段 1 纯函数/安全逻辑。

详见 `docs/engine-redesign.md` §4.3。阶段 1 已把以下**纯函数 / 安全相关逻辑**从
``Engine`` 迁入本模块，``Engine`` 上的同名方法退化为薄委派（行为零变化）：

- ``CredentialRedactor`` —— ``Engine._redact_config`` / ``_is_secret_key`` +
  ``_SECRET_SUBSTRINGS`` / ``_CRED_REF_PREFIXES`` / ``_CONN_URI_RE`` / ``_REDACTED``。
  递归 redact inline secret；``env:``/``file:`` 引用保留；含口令的连接串按形状 redact。
- ``CredentialResolver`` —— ``Engine._resolve_ref``。**框架解析凭证的唯一入口**
  （``grep -r os.environ`` 审计：connector 配置凭证只经此处读取，见 §5 不变量表）。
- ``TargetResolver`` —— ``Engine._resolve_target``。scheme 路由 + github/file 特例 +
  裸本地路径兜底，未实现 scheme 显式 ``NotImplementedError``。

阶段 3/4 再迁入 plugin 构建（``_build_plugin`` / ``_match_object_config`` /
``BuiltPlugin`` 值对象）与 ``_open_path`` / ``_match_connector`` 定位逻辑。
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class TargetResolution:
    """``TargetResolver.resolve`` 的值对象，消除"返回 tuple 还要记位置"的负担。

    原引擎里 ``_resolve_target`` 返回 4-tuple ``(scheme, connector_uri, ctype, config)``，
    其中 ``scheme`` 与 ``ctype`` 恒等（所有分支 pos1 == pos3），故此处只保留 3 字段；
    ``Engine._resolve_target`` 委派时按原 4-tuple 顺序重建，保持所有调用点签名不变。
    """

    ctype: str
    connector_uri: str
    config: dict


# scheme://… 的 scheme 捕获。原 engine.py 模块级常量，迁入此处（Engine 不再直接引用）。
_SCHEME_RE = re.compile(r"^([a-z][a-z0-9+.\-]*)://")

# 裸 scheme 直接透传（无特例解析）的 connector 类型。
_PASSTHROUGH_SCHEMES = (
    "web",
    "postgres",
    "mysql",
    "mongo",
    "slack",
    "discord",
    "gmail",
    "notion",
    "jira",
    "linear",
    "zendesk",
    "hubspot",
    "bigquery",
    "snowflake",
    "s3",
    "gdrive",
    "feishu",
)


class TargetResolver:
    """``target`` 字符串 → ``(ctype, connector_uri, config)`` 的 scheme 路由。

    取代原 ``Engine._resolve_target`` 的 70 行 if-elif（§4.3）。github 与 file 各有
    特例（github 从 URI 推导 ``repo``；file 区分 ``file:///`` / ``file://local/`` /
    ``file://<client_id>`` / 裸本地路径）；其余已注册 scheme 直接透传；未注册 scheme
    显式 ``NotImplementedError``，绝不静默走兜底。
    """

    @classmethod
    def resolve(cls, target: str) -> TargetResolution:
        m = _SCHEME_RE.match(target)
        if m:
            sch = m.group(1)
            if sch == "github":
                # github://<owner>/<repo> (also tolerate github://github.com/<owner>/<repo>):
                # derive `repo` from the URI into the connector config so the bare documented
                # form works without an explicit `--config repo=…`. This mirrors how a local
                # path injects {root}; the plugin has no access to its own connector URI, so
                # the identity must be carried in config. Without it the github plugin's
                # _owner_repo() has no repo and the sync/read path raises a 500.
                rest = target[len("github://") :].strip("/")
                if rest.startswith("github.com/"):
                    rest = rest[len("github.com/") :]
                parts = [p for p in rest.split("/") if p]
                cfg = {"repo": f"{parts[0]}/{parts[1]}"} if len(parts) >= 2 else {}
                return TargetResolution("github", target, cfg)
            if sch in _PASSTHROUGH_SCHEMES:
                return TargetResolution(sch, target, {})
            if sch != "file":
                raise NotImplementedError(f"connector scheme '{sch}' not yet implemented")
        # file:///abs/path — empty authority — is the canonical URI for a LOCAL path
        #: treat it as the local path, not an upload identity, so
        # `mfs add file:///abs/path` registers with a real root instead of failing.
        if target.startswith("file:///"):
            abs_path = os.path.abspath(target[len("file://") :])
            return TargetResolution(
                "file",
                f"file://local{abs_path}",
                {"root": abs_path, "client_id": "local"},
            )
        # canonical local URI: file://local<abs_path> — what `mfs connector list` prints.
        # Map it back to the same (root, connector_uri) a bare path would resolve to,
        # so inspect/remove/update accept the identifier `connector list` shows.
        if target.startswith("file://local/"):
            abs_path = target[len("file://local") :]
            return TargetResolution(
                "file",
                f"file://local{abs_path}",
                {"root": abs_path, "client_id": "local"},
            )
        # logical upload identity file://<client_id><abs> (client_id != local): the real
        # config (staging root) lives on the already-registered connector, so return bare.
        if target.startswith("file://") and not target.startswith("file://local"):
            return TargetResolution("file", target, {})
        # local path -> file connector
        abs_path = os.path.abspath(target)
        connector_uri = f"file://local{abs_path}"
        return TargetResolution("file", connector_uri, {"root": abs_path, "client_id": "local"})


class CredentialRedactor:
    """递归 redact 配置中的 inline secret，落库前调用（``register_or_get_connector``）。

    安全相关，须单独可测（§1.2 / §5 不变量）。规则：
      - ``env:``/``file:`` 凭证引用保留原样（运行时由 ``CredentialResolver`` 解析）；
      - secret-looking key 下的非空值 → ``<redacted>``；
      - 不含 secret 词的字段（dsn/uri/url/connection）若值是带口令的连接串 → 按形状 redact。
    """

    # substrings that mark a config key as holding a secret. Matched case-insensitively
    # anywhere in the key, and recursively (nested OAuth token dicts, lists), so e.g.
    # secret_access_key / refresh_token / client_secret are all caught.
    # dsn (postgres) carries credentials but doesn't contain any of the obvious words;
    # we DON'T add 'uri'/'url' here because those also name benign fields (mongo's
    # password is caught by the value check below, while the web connector's target
    # urls must be kept).
    _SECRET_SUBSTRINGS = (
        "token",
        "secret",
        "password",
        "passwd",
        "apikey",
        "api_key",
        "access_key",
        "private_key",
        "refresh",
        "credential",
        "dsn",
        "session_id",
    )
    # credential-reference schemes that are actually resolved (see CredentialResolver). Only
    # these are treated as safe (kept, not redacted); anything else under a secret key is
    # redacted, so an unimplemented scheme can't masquerade as a working ref and silently
    # fail auth.
    _CRED_REF_PREFIXES = ("env:", "file:")
    # a connection string carrying inline credentials: scheme://user:password@host…
    # (postgres://u:p@…, mongodb://u:p@…). A plain URL with no userinfo password is NOT
    # matched, so web targets / instance_url stay intact.
    _CONN_URI_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9+.\-]*://[^/\s:@]+:[^/\s@]+@")
    _REDACTED = "<redacted: use credential_ref=env:VAR>"

    @classmethod
    def is_secret_key(cls, key: str) -> bool:
        kl = str(key).lower()
        return any(s in kl for s in cls._SECRET_SUBSTRINGS)

    @classmethod
    def redact(cls, value, key_is_secret: bool = False):
        """Recursively redact raw inline secrets from a config before persistence. A
        credential_ref (env:/secret:/file:/vault:) is kept; anything else under a
        secret-looking key is replaced. Recurses into dicts/lists so nested OAuth token
        dicts don't leak."""
        if isinstance(value, dict):
            return {k: cls.redact(v, cls.is_secret_key(k)) for k, v in value.items()}
        if isinstance(value, list):
            return [cls.redact(v, key_is_secret) for v in value]
        if isinstance(value, str) and value.startswith(cls._CRED_REF_PREFIXES):
            return value  # a safe credential reference, keep as-is
        if key_is_secret and value not in (None, "", [], {}):
            return cls._REDACTED
        # value-level catch: an inline connection string carrying a password leaks via a
        # field name (dsn/uri/url/connection) that doesn't look secret — redact by shape.
        if isinstance(value, str) and cls._CONN_URI_RE.search(value):
            return cls._REDACTED
        return value


class CredentialResolver:
    """把凭证引用解析为实际值：``env:VAR`` → 环境变量，``file:/path`` → 文件内容。

    **框架解析凭证的唯一入口**（``_build_plugin`` 构建插件时调用）。这是 §5 不变量
    "凭证只经 framework 解析，禁止 ``os.environ[...]``" 的代码落脚点：除本处与
    connector 内部已记录的镜像（snowflake sibling-secret）外，connector 配置凭证
    不应直接读 ``os.environ``。未实现 scheme（``secret:``/``vault:``）显式 raise，
    绝不把引用当字面 token 用。
    """

    @staticmethod
    def resolve(v):
        """Resolve a credential reference to its actual value: `env:VAR` ->
        environment, `file:/path` -> the file's contents (k8s/docker secret mounts).
        Non-ref values pass through unchanged. These are the only schemes _CRED_REF_PREFIXES
        advertises, so a ref left unresolved (and silently used as a literal token) can't
        happen."""
        if isinstance(v, str):
            if v.startswith("env:"):
                name = v[4:]
                if name not in os.environ:
                    raise ValueError(
                        f"credential_ref {v!r}: environment variable {name} is not set"
                    )
                return os.environ[name]
            if v.startswith("file:"):
                try:
                    with open(v[5:], encoding="utf-8") as f:
                        return f.read().strip()
                except OSError as e:
                    raise ValueError(f"credential_ref {v!r}: cannot read secret file ({e})") from e
            if v.startswith(("secret:", "vault:")):
                # advertised-looking but unimplemented schemes must fail loudly, never be
                # used as a literal credential token.
                raise ValueError(
                    f"credential_ref scheme {v.split(':', 1)[0]!r} is not implemented "
                    f"(use env: or file:)"
                )
        return v


class ConnectorFactory:
    """阶段 0 空壳：转发回 Engine 上的工厂/凭证方法。

    阶段 1 已把纯函数/安全逻辑（``CredentialRedactor`` / ``CredentialResolver`` /
    ``TargetResolver``）迁入本模块；``Engine`` 上的 ``_resolve_target`` /
    ``_redact_config`` / ``_resolve_ref`` 委派回此处。``_build_plugin`` 等 plugin
    构建逻辑留待阶段 3/4 迁入。
    """

    def __init__(self, engine):
        self._engine = engine
