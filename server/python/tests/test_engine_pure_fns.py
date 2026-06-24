"""阶段 1 抽离的纯函数 / 安全逻辑单测。

覆盖 `docs/engine-redesign-testgap.md` 阶段 1 前置单测要求：
- ``CredentialRedactor`` — 含 secret 的 config 不落库；``env:``/``file:`` 引用保留；
  带口令的连接串按形状 redact；benign url 保留；递归 dict/list。
- ``CredentialResolver`` — ``env:``/``file:`` 解析；未实现 scheme raise；非引用透传。
- ``TargetResolver`` — 注册 scheme 分派；github 推导 repo；未注册 scheme raise；
  file 各形态与裸本地路径兜底。
- ``density_view`` / ``locator_matches`` — 边界输入。
- ``BackoffPolicy`` — 指数退避 + ``max_ms`` 封顶。
- ``ErrorClassifier`` — auth/quota 不可重试；其余 retryable；429 非 quota。
"""

from __future__ import annotations

import pytest

from mfs_server.connectors.base import ObjectConfig
from mfs_server.engine.components import (
    BackoffPolicy,
    CredentialRedactor,
    CredentialResolver,
    ErrorClass,
    ErrorClassifier,
    TargetResolution,
    TargetResolver,
)
from mfs_server.engine.components.reads.text_views import density_view, locator_matches

REDACTED = CredentialRedactor._REDACTED


# ---------------------------------------------------------------------------
# CredentialRedactor
# ---------------------------------------------------------------------------


class TestCredentialRedactor:
    @pytest.mark.parametrize(
        "key",
        [
            "token",
            "access_token",
            "api_key",
            "apikey",
            "password",
            "passwd",
            "secret",
            "client_secret",
            "access_key",
            "private_key",
            "refresh_token",
            "credential",
            "dsn",
            "session_id",
        ],
    )
    def test_secret_keys_are_redacted(self, key):
        assert CredentialRedactor.redact("hunter2", key_is_secret=True) == REDACTED
        assert CredentialRedactor.is_secret_key(key) is True

    @pytest.mark.parametrize(
        "key", ["repo", "root", "url", "uri", "client_id", "name", "text_fields"]
    )
    def test_benign_keys_kept(self, key):
        assert CredentialRedactor.is_secret_key(key) is False
        assert CredentialRedactor.redact("plain", key_is_secret=False) == "plain"

    def test_empty_secret_value_kept(self):
        # empty/None values under a secret key are NOT redacted (no leak, avoid noise)
        for empty in (None, "", [], {}):
            assert CredentialRedactor.redact(empty, key_is_secret=True) == empty

    def test_env_file_refs_preserved_under_secret_key(self):
        assert CredentialRedactor.redact("env:MY_TOKEN", key_is_secret=True) == "env:MY_TOKEN"
        assert (
            CredentialRedactor.redact("file:/etc/secret", key_is_secret=True) == "file:/etc/secret"
        )

    def test_recursive_dict_redacts_nested_secrets(self):
        cfg = {
            "repo": "acme/web",
            "token": "abc123",  # secret
            "oauth": {"client_secret": "s", "client_id": "cid"},
            "tokens": ["env:OK", "plain-leak"],  # list under secret-looking key
            "dsn": "postgres://u:p@host/db",  # connection string with password
        }
        out = CredentialRedactor.redact(cfg)
        assert out["repo"] == "acme/web"
        assert out["token"] == REDACTED
        assert out["oauth"]["client_secret"] == REDACTED
        assert out["oauth"]["client_id"] == "cid"
        # 'tokens' key matches 'token' substring → secret key; list recursed:
        # env: ref preserved, plain value redacted
        assert out["tokens"] == ["env:OK", REDACTED]
        # dsn is both a secret key AND a connection string → redacted
        assert out["dsn"] == REDACTED

    def test_connection_string_redacted_by_shape(self):
        # field name 'uri' doesn't look secret, but the value carries user:password@
        out = CredentialRedactor.redact({"uri": "postgres://alice:s3cret@db.host:5432/app"})
        assert out["uri"] == REDACTED

    def test_benign_url_without_password_kept(self):
        # no userinfo password → not matched by _CONN_URI_RE, kept
        out = CredentialRedactor.redact({"url": "https://example.com/path"})
        assert out["url"] == "https://example.com/path"
        # web connector target urls (https://host, no password) stay intact
        out2 = CredentialRedactor.redact({"target": "https://docs.example.com"})
        assert out2["target"] == "https://docs.example.com"

    def test_redact_does_not_mutate_input(self):
        cfg = {"token": "abc", "repo": "x"}
        CredentialRedactor.redact(cfg)
        assert cfg == {"token": "abc", "repo": "x"}  # original unchanged


# ---------------------------------------------------------------------------
# CredentialResolver
# ---------------------------------------------------------------------------


class TestCredentialResolver:
    def test_env_ref_resolves(self, monkeypatch):
        monkeypatch.setenv("MFS_TEST_TOKEN", "tok-123")
        assert CredentialResolver.resolve("env:MFS_TEST_TOKEN") == "tok-123"

    def test_env_ref_missing_raises(self, monkeypatch):
        monkeypatch.delenv("MFS_TEST_DEFINITELY_MISSING", raising=False)
        with pytest.raises(ValueError, match="environment variable"):
            CredentialResolver.resolve("env:MFS_TEST_DEFINITELY_MISSING")

    def test_file_ref_resolves(self, tmp_path):
        p = tmp_path / "secret.txt"
        p.write_text("  file-secret-value\n")
        assert CredentialResolver.resolve(f"file:{p}") == "file-secret-value"  # stripped

    def test_file_ref_missing_raises(self):
        with pytest.raises(ValueError, match="cannot read secret file"):
            CredentialResolver.resolve("file:/no/such/path/here/abc")

    @pytest.mark.parametrize("ref", ["secret:foo", "vault:foo"])
    def test_unimplemented_scheme_raises(self, ref):
        # advertised-looking but unimplemented schemes must fail loudly, never be
        # used as a literal credential token.
        with pytest.raises(ValueError, match="not implemented"):
            CredentialResolver.resolve(ref)

    @pytest.mark.parametrize("v", ["plain", "", 42, None, {"k": "v"}])
    def test_non_ref_passes_through(self, v):
        assert CredentialResolver.resolve(v) == v


# ---------------------------------------------------------------------------
# TargetResolver
# ---------------------------------------------------------------------------


class TestTargetResolver:
    @pytest.mark.parametrize(
        "scheme",
        [
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
        ],
    )
    def test_passthrough_schemes(self, scheme):
        target = f"{scheme}://resource-id"
        res = TargetResolver.resolve(target)
        assert res == TargetResolution(scheme, target, {})

    def test_github_derives_repo(self):
        res = TargetResolver.resolve("github://owner/repo")
        assert res.ctype == "github"
        assert res.connector_uri == "github://owner/repo"
        assert res.config == {"repo": "owner/repo"}

    def test_github_tolerates_github_com_prefix(self):
        res = TargetResolver.resolve("github://github.com/owner/repo")
        assert res.config == {"repo": "owner/repo"}

    def test_github_without_repo_empty_config(self):
        res = TargetResolver.resolve("github://owner")
        assert res.config == {}

    def test_unregistered_scheme_raises(self):
        with pytest.raises(NotImplementedError, match="not yet implemented"):
            TargetResolver.resolve("ftp://host/path")

    def test_file_triple_slash_local_path(self, tmp_path):
        abs_path = str(tmp_path)
        target = f"file://{abs_path}"  # file:///<abs>
        res = TargetResolver.resolve(target)
        assert res.ctype == "file"
        assert res.connector_uri == f"file://local{abs_path}"
        assert res.config == {"root": abs_path, "client_id": "local"}

    def test_file_local_canonical_uri(self, tmp_path):
        abs_path = str(tmp_path)
        target = f"file://local{abs_path}"
        res = TargetResolver.resolve(target)
        assert res.connector_uri == f"file://local{abs_path}"
        assert res.config == {"root": abs_path, "client_id": "local"}

    def test_file_upload_identity_bare(self):
        # file://<client_id>/abs (client_id != local): staging root lives on the
        # already-registered connector, so config returned bare.
        target = "file://myclient/some/abs/path"
        res = TargetResolver.resolve(target)
        assert res == TargetResolution("file", target, {})

    def test_bare_local_path(self, tmp_path):
        abs_path = str(tmp_path)
        res = TargetResolver.resolve(abs_path)
        assert res.ctype == "file"
        assert res.connector_uri == f"file://local{abs_path}"
        assert res.config == {"root": abs_path, "client_id": "local"}


# ---------------------------------------------------------------------------
# density_view
# ---------------------------------------------------------------------------


class TestDensityView:
    def test_markdown_peek_headings(self):
        text = "# Title\npara\n## Sub\nmore"
        out = density_view(text, ".md", "peek")
        assert out == "# Title\n## Sub"

    def test_markdown_skim_includes_first_prose_line(self):
        text = "# Title\n\nfirst prose line\nsecond\n## Sub\nbody"
        out = density_view(text, ".md", "skim")
        assert "# Title" in out
        assert "    first prose line" in out  # indented skim line, truncated to 120

    def test_code_peek_strips_signature(self):
        text = "def foo(a, b):\n    return a\n\nclass Bar:\n    pass\n"
        out = density_view(text, ".py", "peek")
        # peek strips at '(' → 'def foo'
        assert "def foo" in out
        assert "class Bar" in out
        assert "(" not in out

    def test_code_skim_keeps_full_line(self):
        text = "def foo(a, b):\n    return a\n"
        out = density_view(text, ".py", "skim")
        assert "def foo(a, b):" in out

    def test_no_structure_falls_back_to_first_lines(self):
        text = "line1\nline2\nline3"
        out = density_view(text, ".md", "peek")
        # no headings → fallback to first 15 lines
        assert out == "line1\nline2\nline3"

    def test_empty_text(self):
        assert density_view("", ".md", "peek") == ""


# ---------------------------------------------------------------------------
# locator_matches
# ---------------------------------------------------------------------------


class TestLocatorMatches:
    def test_row_locator_matches_index(self):
        ocfg = ObjectConfig(locator_fields=["id"])
        assert locator_matches({"id": "x"}, ocfg, 3, {"_row": 3}) is True
        assert locator_matches({"id": "x"}, ocfg, 3, {"_row": 1}) is False

    def test_match_by_locator_fields(self):
        ocfg = ObjectConfig(locator_fields=["id"])
        rec = {"id": "abc", "text": "hello"}
        assert locator_matches(rec, ocfg, 0, {"id": "abc"}) is True
        assert locator_matches(rec, ocfg, 0, {"id": "xyz"}) is False

    def test_empty_locator_returns_false(self):
        # guard against all([]) == True silently matching record #0
        ocfg = ObjectConfig(locator_fields=["id"])
        assert locator_matches({"id": "abc"}, ocfg, 0, {}) is False

    def test_unrecognized_keys_return_false(self):
        ocfg = ObjectConfig(locator_fields=["id"])
        # locator keys don't intersect locator_fields → no present keys → False
        assert locator_matches({"id": "abc"}, ocfg, 0, {"bogus": "x"}) is False

    def test_lines_key_ignored(self):
        # "lines" is framework-reserved, never compared against the row
        ocfg = ObjectConfig(locator_fields=["id"])
        rec = {"id": "abc"}
        # a locator that ONLY has "lines" → no recognized keys → False (not record #0)
        assert locator_matches(rec, ocfg, 0, {"lines": [0, 10]}) is False
        # a locator with id + lines → id drives the match, lines ignored
        assert locator_matches(rec, ocfg, 0, {"id": "abc", "lines": [0, 10]}) is True

    def test_uses_locator_fields_when_present(self):
        # when locator has keys not in ocfg.locator_fields but ocfg.locator_fields empty,
        # falls back to locator's own keys
        ocfg = ObjectConfig(locator_fields=[])
        rec = {"key": "k1"}
        assert locator_matches(rec, ocfg, 0, {"key": "k1"}) is True


# ---------------------------------------------------------------------------
# BackoffPolicy
# ---------------------------------------------------------------------------


class TestBackoffPolicy:
    def test_exponential_growth(self):
        bp = BackoffPolicy(initial_ms=1000, max_ms=30000)
        assert bp.delay_ms(0) == 1000
        assert bp.delay_ms(1) == 2000
        assert bp.delay_ms(2) == 4000
        assert bp.delay_ms(3) == 8000

    def test_capped_at_max(self):
        bp = BackoffPolicy(initial_ms=1000, max_ms=5000)
        assert bp.delay_ms(0) == 1000
        assert bp.delay_ms(2) == 4000
        assert bp.delay_ms(3) == 5000  # 8000 capped to 5000
        assert bp.delay_ms(10) == 5000  # stays capped

    def test_max_equals_initial(self):
        bp = BackoffPolicy(initial_ms=100, max_ms=100)
        assert bp.delay_ms(0) == 100
        assert bp.delay_ms(5) == 100


# ---------------------------------------------------------------------------
# ErrorClassifier
# ---------------------------------------------------------------------------


class TestErrorClassifier:
    @pytest.mark.parametrize(
        "msg",
        [
            "invalid_api_key provided",
            "Unauthorized access",
            "401 Client Error",
            "permission denied for resource",
            "Authentication failed",
        ],
    )
    def test_auth_markers(self, msg):
        assert ErrorClassifier.classify(RuntimeError(msg)) is ErrorClass.AUTH

    def test_auth_by_exception_type_name(self):
        class AuthenticationError(Exception):
            pass

        assert ErrorClassifier.classify(AuthenticationError("nope")) is ErrorClass.AUTH

    @pytest.mark.parametrize("msg", ["insufficient_quota exceeded", "402 Payment Required"])
    def test_quota_markers(self, msg):
        assert ErrorClassifier.classify(RuntimeError(msg)) is ErrorClass.QUOTA

    def test_transient_is_retryable(self):
        assert (
            ErrorClassifier.classify(RuntimeError("429 Too Many Requests")) is ErrorClass.RETRYABLE
        )
        assert (
            ErrorClassifier.classify(TimeoutError("connection timed out")) is ErrorClass.RETRYABLE
        )
        assert (
            ErrorClassifier.classify(RuntimeError("503 Service Unavailable"))
            is ErrorClass.RETRYABLE
        )

    def test_classify_returns_enum_with_string_value(self):
        assert ErrorClassifier.classify(RuntimeError("boom")).value == "retryable"
        assert ErrorClassifier.classify(RuntimeError("401")).value == "auth"

    def test_engine_classify_error_delegates_and_returns_str(self):
        # Engine._classify_error kept as a thin delegator returning the str value
        from mfs_server.engine.engine import Engine

        assert Engine._classify_error(RuntimeError("401")) == "auth"
        assert Engine._classify_error(RuntimeError("insufficient_quota")) == "quota"
        assert Engine._classify_error(RuntimeError("429 rate limit")) == "retryable"


# ---------------------------------------------------------------------------
# Engine delegation smoke (behavior equivalence of the thin delegators)
# ---------------------------------------------------------------------------


class TestEngineDelegation:
    def test_resolve_target_returns_legacy_4tuple(self, tmp_path):
        from mfs_server.engine.engine import Engine

        abs_path = str(tmp_path)
        scheme, connector_uri, ctype, config = Engine._resolve_target(None, abs_path)
        assert scheme == "file"
        assert ctype == "file"  # pos1 == pos3 invariant
        assert connector_uri == f"file://local{abs_path}"
        assert config == {"root": abs_path, "client_id": "local"}

    def test_redact_config_delegates(self):
        from mfs_server.engine.engine import Engine

        assert Engine._redact_config({"token": "x"}) == {"token": REDACTED}
        assert Engine._is_secret_key("password") is True

    def test_resolve_ref_delegates(self, monkeypatch):
        from mfs_server.engine.engine import Engine

        monkeypatch.setenv("MFS_TEST_REF", "v")
        assert Engine._resolve_ref("env:MFS_TEST_REF") == "v"
        assert Engine._resolve_ref("plain") == "plain"
