"""Unit tests for ReadService: search, content reading, and path resolution.

These tests verify the ReadService component's behavior independently, using
fake connector plugins and in-memory metadata, without requiring Milvus or
the embedding model.
"""

from __future__ import annotations

import pytest

from mfs_server.config import ServerConfig
from mfs_server.connectors.base import ObjectConfig, PathStat
from mfs_server.engine.engine import Engine
from mfs_server.engine.reads import ReadService


# --- fakes ---


class _FailingEmbed:
    async def batch_embed(self, texts):
        raise AssertionError("empty search should not call the embedder")


class _FailingMilvus:
    def sparse_search(self, *args, **kwargs):
        raise AssertionError("empty search should not call Milvus")

    def search_dense(self, *args, **kwargs):
        raise AssertionError("empty search should not call Milvus")

    def hybrid_search(self, *args, **kwargs):
        raise AssertionError("empty search should not call Milvus")


class _FakeConnCtx:
    def object_config_for(self, path):
        return ObjectConfig(text_fields=["title"], locator_fields=["id"])

    def was_partial(self, path):
        return False


class _FakeStructuredPlugin:
    """Minimal structured connector: yields canned records as table_rows."""

    def __init__(self, records: list[dict]):
        self._records = records
        self.ctx = _FakeConnCtx()
        self.closed = False

    async def stat(self, rel):
        return PathStat(
            path=rel,
            type="file",
            media_type="application/x-collection",
            size_hint=1,
            fingerprint="fp:" + rel,
        )

    def object_kind_of(self, rel):
        return "table_rows"

    def read_records(self, rel, range=None):
        recs = self._records

        async def gen():
            for r in recs:
                yield r

        return gen()

    async def close(self) -> None:
        self.closed = True


class _FakeTextPlugin:
    """Minimal file connector: canned text content, document okind."""

    def __init__(self, text: str):
        self._text = text
        self.ctx = _FakeConnCtx()
        self.closed = False

    async def stat(self, rel):
        return PathStat(
            path=rel,
            type="file",
            media_type="text/plain",
            size_hint=len(self._text),
            fingerprint="fp:" + rel,
        )

    def object_kind_of(self, rel):
        return "document"

    async def read(self, rel, range=None):
        yield self._text.encode("utf-8")

    async def list(self, rel):
        return []

    @property
    def CAPABILITIES(self):
        from mfs_server.connectors.base import ConnectorCapabilities

        return ConnectorCapabilities()

    async def close(self) -> None:
        self.closed = True


async def _build_engine(tmp_path) -> Engine:
    cfg = ServerConfig()
    cfg.metadata.backend = "sqlite"
    cfg.metadata.path = str(tmp_path / "meta.db")
    cfg.transformation_cache.backend = "sqlite"
    cfg.transformation_cache.db_path = str(tmp_path / "tx.db")
    cfg.artifact_cache.root = str(tmp_path / "art")
    eng = Engine(cfg)
    eng.infra.embed = _FailingEmbed()
    eng.infra.milvus = _FailingMilvus()
    await eng.infra.meta.connect()
    await eng.infra.meta.init_schema()
    return eng


# --- construction tests ---


def test_read_service_constructs(tmp_path) -> None:
    """ReadService can be constructed with the standard dependencies."""
    cfg = ServerConfig()
    cfg.metadata.backend = "sqlite"
    cfg.metadata.path = str(tmp_path / "meta.db")
    eng = Engine(cfg)
    assert isinstance(eng.reads, ReadService)
    assert eng.reads is eng.reads  # stable reference
    assert eng.reads._cfg is cfg
    assert eng.reads._infra is eng.infra
    assert eng.reads._factory is eng.connector_factory
    assert eng.reads._art is eng.artifacts
    assert eng.reads._obj is eng.objects


# --- search tests ---


async def test_search_empty_namespace_returns_without_query_backend(tmp_path) -> None:
    """An empty namespace must not call Milvus or the embedder."""
    eng = await _build_engine(tmp_path)
    try:
        assert await eng.search("mfs-e2e-empty-query", top_k=3) == []
    finally:
        await eng.infra.meta.close()


async def test_search_unregistered_scope_returns_without_query_backend(tmp_path) -> None:
    """An unregistered connector_uri scope must not call Milvus or the embedder."""
    eng = await _build_engine(tmp_path)
    try:
        assert (
            await eng.search(
                "mfs-e2e-empty-query",
                connector_uri="file://local/mfs-e2e-empty",
                object_prefix=None,
                top_k=3,
            )
            == []
        )
    finally:
        await eng.infra.meta.close()


async def test_search_top_k_zero_returns_empty(tmp_path) -> None:
    """A zero top_k must return [] without calling any backend."""
    eng = await _build_engine(tmp_path)
    try:
        assert await eng.search("query", top_k=0) == []
    finally:
        await eng.infra.meta.close()


# --- cat tests ---


async def test_cat_with_matching_dict_locator_returns_the_record(tmp_path) -> None:
    """cat with a locator that matches a record returns the record content."""
    eng = await _build_engine(tmp_path)
    plugin = _FakeStructuredPlugin([{"id": "ord_1001", "title": "widget"}])

    async def fake_open_path(path: str):
        return "cid", "postgres://db", "/orders", plugin

    eng.reads._open_path = fake_open_path  # type: ignore[method-assign]

    out = await eng.cat("postgres://db/orders", locator={"id": "ord_1001"})

    assert out["locator"] == {"id": "ord_1001"}
    assert "ord_1001" in out["content"]
    await eng.infra.meta.close()


async def test_cat_with_dict_locator_no_match_raises_locator_not_found(tmp_path) -> None:
    """cat with a locator that matches no record raises locator_not_found."""
    eng = await _build_engine(tmp_path)
    plugin = _FakeStructuredPlugin([{"id": "ord_1001", "title": "widget"}])

    async def fake_open_path(path: str):
        return "cid", "postgres://db", "/orders", plugin

    eng.reads._open_path = fake_open_path  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="locator_not_found"):
        await eng.cat("postgres://db/orders", locator={"id": "does_not_exist"})

    await eng.infra.meta.close()


async def test_cat_text_object_returns_content(tmp_path) -> None:
    """cat on a plain-text document returns the file content."""
    eng = await _build_engine(tmp_path)
    plugin = _FakeTextPlugin("hello world\nline two")

    async def fake_open_path(path: str):
        return "cid", "file://local/root", "/doc.md", plugin

    eng.reads._open_path = fake_open_path  # type: ignore[method-assign]

    content = await eng.cat("file://local/root/doc.md")
    assert "hello world" in content
    assert plugin.closed
    await eng.infra.meta.close()


# --- tail tests ---


@pytest.mark.parametrize("okind", ["table_rows", "record_collection", "message_stream"])
async def test_tail_rejects_unstable_structured_objects(tmp_path, okind: str) -> None:
    """tail on a structured object must raise tail_unsupported."""
    eng = await _build_engine(tmp_path)
    plugin = _FakeStructuredPlugin([])
    plugin.object_kind_of = lambda rel: okind  # type: ignore[method-assign]

    async def fake_open_path(path: str):
        return "cid", "postgres://db", "/rows.jsonl", plugin

    eng.reads._open_path = fake_open_path  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="tail_unsupported"):
        await eng.tail("postgres://db/rows.jsonl", 5)

    assert plugin.closed
    await eng.infra.meta.close()


# --- grep tests ---


async def test_grep_missing_path_raises_file_not_found(tmp_path) -> None:
    """grep on a path that doesn't exist must raise FileNotFoundError, not return empty."""
    eng = await _build_engine(tmp_path)
    plugin = _FakeTextPlugin("")

    async def fake_stat(rel):
        if rel != "/exists.txt":
            raise FileNotFoundError(rel)
        return PathStat(path=rel, type="file", media_type="text/plain", size_hint=1)

    async def fake_grep(pattern, rel, options):
        return None

    plugin.stat = fake_stat  # type: ignore[method-assign]
    plugin.grep = fake_grep  # type: ignore[method-assign]

    async def fake_open_path(path: str):
        return "cid", "file://local/root", "/does/not/exist", plugin

    eng.reads._open_path = fake_open_path  # type: ignore[method-assign]

    with pytest.raises(FileNotFoundError):
        await eng.grep("needle", "file://local/root/does/not/exist")

    assert plugin.closed
    await eng.infra.meta.close()


# --- export / head tests ---


async def test_export_returns_text_and_partial_flag(tmp_path) -> None:
    """export returns (text, partial) for a plain-text object."""
    eng = await _build_engine(tmp_path)
    plugin = _FakeTextPlugin("export content here")

    async def fake_open_path(path: str):
        return "cid", "file://local/root", "/doc.md", plugin

    eng.reads._open_path = fake_open_path  # type: ignore[method-assign]

    text, partial = await eng.export("file://local/root/doc.md")
    assert "export content" in text
    assert partial is False
    await eng.infra.meta.close()


async def test_head_returns_first_n_lines(tmp_path) -> None:
    """head returns the first n lines of a text object."""
    eng = await _build_engine(tmp_path)
    plugin = _FakeTextPlugin("line1\nline2\nline3\nline4\nline5")

    async def fake_open_path(path: str):
        return "cid", "file://local/root", "/log.txt", plugin

    eng.reads._open_path = fake_open_path  # type: ignore[method-assign]

    result = await eng.head("file://local/root/log.txt", n=3)
    assert result == "line1\nline2\nline3"
    await eng.infra.meta.close()
