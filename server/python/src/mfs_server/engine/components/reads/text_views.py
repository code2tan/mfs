"""读路径纯函数（阶段 1 迁出）。

详见 `docs/engine-redesign.md` §4.8。``density_view`` / ``locator_matches`` 原为
``engine.py`` 的模块级函数 / ``Engine`` 静态方法，无副作用、无 ``self`` 依赖，是阶段 1
最该先脱离类的纯函数。迁入此处后，``Engine`` 的 ``cat`` 路径直接导入调用。
"""

from __future__ import annotations

import re

from ...producers.render import resolve_path


_CODE_SYMBOL = re.compile(r"^\s*(def |class |func |fn |public |private |func\(|type )")


def density_view(text: str, ext: str, density: str) -> str:
    """Skeleton view of a document/code object:
    peek = headings (markdown #) or code symbol lines only;
    skim = peek + the first non-blank line of prose under each heading.
    """
    lines = text.splitlines()
    is_md = ext in (".md", ".markdown", ".rst", ".txt", "")
    out: list[str] = []
    if is_md:
        for i, ln in enumerate(lines):
            if ln.lstrip().startswith("#"):
                out.append(ln.rstrip())
                if density == "skim":
                    for nxt in lines[i + 1 :]:
                        if nxt.strip():
                            out.append("    " + nxt.strip()[:120])
                            break
    else:
        for ln in lines:
            if _CODE_SYMBOL.match(ln):
                out.append(ln.rstrip() if density == "skim" else ln.split("(")[0].rstrip())
    if not out:
        # nothing structural found -> first lines as a fallback peek
        out = [ln.rstrip() for ln in lines[:15]]
    return "\n".join(out)


def locator_matches(rec: dict, ocfg, idx: int, locator: dict) -> bool:
    """Whether structured-record ``rec`` (at row ``idx``) matches ``locator``.

    原为 ``Engine._locator_matches`` 静态方法。框架保留 ``lines`` 作为 body/code chunk
    的 locator key，绝不参与结构化记录 PK 比对；空/拼错的 locator 返回 False 而非
    误命中第 0 行（避免 ``all([]) is True`` 的陷阱）。
    """
    if "_row" in locator:
        return idx == int(locator["_row"])
    # "lines" is the framework-reserved key for body/code chunks and is never a
    # structured-record PK — never compare it against the row. The cat router
    # dispatches body-chunk reads through plugin.read(range=...) before reaching
    # this helper, so seeing it here is a misconfiguration we just ignore.
    keys = [k for k in (ocfg.locator_fields or list(locator.keys())) if k != "lines"]
    present = [k for k in keys if k in locator]
    # Require at least one recognized locator key: a locator that's empty or whose keys
    # don't correspond to this object's locator_fields matches nothing. Without this guard
    # `all([])` is True, so a bogus/typo'd locator silently returns record #0 instead of
    # the documented locator_not_found.
    if not present:
        return False
    # resolve with the SAME JSONPath-lite used to WRITE the locator (engine indexing:
    # {f: resolve_path(rec, f)}); plain rec.get() couldn't reopen a nested locator key.
    return all(str(resolve_path(rec, k)) == str(locator.get(k)) for k in present)
