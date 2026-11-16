"""逐文件采信：解码 → 识别格式 → 保留原始行号 → 尾行与声明条数校验。

每个数据行都保留 `文件名:行号` 的证据引用与原始文本，隔离导出直接用它复核。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path

from . import decode, format as fmt, tail
from ... import manifest


@dataclass
class RawRow:
    file: str
    lineno: int
    text: str
    raw: dict | None = None
    error: str | None = None
    note: str = ""
    seq: int = 0

    @property
    def row_ref(self) -> str:
        return f"{self.file}:{self.lineno}"


@dataclass
class FileRead:
    name: str
    size: int
    sha256: str
    kind: str
    encoding: str
    declared: str | None
    declared_rows: int | None
    status: str
    code: str | None
    codes: list[str] = field(default_factory=list)
    rows: list[RawRow] = field(default_factory=list)
    header: list[str] = field(default_factory=list)
    lines: int = 0
    blank: int = 0
    note: str = ""
    # ---- 截止位置（续写文件只消费完整前缀）----
    cutoff_offset: int = 0          # 本次采用的字节截止位置（完整前缀长度）
    cutoff_line: int = 0            # 本次采用到的物理行号（完整前缀行数）
    pending_tail_bytes: int = 0     # 待续尾行字节数（未换行，挂起不解析）
    pending_tail: str = ""          # 待续尾行预览（仅展示用）
    prefix_sha256: str = ""         # 完整前缀的 sha256，续跑时校验只追加不改写

    def as_input_row(self) -> dict:
        return {"name": self.name, "bytes": self.size, "sha256": self.sha256,
                "declared_rows": self.declared_rows, "encoding": self.encoding,
                "kind": self.kind, "status": self.status, "code": self.code,
                "note": self.note, "lines": self.lines,
                "cutoff_offset": self.cutoff_offset, "cutoff_line": self.cutoff_line,
                "pending_tail_bytes": self.pending_tail_bytes,
                "pending_tail": self.pending_tail,
                "prefix_sha256": self.prefix_sha256}


def _first_error(codes: list[str]) -> str | None:
    for code in codes:
        if code.startswith("E_"):
            return code
    return codes[0] if codes else None


def read_bytes(name: str, raw: bytes) -> FileRead:
    digest = hashlib.sha256(raw).hexdigest()
    # 续写场景：先按字节切出完整前缀，待续尾行（未换行）挂起，不参与解码与解析。
    prefix, pending = tail.split_prefix(raw, csv_aware=False)
    probe = decode.decode_bytes(prefix)
    meta = fmt.Meta()
    if not probe.failed:
        meta = fmt.split_comments(probe.text)[1]
    decoded = decode.decode_bytes(prefix, declared=meta.declared_charset)
    codes = list(decoded.codes)
    if decoded.failed:
        return FileRead(name=name, size=len(raw), sha256=digest, kind="unknown",
                        encoding="", declared=meta.declared_charset,
                        declared_rows=meta.declared_rows, status="failed",
                        code="E_DECODE_FAILED", codes=codes, note="全部编码解码失败",
                        cutoff_offset=len(prefix), pending_tail_bytes=len(pending),
                        pending_tail=tail.preview(pending, None),
                        prefix_sha256=manifest.digest_bytes(prefix))

    sniffed = fmt.sniff(decoded.text)
    if sniffed.kind == "csv":
        # CSV 引号内换行不是记录边界：截止位置收紧到最后一个引号外换行。
        refined, pending = tail.split_prefix(raw, csv_aware=True)
        if refined != prefix:
            prefix = refined
            decoded = decode.decode_bytes(prefix, declared=meta.declared_charset)
            sniffed = fmt.sniff(decoded.text)
    codes.extend(sniffed.header_codes)
    rows: list[RawRow] = []
    if sniffed.kind == "empty":
        if not pending:
            rest, _m, _c = fmt.split_comments(decoded.text)
            blanks = [ln for ln in rest.splitlines() if not ln.strip()]
            codes.append("E_ALL_BLANK" if blanks else "S_EMPTY_INPUT")
    elif sniffed.kind == "jsonl":
        rows = _read_jsonl(name, decoded.text, sniffed.comment_lines)
    else:
        rows, csv_codes = _read_csv(name, decoded.text)
        codes.extend(csv_codes)

    data_rows = [r for r in rows if r.raw is not None or r.error]
    if sniffed.kind == "csv" and not rows:
        codes.append("S_HEADER_ONLY")
    declared = meta.declared_rows
    if (declared is not None and len(rows) != declared
            and sniffed.kind != "empty" and not pending):
        # 文件已写完（无待续尾行）而行数与声明不符 → 截断；还在续写则不判。
        codes.append("E_TAIL_TRUNCATED")
    if pending:
        codes.append("S_PENDING_TAIL")
    return FileRead(
        name=name, size=len(raw), sha256=digest, kind=sniffed.kind,
        encoding=decoded.encoding, declared=decoded.declared, declared_rows=declared,
        status="ok", code=_first_error(codes), codes=_dedup(codes), rows=rows,
        header=sniffed.header, lines=len(rows), blank=_blank_lines(decoded.text),
        note=_note_for(codes, declared, len(rows), len(data_rows), len(pending)),
        cutoff_offset=len(prefix), cutoff_line=decoded.text.count("\n"),
        pending_tail_bytes=len(pending),
        pending_tail=tail.preview(pending, decoded.encoding),
        prefix_sha256=manifest.digest_bytes(prefix),
    )


def _blank_lines(text: str) -> int:
    rest, _meta, _c = fmt.split_comments(text)
    return sum(1 for line in rest.splitlines() if not line.strip())


def _dedup(codes: list[str]) -> list[str]:
    out: list[str] = []
    for code in codes:
        if code not in out:
            out.append(code)
    return out


def _note_for(codes: list[str], declared: int | None, rows: int, data: int,
              pending: int = 0) -> str:
    if "E_DECODE_FAILED" in codes:
        return "解码失败"
    parts = []
    if "E_TAIL_TRUNCATED" in codes:
        parts.append(f"声明 {declared} 行，实收 {rows} 行")
    elif "S_HEADER_ONLY" in codes:
        parts.append("仅表头")
    elif "S_EMPTY_INPUT" in codes:
        parts.append("0 行")
    elif "E_ALL_BLANK" in codes:
        parts.append("只有空行")
    else:
        parts.append(f"{data} 行")
    if pending:
        parts.append(f"待续尾行 {pending} 字节")
    return " · ".join(parts)


def _read_jsonl(name: str, text: str, comment_lines: int) -> list[RawRow]:
    rest, _meta, _c = fmt.split_comments(text)
    lines = rest.splitlines()
    rows: list[RawRow] = []
    pending: list[tuple[int, str]] = []
    for idx, line in enumerate(lines, 1):
        lineno = idx + comment_lines
        if not line.strip():
            continue
        pending.append((lineno, line))
    for pos, (lineno, line) in enumerate(pending):
        is_last = pos == len(pending) - 1
        try:
            raw = json.loads(line)
        except json.JSONDecodeError:
            code = "E_TAIL_TRUNCATED" if is_last else "E_BAD_JSON"
            rows.append(RawRow(file=name, lineno=lineno, text=line, error=code))
            continue
        if not isinstance(raw, dict):
            rows.append(RawRow(file=name, lineno=lineno, text=line, error="E_BAD_JSON"))
            continue
        rows.append(RawRow(file=name, lineno=lineno, text=line, raw=raw))
    return rows


def _read_csv(name: str, text: str) -> tuple[list[RawRow], list[str]]:
    header, records, codes = fmt.csv_records(text)
    rows: list[RawRow] = []
    width = len(header)
    for lineno, cells in records:
        if width and len(cells) != width:
            rows.append(RawRow(file=name, lineno=lineno, text=",".join(cells),
                               raw={"__cells": cells},
                               error="E_COLUMN_COUNT",
                               note=f"列数 {len(cells)}，表头 {width}"))
            continue
        raw = {header[i]: cells[i] for i in range(min(width, len(cells)))}
        rows.append(RawRow(file=name, lineno=lineno, text=",".join(cells), raw=raw))
    return rows, codes


def read_file(path: Path, name: str | None = None) -> FileRead:
    return read_bytes(name or Path(path).name, Path(path).read_bytes())
