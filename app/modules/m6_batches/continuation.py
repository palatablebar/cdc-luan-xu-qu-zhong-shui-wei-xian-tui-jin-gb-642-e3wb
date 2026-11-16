"""续跑规则：只追加校验、前后对照与截止位置小节。

「一份批次结果」的语义靠两条守住：

1. **只追加**：同一来源的文件再次采信时，上次截止位置之前的字节必须一字不差
   （sha256 前缀校验）。被改写、被截短或文件消失都按 `C_STREAM_REWRITTEN`
   拒绝——继续会丢记录或把记录算两次，宁可 409 让人换来源重跑。
2. **可对照**：每次运行记录「本次采用的截止位置」与「上次截止位置」，水位、
   已应用、原始行都给出前后值，页面与导出读同一份。
"""
from __future__ import annotations

import hashlib
from pathlib import Path

from ... import errors


def verify_append_only(prev_inputs: list[dict], current_names: set[str],
                       sample_dir: str) -> None:
    """上次采信过的每个文件：还在、没缩短、截止位置前的字节没被改写。"""
    for row in prev_inputs:
        name = row["name"]
        prev_offset = int(row.get("cutoff_offset") or 0)
        prev_sha = row.get("prefix_sha256") or ""
        if name not in current_names:
            raise errors.StreamRewrite(
                f"输入 {name} 在上次采信（截止第 {row.get('cutoff_line') or 0} 行）后消失，"
                "继续会丢记录，已拒绝",
                detail={"file": name, "reason": "missing",
                        "prev_cutoff_offset": prev_offset})
        path = Path(sample_dir) / name
        raw = path.read_bytes() if path.is_file() else b""
        if len(raw) < prev_offset:
            raise errors.StreamRewrite(
                f"输入 {name} 比上次截止位置（{prev_offset} 字节）还短，疑似被截断重写",
                detail={"file": name, "reason": "shrunk",
                        "prev_cutoff_offset": prev_offset, "bytes": len(raw)})
        if hashlib.sha256(raw[:prev_offset]).hexdigest() != prev_sha:
            raise errors.StreamRewrite(
                f"输入 {name} 在上次截止位置（第 {row.get('cutoff_line') or 0} 行 / "
                f"{prev_offset} 字节）之前的内容被改写，拒绝继续",
                detail={"file": name, "reason": "rewritten",
                        "prev_cutoff_offset": prev_offset})


def prev_cutoffs(prev_inputs: list[dict]) -> dict[str, dict]:
    """文件名 → 上次截止位置，供本次输入行填「前后」两列。"""
    return {row["name"]: {"cutoff_offset": row.get("cutoff_offset"),
                          "cutoff_line": row.get("cutoff_line")}
            for row in prev_inputs}


def cutoff_section(*, run_seq: int, continued: bool, prev_watermark,
                   watermark, prev_applied: int, applied: int,
                   prev_raw_lines: int, raw_lines: int,
                   files: list[dict]) -> dict:
    """「本次采用的截止位置及前后结果」的机器可读版，API / 页面 / 清单共用。"""
    file_rows = []
    byte_ok = True
    for item in files:
        size = int(item.get("bytes") or 0)
        offset = int(item.get("cutoff_offset") or 0)
        tail = int(item.get("pending_tail_bytes") or 0)
        ok = size == offset + tail
        byte_ok = byte_ok and ok
        file_rows.append({
            "name": item.get("name"),
            "bytes": size,
            "cutoff_offset": offset,
            "cutoff_line": int(item.get("cutoff_line") or 0),
            "prev_cutoff_offset": item.get("prev_cutoff_offset"),
            "prev_cutoff_line": item.get("prev_cutoff_line"),
            "pending_tail_bytes": tail,
            "pending_tail": item.get("pending_tail") or "",
            "byte_ok": ok,
        })
    return {
        "run_seq": run_seq,
        "continued": continued,
        "watermark": {"prev": prev_watermark, "current": watermark},
        "applied": {"prev": prev_applied, "current": applied},
        "raw_lines": {"prev": prev_raw_lines, "current": raw_lines},
        "files": file_rows,
        "byte_ok": byte_ok,
        "byte_note": "文件字节 = 截止位移 + 待续尾行字节",
    }
