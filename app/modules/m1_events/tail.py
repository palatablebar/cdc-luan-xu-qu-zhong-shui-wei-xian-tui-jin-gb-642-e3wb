"""完整前缀切分：续写中的文件只消费到「截止位置」，尾行未写完一律挂起。

判定在原始字节上做，不解码：

- 换行字节 0x0A 与引号字节 0x22 在 UTF-8 / GB18030 里都不会出现在多字节字符
  内部，所以字节级扫描对所有受支持编码安全；
- JSONL：JSON 字符串内不允许出现裸换行，最后一个 0x0A 之后就是完整前缀；
- CSV：引号内换行不是记录边界，截止位置取最后一个「引号外换行」之后；
  文件头的 `#` 注释行不参与引号状态。

切出来的待续尾行（pending tail）不解析、不隔离、不计入数据行，等上游续写
完成后由下一次运行采信——未完成尾行永远不会被当成完整事件。
"""
from __future__ import annotations

NEWLINE = 0x0A
QUOTE = 0x22
COMMENT = 0x23  # '#'


def line_complete_offset(raw: bytes) -> int:
    """JSONL 的截止字节位移：最后一个换行之后的位置；没有换行则为 0。"""
    return raw.rfind(b"\n") + 1


def csv_complete_offset(raw: bytes) -> int:
    """CSV 的截止字节位移：最后一个「引号外换行」之后的位置。

    引号内换行不是记录边界（TRAP：文件在引号字段中间被切断时，朴素切分会把
    半截记录当成完整行）；`""` 是转义引号，不改变引号状态。
    """
    in_quotes = False
    line_start = True
    last = 0
    i, n = 0, len(raw)
    while i < n:
        byte = raw[i]
        if byte == NEWLINE:
            if not in_quotes:
                last = i + 1
            line_start = True
            i += 1
            continue
        if line_start and not in_quotes and byte == COMMENT:
            newline = raw.find(b"\n", i)
            if newline == -1:
                break
            last = newline + 1
            i = newline + 1
            line_start = True
            continue
        line_start = False
        if byte == QUOTE:
            if in_quotes and i + 1 < n and raw[i + 1] == QUOTE:
                i += 2
                continue
            in_quotes = not in_quotes
        i += 1
    return last


def split_prefix(raw: bytes, *, csv_aware: bool) -> tuple[bytes, bytes]:
    """把原始字节切成 (完整前缀, 待续尾行)。"""
    offset = csv_complete_offset(raw) if csv_aware else line_complete_offset(raw)
    return raw[:offset], raw[offset:]


def preview(raw: bytes, encoding: str | None, limit: int = 80) -> str:
    """待续尾行的可读预览：坏字节替换、换行转义、截断，只用于展示。"""
    if not raw:
        return ""
    text = raw.decode(encoding or "utf-8", errors="replace")
    text = text.replace("\r", "\\r").replace("\n", "\\n")
    return text[:limit] + ("…" if len(text) > limit else "")
