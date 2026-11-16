"""截止位置与续跑：待续尾行挂起、追加续跑同批次、只追加校验、失败重试、翻页不变量。

场景：上游导出文件还在续写，早班要在九点前看到「处理到哪一行」；
太早封板不能漏掉随后追加，一直等又交不了班。
"""
from __future__ import annotations

import json
import shutil
from dataclasses import replace
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import errors
from app.main import create_app
from app.modules.m1_events import reader
from app.modules.m6_batches import service as batch_service
from fixtures import expected as X

SAMPLES = Path(__file__).resolve().parent.parent / "sample_data"
HEAD = "# cdc-export v2 rows=4 charset=utf-8\n"


def _evt(pk: str, version: int, event_time: str, amount: str) -> str:
    return ('{"table":"orders","pk":"%s","version":%d,"op":"upsert",'
            '"event_time":"%s","schema":"v2",'
            '"payload":{"status":"新建","amount":"%s"}}\n'
            % (pk, version, event_time, amount))


def _stream_settings(settings, directory: Path):
    return replace(settings, sample_dir=str(directory))


def _write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


# ---------------------------------------------------------------- 待续尾行


def test_pending_tail_held_not_quarantined(settings, store, tmp_samples):
    """末行未换行 → 挂起待续：不解析、不隔离、不计入数据行。"""
    body = HEAD + _evt("9001", 1, "2026-11-15T08:00:00Z", "10.00") \
        + _evt("9002", 1, "2026-11-15T08:05:00Z", "20.00")
    partial = '{"table":"orders","pk":"9003","version":1,"op":"ups'
    _write(tmp_samples / "stream.jsonl", body + partial)

    result = batch_service.run(settings=_stream_settings(settings, tmp_samples),
                               store=store, source="stream.jsonl")
    assert result["counters"]["applied"] == 2
    assert result["counters"]["quarantine"] == 0
    assert result["totals"]["raw_lines"] == 2
    assert result["totals"]["pending_tail_bytes"] == len(partial.encode())
    assert result["totals"]["pending_tail_files"] == 1

    item = result["inputs"][0]
    assert item["code"] == "S_PENDING_TAIL"
    assert item["cutoff_line"] == 3                 # 头 + 2 条完整行
    assert item["cutoff_offset"] == len(body.encode("utf-8"))
    assert item["pending_tail"].startswith('{"table"')
    # 声明 4 行但文件还在续写：不判 E_TAIL_TRUNCATED
    assert "E_TAIL_TRUNCATED" not in str(item.get("code"))
    # 字节账：文件字节 = 截止位移 + 待续尾行
    assert result["cutoff"]["byte_ok"] is True
    assert result["totals"]["cutoff_ok"] == 1
    events = store.load_events(result["batch"]["batch_id"])
    assert all(e["row_ref"] != "stream.jsonl:4" for e in events)


def test_whole_file_pending_is_not_empty_input(tmp_samples):
    """整文件只有半行（没有任何换行）→ 全是待续，不报空文件。"""
    partial = '{"table":"orders","pk":"9001","version":1'
    item = reader.read_bytes("half.jsonl", partial.encode("utf-8"))
    assert item.codes == ["S_PENDING_TAIL"]
    assert item.lines == 0
    assert item.cutoff_offset == 0
    assert item.pending_tail_bytes == len(partial.encode("utf-8"))


def test_declared_rows_mismatch_not_truncated_while_pending(tmp_samples):
    """声明行数不符 + 有待续尾行 = 还在写，不判截断；写完后行数对上，无码。"""
    body = HEAD + _evt("9001", 1, "2026-11-15T08:00:00Z", "10.00")
    item = reader.read_bytes("s.jsonl", (body + '{"table":"ord').encode("utf-8"))
    assert "S_PENDING_TAIL" in item.codes
    assert "E_TAIL_TRUNCATED" not in item.codes
    done = body + _evt("9002", 1, "2026-11-15T08:05:00Z", "20.00") \
        + _evt("9003", 1, "2026-11-15T08:10:00Z", "30.00") \
        + _evt("9004", 1, "2026-11-15T08:15:00Z", "40.00")
    item2 = reader.read_bytes("s.jsonl", done.encode("utf-8"))
    assert item2.lines == 4
    assert "E_TAIL_TRUNCATED" not in item2.codes
    assert "S_PENDING_TAIL" not in item2.codes


def test_half_multibyte_char_at_cut_is_pending(tmp_samples):
    """GB18030 文件在多字节字符中间被切断：前缀正常解码，半截字符挂起。"""
    head = "# cdc-export v2 charset=gb18030\n"
    line = _evt("9001", 1, "2026-11-15T08:00:00Z", "10.00")
    raw = (head + line).encode("gb18030") + '{"pk":"新'.encode("gb18030")[:-1]
    item = reader.read_bytes("gb.jsonl", raw)
    assert item.code == "S_PENDING_TAIL"
    assert "E_DECODE_FAILED" not in item.codes
    assert item.lines == 1
    assert item.pending_tail_bytes == len(raw) - item.cutoff_offset


def test_csv_cut_inside_quoted_field_is_pending(settings, store, tmp_samples):
    """CSV 引号字段内的换行不是记录边界：切在引号里也算待续，不当完整行。"""
    head = ("# cdc-export v2 rows=2 charset=utf-8\n"
            "table,pk,version,op,event_time,schema,ref_no,amt,st,qt\n")
    row1 = "orders,5001,1,upsert,2026-11-15T08:00:00Z,v2,,10.00,新建,\n"
    # 第二条记录的 st 字段带引号内换行，文件恰好在引号内换行后被切断
    row2_part = 'orders,5002,1,upsert,2026-11-15T08:05:00Z,v2,,12.00,"新\n'
    st = _stream_settings(settings, tmp_samples)
    _write(tmp_samples / "q.csv", head + row1 + row2_part)

    first = batch_service.run(settings=st, store=store, source="q.csv")
    assert first["counters"]["applied"] == 1
    assert first["counters"]["quarantine"] == 0     # 半截记录没被当成坏行
    item = first["inputs"][0]
    assert item["code"] == "S_PENDING_TAIL"
    assert item["cutoff_line"] == 3                 # 头 + 表头 + 1 条完整记录

    # 续写补完引号字段：状态是「新\n建」的一条完整记录
    _write(tmp_samples / "q.csv", head + row1 + row2_part + '建",\n')
    second = batch_service.run(settings=st, store=store, source="q.csv")
    assert second["continued"] is True
    assert second["counters"]["applied"] == 2
    assert second["counters"]["quarantine"] == 0
    applied = [e for e in store.load_events(first["batch"]["batch_id"], "applied")]
    assert {e["pk"] for e in applied} == {"5001", "5002"}


# ---------------------------------------------------------------- 追加续跑


def _seed_stream(tmp_samples: Path) -> tuple[str, str, str, str]:
    lines = (_evt("9001", 1, "2026-11-15T08:00:00Z", "10.00"),
             _evt("9002", 1, "2026-11-15T08:05:00Z", "20.00"),
             _evt("9003", 1, "2026-11-15T08:10:00Z", "30.00"),
             _evt("9004", 1, "2026-11-15T08:20:00Z", "40.00"))
    return lines


def test_append_continues_same_batch_exactly_once(settings, store, tmp_samples):
    """九点封板后上游继续追加：同一批次续跑，完整记录只进一次，前后对照可查。"""
    st = _stream_settings(settings, tmp_samples)
    l1, l2, l3, l4 = _seed_stream(tmp_samples)
    target = tmp_samples / "stream.jsonl"
    _write(target, HEAD + l1 + l2 + l3[:-12])       # l3 只有半截

    first = batch_service.run(settings=st, store=store, source="stream.jsonl")
    batch_id = first["batch"]["batch_id"]
    assert first["counters"]["applied"] == 2
    assert first["run_seq"] == 1
    assert first["continued"] is False
    first_cutoff = first["cutoff"]["files"][0]["cutoff_line"]
    seq_before = {e["row_ref"]: e["seq"]
                  for e in store.load_events(batch_id)}

    _write(target, HEAD + l1 + l2 + l3 + l4)        # 补完 l3 并追加 l4
    second = batch_service.run(settings=st, store=store, source="stream.jsonl")
    assert second["batch"]["batch_id"] == batch_id  # 一份批次结果
    assert second["continued"] is True
    assert second["run_seq"] == 2
    assert second["note"]
    assert second["counters"]["applied"] == 4
    assert len(store.list_batches()) == 1

    events = store.load_events(batch_id)
    refs = [e["row_ref"] for e in events]
    assert len(refs) == len(set(refs))              # 完整记录只进入一次
    assert refs.count("stream.jsonl:4") == 1        # 补完的尾行只进一次
    seq_after = {e["row_ref"]: e["seq"] for e in events}
    for ref, seq in seq_before.items():             # 已有记录序号不动，翻页仍对得上
        assert seq_after[ref] == seq

    cutoff = second["cutoff"]                       # 本次采用的截止位置及前后结果
    assert cutoff["watermark"]["prev"] == first["watermark"]["final"]
    assert cutoff["watermark"]["current"] == second["watermark"]["final"]
    assert cutoff["applied"] == {"prev": 2, "current": 4}
    assert cutoff["raw_lines"] == {"prev": 2, "current": 4}
    f0 = cutoff["files"][0]
    assert f0["prev_cutoff_line"] == first_cutoff
    assert f0["cutoff_line"] == 5                   # 头 + 4 条完整行
    assert f0["pending_tail_bytes"] == 0
    assert cutoff["byte_ok"] is True

    totals = store.load_totals(batch_id)
    assert totals["run_seq"] == "2"
    assert totals["prev_applied"] == "2"
    assert totals["prev_watermark_utc"] == first["watermark"]["final"]

    third = batch_service.run(settings=st, store=store, source="stream.jsonl")
    assert third["idempotent_replay"] is True       # 输入没变：幂等重放
    assert third["run_seq"] == 2
    assert store.count_rows(batch_id)["events"] == 4


def test_retry_after_failure_leaves_no_duplicates(settings, store, tmp_samples,
                                                  monkeypatch):
    """落库中途失败：什么都不留；重试后结果精确、不重复、不丢行。"""
    st = _stream_settings(settings, tmp_samples)
    l1, l2, l3, l4 = _seed_stream(tmp_samples)
    _write(tmp_samples / "stream.jsonl", HEAD + l1 + l2 + l3 + l4)

    real_save = store.save_run
    calls = {"n": 0}

    def boom(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("模拟落库失败")
        return real_save(**kwargs)

    monkeypatch.setattr(store, "save_run", boom)
    with pytest.raises(RuntimeError):
        batch_service.run(settings=st, store=store, source="stream.jsonl")
    assert store.list_batches() == []               # 失败不留半批数据

    result = batch_service.run(settings=st, store=store, source="stream.jsonl")
    assert result["status"] == "done"
    assert result["counters"]["applied"] == 4
    assert len(store.list_batches()) == 1
    events = store.load_events(result["batch"]["batch_id"])
    assert len(events) == 4
    assert len({e["row_ref"] for e in events}) == 4

    again = batch_service.run(settings=st, store=store, source="stream.jsonl")
    assert again["idempotent_replay"] is True       # 重试成功后再跑仍幂等
    assert store.count_rows(result["batch"]["batch_id"])["events"] == 4


# ---------------------------------------------------------------- 只追加校验


def test_prefix_rewrite_rejected(settings, store, tmp_samples):
    """已采信前缀被改写 → 409，已落库结果不动。"""
    st = _stream_settings(settings, tmp_samples)
    l1, l2, l3, _ = _seed_stream(tmp_samples)
    target = tmp_samples / "stream.jsonl"
    _write(target, HEAD + l1 + l2 + l3)
    first = batch_service.run(settings=st, store=store, source="stream.jsonl")
    batch_id = first["batch"]["batch_id"]

    _write(target, HEAD + l1.replace("10.00", "99.99") + l2 + l3)
    with pytest.raises(errors.StreamRewrite) as info:
        batch_service.run(settings=st, store=store, source="stream.jsonl")
    assert info.value.status_code == 409
    assert info.value.code == "C_STREAM_REWRITTEN"
    assert info.value.detail["reason"] == "rewritten"
    assert len(store.list_batches()) == 1
    assert store.load_totals(batch_id)["applied"] == "3"


def test_shrunk_input_rejected(settings, store, tmp_samples):
    """文件比上次截止位置还短 → 409。"""
    st = _stream_settings(settings, tmp_samples)
    l1, l2, l3, _ = _seed_stream(tmp_samples)
    target = tmp_samples / "stream.jsonl"
    _write(target, HEAD + l1 + l2 + l3)
    batch_service.run(settings=st, store=store, source="stream.jsonl")

    _write(target, HEAD + l1)                       # 截短
    with pytest.raises(errors.StreamRewrite) as info:
        batch_service.run(settings=st, store=store, source="stream.jsonl")
    assert info.value.detail["reason"] == "shrunk"


def test_missing_input_file_rejected(settings, store, tmp_samples):
    """上次采信过的文件消失 → 409，不能静默丢记录。"""
    st = _stream_settings(settings, tmp_samples)
    for name in ("events_main.jsonl", "events_v1_gb.csv", "events_late.jsonl"):
        shutil.copy(SAMPLES / name, tmp_samples / name)
    first = batch_service.run(settings=st, store=store, source="all")
    assert first["status"] == "done"

    (tmp_samples / "events_late.jsonl").unlink()
    with pytest.raises(errors.StreamRewrite) as info:
        batch_service.run(settings=st, store=store, source="all")
    assert info.value.detail["reason"] == "missing"
    assert info.value.detail["file"] == "events_late.jsonl"


def test_rewrite_rejected_via_api(settings, tmp_samples):
    """API 面：改写前缀 → 409 C_STREAM_REWRITTEN。"""
    st = _stream_settings(settings, tmp_samples)
    l1, l2, l3, _ = _seed_stream(tmp_samples)
    _write(tmp_samples / "stream.jsonl", HEAD + l1 + l2 + l3)
    app = create_app(st, seed=True)
    with TestClient(app) as client:
        first = client.post("/api/events/import", json={"source": "stream.jsonl"})
        assert first.status_code == 200
        _write(tmp_samples / "stream.jsonl", HEAD + l2 + l1 + l3)   # 换序=改写
        response = client.post("/api/events/import",
                               json={"source": "stream.jsonl"})
        assert response.status_code == 409
        assert response.json()["error"] == "C_STREAM_REWRITTEN"


# ---------------------------------------------------------------- 翻页


def test_events_api_keyset_pagination_covers_all(client):
    """翻页不变量：页与页首尾相接，拼起来恰好是完整有序集合。"""
    seen: list[int] = []
    after = 0
    pages = 0
    while True:
        data = client.get(f"/api/events?limit=7&after={after}").json()
        assert data["total"] == X.RAW_LINES
        seen.extend(row["seq"] for row in data["rows"])
        if not data["has_more"]:
            break
        after = data["next_after"]
        pages += 1
        assert pages < 50
    assert seen == list(range(1, X.RAW_LINES + 1))  # 不重不漏
    assert pages == X.RAW_LINES // 7 - 1 + (1 if X.RAW_LINES % 7 else 0)


def test_events_api_pagination_with_filter(client):
    seen: list[int] = []
    after = 0
    while True:
        data = client.get(f"/api/events?row_class=applied&limit=5&after={after}").json()
        assert data["total"] == X.APPLIED
        seen.extend(row["seq"] for row in data["rows"])
        if not data["has_more"]:
            break
        after = data["next_after"]
    full = client.get("/api/events?row_class=applied&limit=2000").json()
    assert seen == [row["seq"] for row in full["rows"]]
    assert len(seen) == X.APPLIED


def test_events_page_before_cursor(settings, store, run, batch_id):
    page = store.page_events(batch_id, after=7, limit=7)
    assert [r["seq"] for r in page["rows"]] == list(range(8, 15))
    assert page["has_prev"] is True

    back = store.page_events(batch_id, before=8, limit=7)
    assert [r["seq"] for r in back["rows"]] == list(range(1, 8))
    assert back["has_prev"] is False

    deep = store.page_events(batch_id, before=15, limit=7)
    assert [r["seq"] for r in deep["rows"]] == list(range(8, 15))
    assert deep["has_prev"] is True


def test_events_page_shows_range_and_pager(client):
    page = client.get("/events?limit=7")
    assert page.status_code == 200
    assert "第 1–7 行 / 共 42 行" in page.text
    assert "下一页" in page.text
    page2 = client.get("/events?limit=7&after=7")
    assert "第 8–14 行 / 共 42 行" in page2.text
    assert "上一页" in page2.text


# ---------------------------------------------------------------- 导出与页面


def test_cutoff_in_manifest_and_export(settings, tmp_samples):
    """清单与 cutoffs.csv 都带本次采用的截止位置及前后结果。"""
    st = _stream_settings(settings, tmp_samples)
    l1, l2, l3, l4 = _seed_stream(tmp_samples)
    target = tmp_samples / "stream.jsonl"
    _write(target, HEAD + l1 + l2 + l3[:-12])
    app = create_app(st, seed=True)
    with TestClient(app) as client:
        first = client.post("/api/events/import", json={"source": "stream.jsonl"}).json()
        assert first["run_seq"] == 1
        _write(target, HEAD + l1 + l2 + l3 + l4)
        run = client.post("/api/events/import", json={"source": "stream.jsonl"}).json()
        assert run["continued"] is True
        assert run["cutoff"]["run_seq"] == 2

        manifest = json.loads(client.get("/api/export/manifest.json").content)
        cutoff = manifest["cutoff"]
        assert cutoff["run_seq"] == 2
        assert cutoff["byte_ok"] is True
        assert cutoff["applied"] == {"prev": 2, "current": 4}
        f0 = cutoff["files"][0]
        assert f0["prev_cutoff_line"] == 3
        assert f0["cutoff_line"] == 5
        assert manifest["inputs"][0]["cutoff_offset"] == f0["cutoff_offset"]

        csv_text = client.get("/api/export/cutoffs.csv").content.decode("utf-8-sig")
        assert "上次截止行" in csv_text
        assert "本次截止行" in csv_text
        assert "stream.jsonl" in csv_text
        assert "平" in csv_text

        detail = client.get(f"/api/batches/{run['batch_id']}").json()
        assert detail["counters"]["run_seq"] == "2"
        assert detail["counters"]["prev_applied"] == "2"


def test_pages_show_cutoff_and_pending_tail(settings, tmp_samples):
    """批次页与事件页都显示截止位置、前后对照与待续尾行。"""
    st = _stream_settings(settings, tmp_samples)
    l1, l2, l3, _ = _seed_stream(tmp_samples)
    _write(tmp_samples / "stream.jsonl", HEAD + l1 + l2 + l3[:-12])
    app = create_app(st, seed=True)
    with TestClient(app) as client:
        batches = client.get("/batches")
        assert batches.status_code == 200
        assert "截止位置" in batches.text
        assert "待续尾行" in batches.text
        assert "字节账" in batches.text

        events = client.get("/events")
        assert events.status_code == 200
        assert "待续尾行" in events.text
        assert "截止行" in events.text
        assert "页边界按到达序号" in events.text
