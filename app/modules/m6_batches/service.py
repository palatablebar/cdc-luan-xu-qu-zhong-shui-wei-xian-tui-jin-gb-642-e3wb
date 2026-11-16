"""批次编排：采信 → 契约 → 去重冲突 → 水位迟到 → 快照 → 落库与对账。

一次运行的所有计数、金额与身份都从这里产生，页面与导出读同一份落库结果。
"""
from __future__ import annotations

import datetime as dt
import json
import time
from decimal import Decimal

from ... import config, manifest as manifest_mod, reasons
from ..m1_events import service as m1_service
from ..m2_schema import service as m2_service
from ..m3_dedup import service as m3_service
from ..m4_watermark import watermark as wm_mod
from ..m5_snapshot import materialize, service as m5_service
from ..m5_snapshot import apply as apply_mod
from . import continuation, history, resume, state


COUNT_KEYS = ("files", "raw_lines", "blank_lines", "header_repeat", "applied",
              "duplicates", "conflicts", "quarantine", "late_events",
              "on_time_events", "max_behind_minutes", "keys_total",
              "snapshot_keys", "active_keys", "tombstone_keys",
              "pending_only_keys", "pending_events", "drift_keys",
              "run_seq", "prev_applied", "prev_raw_lines",
              "pending_tail_bytes", "pending_tail_files", "cutoff_ok")
AMOUNT_KEYS = ("amount_snapshot", "amount_pending", "amount_drift", "amount_applied")
TIME_KEYS = ("watermark_utc", "max_event_time", "snapshot_as_of")


def public_totals(totals: dict) -> dict:
    """对外计数用整数、金额用两位字符串，保证首跑与重放同一个形状。"""
    out = {}
    for key, value in totals.items():
        if key == "reconcile_json":
            continue
        if key in COUNT_KEYS:
            out[key] = int(value or 0)
        elif key in AMOUNT_KEYS:
            out[key] = f"{Decimal(str(value or '0.00')):.2f}"
        elif key in TIME_KEYS:
            out[key] = value
        else:
            out[key] = value
    return out


def _stage(name: str, *, in_count: int, out_count: int, quarantine: int = 0,
           elapsed_ms: int = 0, status: str = "ok", note: str = "") -> dict:
    return {"stage": name, "in_count": in_count, "out_count": out_count,
            "quarantine_count": quarantine, "status": status,
            "elapsed_ms": elapsed_ms, "note": note}


def _now_iso() -> str:
    return dt.datetime.now(config.UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _ms(start: float) -> int:
    return int((time.perf_counter() - start) * 1000)


def run(*, settings, store, source: str = "all", params: dict | None = None,
        resume_of: str | None = None) -> dict:
    started_at = _now_iso()
    clock = time.perf_counter()
    run_id = manifest_mod.new_run_id()
    params = state.normalize_params(params)
    stages: list[dict] = []

    # ---- M1 采信 ----
    step = time.perf_counter()
    bundle = m1_service.ingest(settings, source=source)
    counts = bundle.counters
    row_errors = sum(1 for r in bundle.rows if r.error)
    stages.append(_stage("ingest", in_count=counts["files"],
                         out_count=counts["data_lines"], quarantine=row_errors,
                         elapsed_ms=_ms(step),
                         note=f"{counts['files']} 个文件 / 空行 {counts['blank_lines']}"))

    key = state.batch_key(source or "all", settings.rule_version, params)
    batch_id = state.batch_id_for(key)
    existing = store.find_done_by_key(key)
    continued = False
    prev_batch: dict = {}
    prev_inputs: list[dict] = []
    prev_totals: dict = {}
    if existing is not None:
        if existing["input_digest"] == bundle.input_digest:
            # 输入一字未变：幂等重放，复用已落库结果，不新增行。
            return _replay_payload(store, {"batch_id": existing["batch_id"]},
                                   key, run_id, stages)
        # 输入有变化：只允许「追加」。截止位置前的字节被改写/截短/文件消失 → 409。
        prev_inputs = store.load_inputs(batch_id)
        continuation.verify_append_only(
            prev_inputs, {f.name for f in bundle.files}, settings.sample_dir)
        continued = True
        prev_batch = existing
        prev_totals = store.load_totals(batch_id)

    # ---- M2 契约 ----
    step = time.perf_counter()
    norms = m2_service.normalize_rows(bundle.rows)
    bad = [n for n in norms if n.errors]
    stages.append(_stage("contract", in_count=len(norms), out_count=len(norms) - len(bad),
                         quarantine=len(bad), elapsed_ms=_ms(step),
                         note="别名映射 + 类型归一 + UTC"))

    # ---- M3 去重冲突 ----
    step = time.perf_counter()
    classified = m3_service.classify(norms)
    stages.append(_stage("dedup", in_count=len(norms) - len(bad),
                         out_count=len(classified.applied),
                         quarantine=len(classified.duplicates) + len(classified.conflicts),
                         elapsed_ms=_ms(step),
                         note=f"重复 {len(classified.duplicates)} / 冲突 "
                              f"{len(classified.conflicts)}"))

    # ---- M4 水位与迟到（先打迟到标记，再折叠成应用事件） ----
    step = time.perf_counter()
    marks = wm_mod.advance(classified.applied, settings.late_window)
    applied = [apply_mod.Applied.from_norm(n) for n in classified.applied]
    iso = lambda value: _iso(value)  # noqa: E731
    stages.append(_stage("watermark", in_count=len(applied), out_count=len(applied),
                         quarantine=0, elapsed_ms=_ms(step),
                         note=f"迟到 {marks.late} / 窗口 {settings.late_window_minutes} 分钟"))

    max_event = _iso(marks.max_event_time)
    resume.ensure_not_stale(store, max_event)

    # ---- M5 快照 ----
    step = time.perf_counter()
    snapshot = materialize.materialize(applied, marks.final)
    rows_ok = (counts["data_lines"]
               == len(classified.applied) + len(classified.duplicates)
               + len(classified.conflicts) + len(classified.quarantine))
    reconcile = [
        {"name": "行账", "kind": "count", "left": "原始行", "left_value": counts["data_lines"],
         "right": "已应用 + 重复 + 冲突 + 隔离",
         "right_value": (len(classified.applied) + len(classified.duplicates)
                         + len(classified.conflicts) + len(classified.quarantine)),
         "ok": rows_ok},
        *materialize.reconcile(snapshot),
    ]
    stages.append(_stage("snapshot", in_count=len(applied),
                         out_count=len(snapshot.rows),
                         quarantine=snapshot.counters["pending_only_keys"],
                         elapsed_ms=_ms(step),
                         note=f"活跃 {snapshot.counters['active_keys']} / 墓碑 "
                              f"{snapshot.counters['tombstone_keys']}"))

    run_seq = (int(prev_totals.get("run_seq", 1) or 1) + 1) if continued else 1
    totals = {
        "files": counts["files"],
        "raw_lines": counts["data_lines"],
        "blank_lines": counts["blank_lines"],
        "header_repeat": counts["header_repeat"],
        "applied": len(classified.applied),
        "duplicates": len(classified.duplicates),
        "conflicts": len(classified.conflicts),
        "quarantine": len(classified.quarantine),
        "late_events": marks.late,
        "on_time_events": marks.on_time,
        "max_behind_minutes": marks.max_behind_seconds // 60,
        "keys_total": snapshot.counters["keys_total"],
        "snapshot_keys": snapshot.counters["snapshot_keys"],
        "active_keys": snapshot.counters["active_keys"],
        "tombstone_keys": snapshot.counters["tombstone_keys"],
        "pending_only_keys": snapshot.counters["pending_only_keys"],
        "pending_events": snapshot.counters["pending_events"],
        "drift_keys": snapshot.counters["drift_keys"],
        "snapshot_as_of": _iso(marks.final),
        "amount_snapshot": snapshot.amounts["snapshot"],
        "amount_pending": snapshot.amounts["pending"],
        "amount_drift": snapshot.amounts["drift"],
        "amount_applied": snapshot.amounts["applied"],
        "watermark_utc": _iso(marks.final),
        "max_event_time": max_event,
        "reconcile_ok": 1 if all(r["ok"] for r in reconcile) else 0,
        "reconcile_json": json.dumps(_jsonable(reconcile), ensure_ascii=False),
        # ---- 截止位置与前后对照（续跑场景）----
        "run_seq": run_seq,
        "prev_watermark_utc": prev_batch.get("watermark_utc") if continued else None,
        "prev_max_event_time": prev_batch.get("max_event_time") if continued else None,
        "prev_applied": int(prev_totals.get("applied", 0) or 0) if continued else 0,
        "prev_raw_lines": int(prev_totals.get("raw_lines", 0) or 0) if continued else 0,
        "pending_tail_bytes": counts["pending_tail_bytes"],
        "pending_tail_files": counts["pending_tail_files"],
    }

    prev_by_name = continuation.prev_cutoffs(prev_inputs)
    input_rows = []
    for f in bundle.files:
        row = _input_row(f)
        prev = prev_by_name.get(f.name) or {}
        row["prev_cutoff_offset"] = prev.get("cutoff_offset")
        row["prev_cutoff_line"] = prev.get("cutoff_line")
        input_rows.append(row)
    cutoff = continuation.cutoff_section(
        run_seq=run_seq, continued=continued,
        prev_watermark=totals["prev_watermark_utc"], watermark=_iso(marks.final),
        prev_applied=totals["prev_applied"], applied=len(classified.applied),
        prev_raw_lines=totals["prev_raw_lines"], raw_lines=counts["data_lines"],
        files=input_rows)
    totals["cutoff_ok"] = 1 if cutoff["byte_ok"] else 0

    finished_at = _now_iso()
    elapsed = _ms(clock)
    stages.append(_stage("export", in_count=len(snapshot.rows),
                         out_count=len(snapshot.rows), quarantine=0, elapsed_ms=0,
                         note="证据行 / 隔离 / 冲突 / 快照 / 清单"))

    batch = {
        "batch_id": batch_id, "batch_key": key, "run_id": run_id,
        "source": bundle.source, "status": "done", "created_at": started_at,
        "finished_at": finished_at, "input_digest": bundle.input_digest,
        "watermark_utc": _iso(marks.final), "max_event_time": max_event,
        "params": params, "note": _batch_note(resume_of, marks.late, run_seq,
                                              continued),
        "replay_of": resume_of,
    }
    doc = manifest_mod.build(
        run_id=run_id, batch_id=batch_id, batch_key=key, settings=settings,
        source=bundle.source, input_digest=bundle.input_digest,
        inputs=input_rows, params=params,
        started_at=started_at, finished_at=finished_at, elapsed_ms=elapsed,
        stages=[{"stage": s["stage"], "in": s["in_count"], "out": s["out_count"],
                 "quarantine": s["quarantine_count"], "status": s["status"]}
                for s in stages],
        counters={k: str(v) for k, v in totals.items() if k != "reconcile_json"},
        watermark=_iso(marks.final) or "", max_event_time=max_event or "",
        reconcile=_jsonable(reconcile), cutoff=cutoff)

    snapshot_rows = [{**row, "amount": None if row["amount"] is None else str(row["amount"])}
                     for row in snapshot.rows]
    store.save_run(
        batch=batch,
        inputs=input_rows,
        events=[n.to_row() for n in classified.all_rows],
        snapshots=snapshot_rows,
        watermarks=wm_mod.series_rows(marks, iso),
        stages=stages, totals=totals, manifest=doc)

    return {
        "status": "done",
        "batch": batch,
        "batch_key": key,
        "run_id": run_id,
        "idempotent_replay": False,
        "continued": continued,
        "run_seq": run_seq,
        "note": reasons.label("B_CONTINUED") if continued else None,
        "replay_of": resume_of,
        "stages": [{**s, "label": config.STAGE_LABELS.get(s["stage"], s["stage"])}
                   for s in stages],
        "totals": public_totals(totals),
        "counters": classified.counters,
        "reconciliation": _jsonable(reconcile),
        "all_balanced": all(r["ok"] for r in reconcile),
        "watermark": {
            "final": _iso(marks.final), "max_event_time": max_event,
            "late": marks.late, "on_time": marks.on_time,
            "window_minutes": settings.late_window_minutes,
            "max_behind_minutes": marks.max_behind_seconds // 60,
        },
        "cutoff": cutoff,
        "inputs": input_rows,
        "elapsed_ms": elapsed,
    }


def _replay_payload(store, replay: dict, key: str, run_id: str,
                    stages: list[dict]) -> dict:
    batch = store.get_batch(replay["batch_id"]) or {}
    totals = store.load_totals(replay["batch_id"])
    reconcile = json.loads(totals.get("reconcile_json") or "[]")
    inputs = store.load_inputs(replay["batch_id"])
    run_seq = int(totals.get("run_seq", 1) or 1)
    cutoff = continuation.cutoff_section(
        run_seq=run_seq, continued=False,
        prev_watermark=batch.get("watermark_utc"),
        watermark=batch.get("watermark_utc"),
        prev_applied=int(totals.get("applied", 0) or 0),
        applied=int(totals.get("applied", 0) or 0),
        prev_raw_lines=int(totals.get("raw_lines", 0) or 0),
        raw_lines=int(totals.get("raw_lines", 0) or 0),
        files=inputs)
    return {
        "status": "done",
        "batch": batch,
        "batch_key": key,
        "run_id": batch.get("run_id", run_id),
        "idempotent_replay": True,
        "continued": False,
        "run_seq": run_seq,
        "replay_of": replay["batch_id"],
        "note": reasons.label("B_IDEMPOTENT_REPLAY"),
        "stages": store.load_stages(replay["batch_id"]),
        "totals": public_totals(totals),
        "counters": {
            "applied": int(totals.get("applied", 0) or 0),
            "duplicates": int(totals.get("duplicates", 0) or 0),
            "conflicts": int(totals.get("conflicts", 0) or 0),
            "quarantine": int(totals.get("quarantine", 0) or 0),
            "total": int(totals.get("raw_lines", 0) or 0),
        },
        "reconciliation": reconcile,
        "all_balanced": totals.get("reconcile_ok") == "1",
        "watermark": {"final": batch.get("watermark_utc"),
                      "max_event_time": batch.get("max_event_time"),
                      "late": int(totals.get("late_events", 0) or 0),
                      "on_time": int(totals.get("on_time_events", 0) or 0)},
        "cutoff": cutoff,
        "inputs": inputs,
        "elapsed_ms": 0,
    }


def resume_batch(*, settings, store, batch_id: str) -> dict:
    plan = resume.plan(store, batch_id)
    batch = store.get_batch(batch_id) or {}
    params = json.loads(batch.get("params_json") or "{}")
    result = run(settings=settings, store=store, source=batch.get("source") or "all",
                 params=params, resume_of=batch_id)
    result["resume_plan"] = plan
    result["resume_note"] = resume.resume_note(plan)
    return result


def _input_row(item) -> dict:
    return {"name": item.name, "bytes": item.size, "sha256": item.sha256,
            "lines": item.lines, "encoding": item.encoding,
            "declared": item.declared, "kind": item.kind,
            "declared_rows": item.declared_rows, "status": item.status,
            "code": item.code, "note": item.note,
            "cutoff_offset": item.cutoff_offset, "cutoff_line": item.cutoff_line,
            "pending_tail_bytes": item.pending_tail_bytes,
            "pending_tail": item.pending_tail,
            "prefix_sha256": item.prefix_sha256}


def _batch_note(resume_of: str | None, late: int, run_seq: int = 1,
                continued: bool = False) -> str:
    parts = []
    if continued:
        parts.append(f"续跑 #{run_seq}")
    elif resume_of:
        parts.append("续跑")
    if late:
        parts.append(f"迟到 {late}")
    return " · ".join(parts)


def _iso(value) -> str | None:
    if value is None:
        return None
    return value.astimezone(config.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _jsonable(rows: list[dict]) -> list[dict]:
    out = []
    for row in rows:
        item = {}
        for key, value in row.items():
            if isinstance(value, Decimal):
                item[key] = str(value)
            else:
                item[key] = value
        out.append(item)
    return out


def snapshot_payload(store, batch_id: str) -> dict:
    return {"summary": m5_service.summarize(store, batch_id),
            "rows": m5_service.page_rows(store, batch_id),
            "pending": m5_service.pending_rows(store, batch_id)}


def batches_payload(store, limit: int = 30) -> dict:
    return {"batches": history.rows(store, limit=limit),
            "count": len(history.rows(store, limit=limit))}
