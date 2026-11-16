"""M1 入口：采信一次导出（全部来源或单个文件），并给出事件流明细。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from ...web import Ctx, get_ctx, render
from ..m6_batches import service as batch_service

router = APIRouter(tags=["M1 事件采信"])


@router.post("/api/events/import")
def api_import(payload: dict | None = None, ctx: Ctx = Depends(get_ctx)) -> dict:
    body = payload or {}
    source = str(body.get("source") or "all").strip()
    # 来源可以是 all / 登记来源 / 样例目录内的文件名（续写中的导出文件）；
    # 合法性由采信层校验，未知来源统一 E_BAD_SOURCE。
    result = batch_service.run(settings=ctx.settings, store=ctx.store,
                               source=source or "all")
    return {
        "status": result["status"],
        "batch_id": result["batch"]["batch_id"],
        "batch_key": result["batch_key"],
        "run_id": result["run_id"],
        "idempotent_replay": result.get("idempotent_replay", False),
        "continued": result.get("continued", False),
        "run_seq": result.get("run_seq", 1),
        "replay_of": result.get("replay_of"),
        "note": result.get("note"),
        "elapsed_ms": result["elapsed_ms"],
        "stages": result["stages"],
        "counters": result["counters"],
        "totals": result["totals"],
        "reconciliation": result["reconciliation"],
        "all_balanced": result["all_balanced"],
        "watermark": result["watermark"],
        "cutoff": result.get("cutoff"),
        "inputs": result["inputs"],
    }


@router.get("/api/events")
def api_events(row_class: str | None = None, table: str | None = None,
               limit: int = 200, after: int = 0,
               ctx: Ctx = Depends(get_ctx)) -> dict:
    """事件流：keyset 翻页，页边界是到达序号 seq，翻页不重不漏、页页对得上。"""
    batch = ctx.current_batch()
    if batch is None:
        return {"batch_id": None, "rows": [], "count": 0, "total": 0,
                "after": 0, "next_after": None, "has_more": False}
    page = ctx.store.page_events(batch["batch_id"], row_class or None,
                                 table or None, after=after, limit=limit)
    return {
        "batch_id": batch["batch_id"],
        "count": page["count"],
        "total": page["total"],
        "after": max(0, after),
        "next_after": page["next_after"],
        "has_more": page["has_more"],
        "rows": [{
            "seq": r["seq"], "row_ref": r["row_ref"], "table": r["table_name"],
            "pk": r["pk"], "version": r["version"], "op": r["op"],
            "event_time": r["event_time_utc"], "event_time_raw": r["event_time_raw"],
            "schema": r["schema_version"], "class": r["row_class"],
            "reason_code": r["reason_code"],
            "amount": None if r["amount"] is None else str(r["amount"]),
            "late": r["late"], "detail": r["detail"],
        } for r in page["rows"]],
    }


@router.get("/events")
def page_events(request: Request, row_class: str | None = None,
                table: str | None = None, after: int = 0,
                before: int | None = None, limit: int = 200,
                ctx: Ctx = Depends(get_ctx)):
    batch = ctx.current_batch()
    page = {"rows": [], "total": 0, "count": 0, "first_seq": None,
            "last_seq": None, "next_after": None, "prev_before": None,
            "has_more": False, "has_prev": False}
    inputs: list[dict] = []
    totals: dict = {}
    table_list: list[str] = []
    if batch:
        layers = ctx.layers(batch["batch_id"])
        inputs, totals = layers["inputs"], layers["totals"]
        table_list = sorted({e["table_name"] for e in layers["events"]
                             if e["table_name"]})
        page = ctx.store.page_events(batch["batch_id"], row_class or None,
                                     table or None, after=after, before=before,
                                     limit=limit)
    return render(request, "events.html", batch=batch, page="events",
                  events=page["rows"], total_events=page["total"],
                  page_info=page, inputs=inputs, totals=totals,
                  row_class=row_class or "", table=table or "",
                  table_list=table_list,
                  page_title="事件流")
