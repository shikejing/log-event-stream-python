"""HTTP / WebSocket 接口。

- REST：来源增删与注入、订阅创建/seek/ack、事件长轮询拉取、运行状态快照。
- WebSocket：订阅流实时推送；客户端发送 {"type": "ack"|"seek", "offsets": {...}}。
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .models import (
    Event,
    Filter,
    Gap,
    InvalidSeek,
    OffsetOutOfRange,
    OffsetUnavailable,
    SourceExists,
    SourceStopped,
    UnknownSource,
    UnknownSubscription,
)
from .service import Service


# ---------------------------------------------------------------------------
# 请求模型
# ---------------------------------------------------------------------------


class FilterModel(BaseModel):
    sources: Optional[List[str]] = None
    keyword: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None


class CreateSourceRequest(BaseModel):
    id: Optional[str] = None
    type: str
    path: Optional[str] = None


class InjectRequest(BaseModel):
    payload: str
    timestamp: Optional[float] = None


class CreateSubscriptionRequest(BaseModel):
    id: Optional[str] = None
    filter: Optional[FilterModel] = None
    offsets: Optional[Dict[str, int]] = None
    queue_maxsize: Optional[int] = None


class OffsetsRequest(BaseModel):
    offsets: Dict[str, int]


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------


def item_to_dict(item: Any) -> Dict[str, Any]:
    if isinstance(item, Gap):
        return item.to_dict()
    if isinstance(item, Event):
        return item.to_dict()
    raise TypeError(f"unknown item type: {type(item)}")


def _filter_from_model(model: Optional[FilterModel], case_sensitive: bool) -> Filter:
    if model is None:
        return Filter(case_sensitive=case_sensitive)
    return Filter(
        sources=set(model.sources) if model.sources else None,
        keyword=model.keyword,
        start_time=model.start_time,
        end_time=model.end_time,
        case_sensitive=case_sensitive,
    )


def _status_for(exc: Exception) -> int:
    if isinstance(exc, (UnknownSource, UnknownSubscription)):
        return 404
    if isinstance(exc, SourceExists):
        return 409
    if isinstance(exc, SourceStopped):
        return 409
    if isinstance(exc, (OffsetOutOfRange, OffsetUnavailable, InvalidSeek)):
        return 400
    return 500


# ---------------------------------------------------------------------------
# 应用工厂
# ---------------------------------------------------------------------------


def create_app(service: Service) -> FastAPI:
    app = FastAPI(title="log-event-stream", version="0.1.0")

    @app.exception_handler(Exception)
    async def _unhandled(request, exc):  # pragma: no cover - 兜底
        return JSONResponse(status_code=500, content={"error": str(exc)})

    # -- 健康检查 -----------------------------------------------------------

    @app.get("/healthz")
    async def healthz() -> Dict[str, str]:
        return {"status": "ok"}

    # -- 来源 ---------------------------------------------------------------

    @app.post("/api/sources", status_code=201)
    async def create_source(req: CreateSourceRequest) -> Dict[str, Any]:
        try:
            source = service.add_source(req.type, source_id=req.id, path=req.path)
        except (SourceExists, ValueError) as exc:
            raise HTTPException(status_code=_status_for(exc), detail=str(exc))
        return {"source": service.source_snapshot(source.source_id)}

    @app.delete("/api/sources/{source_id}")
    async def remove_source(source_id: str) -> Dict[str, Any]:
        try:
            await service.remove_source(source_id)
        except UnknownSource as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        return {"removed": source_id}

    @app.get("/api/sources")
    async def list_sources() -> Dict[str, Any]:
        snap = service.snapshot()
        return {"sources": list(snap["sources"].values())}

    @app.get("/api/sources/{source_id}")
    async def get_source(source_id: str) -> Dict[str, Any]:
        try:
            return {"source": service.source_snapshot(source_id)}
        except UnknownSource as exc:
            raise HTTPException(status_code=404, detail=str(exc))

    @app.post("/api/sources/{source_id}/inject")
    async def inject(source_id: str, req: InjectRequest) -> Dict[str, Any]:
        try:
            offset = await service.inject(source_id, req.payload, req.timestamp)
        except (UnknownSource, SourceStopped) as exc:
            raise HTTPException(status_code=_status_for(exc), detail=str(exc))
        return {"source_id": source_id, "offset": offset}

    # -- 订阅 ---------------------------------------------------------------

    @app.post("/api/subscriptions", status_code=201)
    async def create_subscription(req: CreateSubscriptionRequest) -> Dict[str, Any]:
        flt = _filter_from_model(req.filter, service.config.keyword_case_sensitive)
        try:
            sub = service.create_subscription(
                flt=flt, sub_id=req.id, offsets=req.offsets, queue_maxsize=req.queue_maxsize
            )
        except (SourceExists, OffsetOutOfRange, OffsetUnavailable, InvalidSeek) as exc:
            raise HTTPException(status_code=_status_for(exc), detail=str(exc))
        return {"subscription": service.subscription_snapshot(sub.id)}

    @app.get("/api/subscriptions")
    async def list_subscriptions() -> Dict[str, Any]:
        snap = service.snapshot()
        return {"subscriptions": list(snap["subscriptions"].values())}

    @app.get("/api/subscriptions/{sub_id}")
    async def get_subscription(sub_id: str) -> Dict[str, Any]:
        try:
            return {"subscription": service.subscription_snapshot(sub_id)}
        except UnknownSubscription as exc:
            raise HTTPException(status_code=404, detail=str(exc))

    @app.delete("/api/subscriptions/{sub_id}")
    async def delete_subscription(sub_id: str) -> Dict[str, Any]:
        try:
            service.remove_subscription(sub_id)
        except UnknownSubscription as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        return {"removed": sub_id}

    @app.post("/api/subscriptions/{sub_id}/seek")
    async def seek(sub_id: str, req: OffsetsRequest) -> Dict[str, Any]:
        try:
            service.seek(sub_id, req.offsets)
        except (UnknownSubscription, UnknownSource, OffsetOutOfRange, OffsetUnavailable, InvalidSeek) as exc:
            raise HTTPException(status_code=_status_for(exc), detail=str(exc))
        return {"subscription": service.subscription_snapshot(sub_id)}

    @app.post("/api/subscriptions/{sub_id}/ack")
    async def ack(sub_id: str, req: OffsetsRequest) -> Dict[str, Any]:
        try:
            service.ack(sub_id, req.offsets)
        except (UnknownSubscription, UnknownSource, InvalidSeek) as exc:
            raise HTTPException(status_code=_status_for(exc), detail=str(exc))
        return {"subscription": service.subscription_snapshot(sub_id)}

    @app.post("/api/subscriptions/{sub_id}/resume")
    async def resume(sub_id: str) -> Dict[str, Any]:
        try:
            service.resume(sub_id)
        except UnknownSubscription as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        return {"subscription": service.subscription_snapshot(sub_id)}

    @app.get("/api/subscriptions/{sub_id}/events")
    async def events(sub_id: str, limit: int = 500, wait: float = 0.5) -> Dict[str, Any]:
        try:
            items = await service.fetch(sub_id, limit=limit, wait=wait)
        except UnknownSubscription as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        return {"items": [item_to_dict(i) for i in items]}

    # -- WebSocket 实时流 ----------------------------------------------------

    @app.websocket("/ws/subscriptions/{sub_id}")
    async def ws_stream(websocket: WebSocket, sub_id: str) -> None:
        try:
            sub = service.broker.get_subscription(sub_id)
        except UnknownSubscription:
            await websocket.close(code=4404)
            return
        await websocket.accept()
        # 断线重连：从已确认位点继续，未确认事件重新投递。
        service.resume(sub_id)
        stop = asyncio.Event()

        async def reader() -> None:
            try:
                while not stop.is_set():
                    msg = await websocket.receive_json()
                    mtype = msg.get("type")
                    if mtype == "ack":
                        service.ack(sub_id, msg.get("offsets", {}))
                        await websocket.send_json({"type": "ok"})
                    elif mtype == "seek":
                        service.seek(sub_id, msg.get("offsets", {}))
                        await websocket.send_json({"type": "ok"})
                    else:
                        await websocket.send_json(
                            {"type": "error", "error": f"unknown message type {mtype!r}"}
                        )
            except (WebSocketDisconnect, Exception):
                stop.set()

        async def writer() -> None:
            try:
                while not stop.is_set():
                    while sub.queue or sub.gaps:
                        items: List[Any] = service.broker._pop_items(
                            sub, service.config.fetch_limit
                        )
                        await websocket.send_json(
                            {"type": "items", "items": [item_to_dict(i) for i in items]}
                        )
                    try:
                        await asyncio.wait_for(sub.notify.wait(), timeout=1.0)
                    except asyncio.TimeoutError:
                        pass
                    sub.notify.clear()
            except (WebSocketDisconnect, Exception):
                stop.set()

        reader_task = asyncio.create_task(reader())
        writer_task = asyncio.create_task(writer())
        await websocket.send_json(
            {"type": "hello", "subscription": service.subscription_snapshot(sub_id)}
        )
        try:
            await asyncio.gather(reader_task, writer_task)
        finally:
            stop.set()
            for task in (reader_task, writer_task):
                task.cancel()

    return app
