"""HTTP/WebSocket API：订阅推送、断点续传、状态查询、来源管理。"""
from __future__ import annotations

import asyncio
import json

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from .hub import Gap
from .models import Event, Position, SubscriptionFilter
from .service import LogStreamService, ResumeOutOfRange


class AddSourceRequest(BaseModel):
    source_id: str
    type: str = "injected"  # "injected" | "file"
    path: str | None = None


class InjectRequest(BaseModel):
    message: str
    timestamp: float | None = None


def _event_msg(e: Event) -> dict:
    return {"type": "event", "source_id": e.source_id, "seq": e.seq,
            "timestamp": e.timestamp, "message": e.message}


def create_app(service: LogStreamService) -> FastAPI:
    app = FastAPI(title="log-event-stream")

    @app.get("/status")
    async def status() -> dict:
        return service.status()

    @app.post("/sources")
    async def add_source(req: AddSourceRequest) -> dict:
        if req.type == "file":
            if not req.path:
                return {"ok": False, "error": "file source requires path"}
            await service.sources.add_file_source(req.source_id, req.path)
        else:
            await service.sources.add_injected_source(req.source_id)
        return {"ok": True, "active": service.sources.active_sources()}

    @app.delete("/sources/{source_id}")
    async def remove_source(source_id: str) -> dict:
        await service.sources.remove_source(source_id)
        return {"ok": True, "active": service.sources.active_sources()}

    @app.post("/sources/{source_id}/inject")
    async def inject(source_id: str, req: InjectRequest) -> dict:
        src = service.sources._injected.get(source_id)
        if src is None:
            return {"ok": False, "error": f"no injected source {source_id!r}"}
        await src.inject(req.message, req.timestamp)
        return {"ok": True}

    @app.websocket("/subscribe/{subscriber_id}")
    async def subscribe(ws: WebSocket, subscriber_id: str,
                        sources: str | None = None,
                        keyword: str | None = None,
                        start_ts: float | None = None,
                        end_ts: float | None = None,
                        resume: str | None = None) -> None:
        await ws.accept()
        filt = SubscriptionFilter(
            sources=frozenset(sources.split(",")) if sources else None,
            keyword=keyword, start_ts=start_ts, end_ts=end_ts)
        resume_map = json.loads(resume) if resume else None
        try:
            sub = await service.subscribe(subscriber_id, filt, resume=resume_map)
        except ResumeOutOfRange as exc:
            await ws.send_json({"type": "error", "error": "resume_out_of_range",
                                "detail": str(exc)})
            await ws.close(code=4000)
            return

        async def pump() -> None:
            while True:
                item = await sub.next()
                if isinstance(item, Gap):
                    await ws.send_json({"type": "gap", "source_id": item.source_id,
                                        "up_to_seq": item.up_to_seq})
                else:
                    await ws.send_json(_event_msg(item))

        async def consume_acks() -> None:
            while True:
                msg = await ws.receive_json()
                if "ack" in msg:
                    ack = msg["ack"]
                    service.ack(subscriber_id,
                                Position(ack["source_id"], int(ack["seq"])))

        pump_task = asyncio.create_task(pump())
        try:
            await consume_acks()
        except WebSocketDisconnect:
            pass
        finally:
            pump_task.cancel()
            service.hub.remove_subscriber(subscriber_id)

    return app
