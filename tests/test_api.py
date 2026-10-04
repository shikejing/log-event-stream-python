"""API 测试：REST 接口与 WebSocket 实时推送。"""

from __future__ import annotations

import httpx
import pytest
from fastapi.testclient import TestClient

from logstream import Service
from logstream.api import create_app

from .conftest import make_config, make_service


async def _client(tmp_dir):
    svc = await make_service(tmp_dir)
    app = create_app(svc)
    transport = httpx.ASGITransport(app=app)
    client = httpx.AsyncClient(transport=transport, base_url="http://test")
    return client, svc


async def test_healthz(tmp_dir):
    client, svc = await _client(tmp_dir)
    try:
        r = await client.get("/healthz")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"
    finally:
        await client.aclose()
        await svc.stop()


async def test_create_source_and_inject(tmp_dir):
    client, svc = await _client(tmp_dir)
    try:
        r = await client.post("/api/sources", json={"id": "s1", "type": "fake"})
        assert r.status_code == 201
        assert r.json()["source"]["id"] == "s1"
        assert r.json()["source"]["type"] == "fake"

        r = await client.post("/api/sources/s1/inject", json={"payload": "hello"})
        assert r.json()["offset"] == 0

        r = await client.get("/api/sources")
        assert r.json()["sources"][0]["latest_offset"] == 0
    finally:
        await client.aclose()
        await svc.stop()


async def test_inject_unknown_source_404(tmp_dir):
    client, svc = await _client(tmp_dir)
    try:
        r = await client.post("/api/sources/nope/inject", json={"payload": "x"})
        assert r.status_code == 404
    finally:
        await client.aclose()
        await svc.stop()


async def test_create_subscription_and_events(tmp_dir):
    client, svc = await _client(tmp_dir)
    try:
        await client.post("/api/sources", json={"id": "s1", "type": "fake"})
        r = await client.post(
            "/api/subscriptions",
            json={"id": "sub", "filter": {"keyword": "error"}, "offsets": {"s1": 0}},
        )
        assert r.status_code == 201
        for p in ["info0", "error0", "info1"]:
            await client.post("/api/sources/s1/inject", json={"payload": p})
        r = await client.get("/api/subscriptions/sub/events", params={"wait": 0.3})
        items = r.json()["items"]
        assert [(i["offset"], i["payload"]) for i in items] == [(1, "error0")]
    finally:
        await client.aclose()
        await svc.stop()


async def test_ack_and_seek_via_api(tmp_dir):
    client, svc = await _client(tmp_dir)
    try:
        await client.post("/api/sources", json={"id": "s1", "type": "fake"})
        await client.post(
            "/api/subscriptions", json={"id": "sub", "filter": {}, "offsets": {"s1": 0}}
        )
        for i in range(5):
            await client.post("/api/sources/s1/inject", json={"payload": f"e{i}"})
        r = await client.get("/api/subscriptions/sub/events", params={"wait": 0.3})
        assert [i["offset"] for i in r.json()["items"]] == [0, 1, 2, 3, 4]

        r = await client.post("/api/subscriptions/sub/ack", json={"offsets": {"s1": 4}})
        assert r.status_code == 200
        r = await client.get("/api/subscriptions/sub")
        assert r.json()["subscription"]["acked"] == {"s1": 4}

        # 回退到 2
        r = await client.post("/api/subscriptions/sub/seek", json={"offsets": {"s1": 2}})
        assert r.status_code == 200
        r = await client.get("/api/subscriptions/sub/events", params={"wait": 0.3})
        assert [i["offset"] for i in r.json()["items"]] == [2, 3, 4]
    finally:
        await client.aclose()
        await svc.stop()


async def test_seek_out_of_range_400(tmp_dir):
    client, svc = await _client(tmp_dir)
    try:
        await client.post("/api/sources", json={"id": "s1", "type": "fake"})
        await client.post(
            "/api/subscriptions", json={"id": "sub", "filter": {}, "offsets": {"s1": 0}}
        )
        r = await client.post("/api/subscriptions/sub/seek", json={"offsets": {"s1": 100}})
        assert r.status_code == 400
    finally:
        await client.aclose()
        await svc.stop()


async def test_remove_source_and_subscription(tmp_dir):
    client, svc = await _client(tmp_dir)
    try:
        await client.post("/api/sources", json={"id": "s1", "type": "fake"})
        r = await client.delete("/api/sources/s1")
        assert r.status_code == 200
        r = await client.get("/api/sources")
        assert r.json()["sources"][0]["status"] == "removed"

        await client.post("/api/subscriptions", json={"id": "sub", "filter": {}})
        r = await client.delete("/api/subscriptions/sub")
        assert r.status_code == 200
    finally:
        await client.aclose()
        await svc.stop()


async def test_observability_snapshot(tmp_dir):
    client, svc = await _client(tmp_dir)
    try:
        await client.post("/api/sources", json={"id": "s1", "type": "fake"})
        await client.post(
            "/api/subscriptions", json={"id": "sub", "filter": {}, "offsets": {"s1": 0}}
        )
        for i in range(3):
            await client.post("/api/sources/s1/inject", json={"payload": f"e{i}"})
        r = await client.get("/api/subscriptions/sub")
        sub = r.json()["subscription"]
        assert sub["lag"]["s1"] == 3
        assert sub["pending"] == 3
        r = await client.get("/api/sources/s1")
        src = r.json()["source"]
        assert src["latest_offset"] == 2
        assert src["type"] == "fake"
    finally:
        await client.aclose()
        await svc.stop()


def test_websocket_stream(tmp_dir):
    """WebSocket 实时推送：注入后立即收到事件，ack 后不再推送。"""
    svc = Service(make_config(tmp_dir))
    app = create_app(svc)
    with TestClient(app) as client:
        client.portal.call(svc.start)
        client.post("/api/sources", json={"id": "s1", "type": "fake"})
        client.post(
            "/api/subscriptions", json={"id": "sub", "filter": {}, "offsets": {"s1": 0}}
        )
        with client.websocket_connect("/ws/subscriptions/sub") as ws:
            hello = ws.receive_json()
            assert hello["type"] == "hello"
            client.post("/api/sources/s1/inject", json={"payload": "e0"})
            msg = ws.receive_json()
            assert msg["type"] == "items"
            assert msg["items"][0]["payload"] == "e0"
            # ack 后再注入新事件，仍能收到
            ws.send_json({"type": "ack", "offsets": {"s1": 0}})
            assert ws.receive_json()["type"] == "ok"
            client.post("/api/sources/s1/inject", json={"payload": "e1"})
            msg = ws.receive_json()
            assert msg["items"][0]["payload"] == "e1"
        client.portal.call(svc.stop)


def test_websocket_resume(tmp_dir):
    """WebSocket 断线重连：已确认不重复，未确认重新投递。"""
    svc = Service(make_config(tmp_dir))
    app = create_app(svc)
    with TestClient(app) as client:
        client.portal.call(svc.start)
        client.post("/api/sources", json={"id": "s1", "type": "fake"})
        client.post(
            "/api/subscriptions", json={"id": "sub", "filter": {}, "offsets": {"s1": 0}}
        )
        # 第一轮：连接，收到 0..4，ack 2
        with client.websocket_connect("/ws/subscriptions/sub") as ws:
            ws.receive_json()  # hello
            for i in range(5):
                client.post("/api/sources/s1/inject", json={"payload": f"e{i}"})
            got = []
            while len(got) < 5:
                msg = ws.receive_json()
                got.extend(it["offset"] for it in msg["items"])
            assert got == [0, 1, 2, 3, 4]
            ws.send_json({"type": "ack", "offsets": {"s1": 2}})
            ws.receive_json()  # ok
        # 重连：未确认的 3,4 重新投递，已确认的 0,1,2 不重复
        with client.websocket_connect("/ws/subscriptions/sub") as ws:
            ws.receive_json()  # hello
            got = []
            while len(got) < 2:
                msg = ws.receive_json()
                got.extend(it["offset"] for it in msg["items"])
            assert got == [3, 4]
        client.portal.call(svc.stop)
