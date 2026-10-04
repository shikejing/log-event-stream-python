import asyncio

import pytest
from fastapi.testclient import TestClient

from logstream.api import create_app
from logstream.service import LogStreamService


@pytest.fixture
def client(tmp_path):
    svc = LogStreamService(tmp_path / "data", queue_size=8)
    app = create_app(svc)
    with TestClient(app) as c:
        yield c


def test_status_empty(client):
    r = client.get("/status")
    assert r.status_code == 200
    assert r.json() == {"sources": {}, "subscribers": {}}


def test_source_lifecycle_and_inject(client):
    assert client.post("/sources", json={"source_id": "a"}).json()["ok"]
    assert client.post("/sources/a/inject", json={"message": "hello"}).json()["ok"]
    status = client.get("/status").json()
    assert status["sources"]["a"]["last_seq"] == 1
    assert client.delete("/sources/a").json()["active"] == []


def test_websocket_subscribe_and_ack(client):
    client.post("/sources", json={"source_id": "a"})
    client.post("/sources/a/inject", json={"message": "first"})
    with client.websocket_connect("/subscribe/s1") as ws:
        msg = ws.receive_json()
        assert msg["type"] == "event" and msg["seq"] == 1
        ws.send_json({"ack": {"source_id": "a", "seq": 1}})
        client.post("/sources/a/inject", json={"message": "second"})
        msg = ws.receive_json()
        assert msg["seq"] == 2 and msg["message"] == "second"


def test_websocket_filter_by_keyword(client):
    client.post("/sources", json={"source_id": "a"})
    client.post("/sources/a/inject", json={"message": "noise"})
    client.post("/sources/a/inject", json={"message": "important hit"})
    with client.websocket_connect("/subscribe/s1?keyword=hit") as ws:
        msg = ws.receive_json()
        assert msg["seq"] == 2  # 过滤不改变位点


def test_websocket_resume_out_of_range(client):
    client.post("/sources", json={"source_id": "a"})
    client.post("/sources/a/inject", json={"message": "m"})
    with client.websocket_connect('/subscribe/s1?resume={"a": 42}') as ws:
        msg = ws.receive_json()
        assert msg["type"] == "error" and msg["error"] == "resume_out_of_range"
