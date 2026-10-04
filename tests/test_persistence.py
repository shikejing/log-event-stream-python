"""持久化与重启恢复测试。"""

from __future__ import annotations

import asyncio
import json
import os

from logstream import Event, Filter, Gap

from .conftest import make_config, make_service


async def test_restart_committed_not_redelivered(tmp_dir):
    """已确认事件重启后不重复推送。"""
    svc = await make_service(tmp_dir)
    try:
        svc.add_source("fake", source_id="s1")
        svc.create_subscription(Filter(), sub_id="sub", offsets={"s1": 0})
        for i in range(5):
            await svc.inject("s1", f"e{i}")
        items = await svc.fetch("sub", wait=0.3)
        assert [i.offset for i in items] == [0, 1, 2, 3, 4]
        svc.ack("sub", {"s1": 2})
    finally:
        await svc.stop()

    svc2 = await make_service(tmp_dir)
    try:
        items = await svc2.fetch("sub", wait=0.3)
        assert [i.offset for i in items] == [3, 4], "unacked events must be redelivered"
        svc2.ack("sub", {"s1": 4})
        assert await svc2.inject("s1", "e5") == 5
        items = await svc2.fetch("sub", wait=0.3)
        assert [i.offset for i in items] == [5]
    finally:
        await svc2.stop()


async def test_restart_unacked_redelivered(tmp_dir):
    """未确认事件重启后重新投递（不跳过）。"""
    svc = await make_service(tmp_dir)
    try:
        svc.add_source("fake", source_id="s1")
        svc.create_subscription(Filter(), sub_id="sub", offsets={"s1": 0})
        for i in range(5):
            await svc.inject("s1", f"e{i}")
        # 拉取但不 ack
        items = await svc.fetch("sub", wait=0.3)
        assert [i.offset for i in items] == [0, 1, 2, 3, 4]
    finally:
        await svc.stop()

    svc2 = await make_service(tmp_dir)
    try:
        items = await svc2.fetch("sub", wait=0.3)
        assert [i.offset for i in items] == [0, 1, 2, 3, 4]
    finally:
        await svc2.stop()


async def test_restart_source_offsets_continue(tmp_dir):
    """重启后来源位点继续（假来源 + 文件来源），不重置。"""
    svc = await make_service(tmp_dir, file_poll_interval=0.02)
    try:
        svc.add_source("fake", source_id="s1")
        assert await svc.inject("s1", "f0") == 0
        assert await svc.inject("s1", "f1") == 1
        path = os.path.join(tmp_dir, "app.log")
        with open(path, "w") as f:
            f.write("l0\nl1\n")
        svc.add_source("file", source_id="f1", path=path)
        await asyncio.sleep(0.2)
        snap = svc.snapshot()
        assert snap["sources"]["f1"]["latest_offset"] == 1
    finally:
        await svc.stop()

    svc2 = await make_service(tmp_dir, file_poll_interval=0.02)
    try:
        assert await svc2.inject("s1", "f2") == 2
        with open(path, "a") as f:
            f.write("l2\n")
        await asyncio.sleep(0.2)
        snap = svc2.snapshot()
        assert snap["sources"]["s1"]["latest_offset"] == 2
        assert snap["sources"]["f1"]["latest_offset"] == 2
    finally:
        await svc2.stop()


async def test_state_distinguishes_persisted_vs_inflight(tmp_dir):
    """状态文件只含已提交位点（来源 last_offset、订阅 acked），不含队列/缺口。"""
    svc = await make_service(tmp_dir)
    try:
        svc.add_source("fake", source_id="s1")
        svc.create_subscription(Filter(), sub_id="sub", offsets={"s1": 0})
        for i in range(5):
            await svc.inject("s1", f"e{i}")
        await svc.fetch("sub", wait=0.3)
        svc.ack("sub", {"s1": 2})
    finally:
        await svc.stop()

    with open(os.path.join(tmp_dir, "state.json"), "r", encoding="utf-8") as f:
        state = json.load(f)
    # 来源：持久化了最后位点
    assert state["sources"]["s1"]["last_offset"] == 4
    # 订阅：只持久化已确认位点
    assert state["subscriptions"]["sub"]["acked"] == {"s1": 2}
    # 不含未完成的分发状态
    assert "queue" not in json.dumps(state)
    assert "gaps" not in json.dumps(state)
    assert "dropped" not in json.dumps(state)


async def test_restart_with_compaction_gap(tmp_dir):
    """断线期间日志被淘汰：重连后通过缺口感知未确认区间。"""
    svc = await make_service(tmp_dir, log_retention_events=5)
    try:
        svc.add_source("fake", source_id="s1")
        svc.create_subscription(Filter(), sub_id="sub", offsets={"s1": 0})
        for i in range(10):
            await svc.inject("s1", f"e{i}")
        await svc.fetch("sub", wait=0.3)
        svc.ack("sub", {"s1": 3})
    finally:
        await svc.stop()

    svc2 = await make_service(tmp_dir, log_retention_events=5)
    try:
        items = await svc2.fetch("sub", wait=0.3)
        gaps = [(i.from_offset, i.to_offset) for i in items if isinstance(i, Gap)]
        events = [i.offset for i in items if isinstance(i, Event)]
        assert gaps == [(4, 4)], gaps  # 4 未确认但已淘汰
        assert events == [5, 6, 7, 8, 9], events
    finally:
        await svc2.stop()
