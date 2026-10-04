import asyncio

import pytest

from logstream.hub import Gap
from logstream.models import Event, Position, SubscriptionFilter
from logstream.service import LogStreamService, ResumeOutOfRange


async def make_service(tmp_path, queue_size=1024):
    svc = LogStreamService(tmp_path / "data", queue_size=queue_size)
    await svc.start()
    return svc


async def drain(sub, n):
    return [await sub.next(timeout=1.0) for _ in range(n)]


async def test_per_source_monotonic_positions(tmp_path):
    svc = await make_service(tmp_path)
    a = await svc.sources.add_injected_source("a")
    b = await svc.sources.add_injected_source("b")
    # 交错注入，模拟并发采集
    await a.inject("a1"); await b.inject("b1"); await a.inject("a2"); await b.inject("b2")
    sub = await svc.subscribe("s1", SubscriptionFilter())
    events = await drain(sub, 4)
    per_src = {}
    for e in events:
        per_src.setdefault(e.source_id, []).append(e.seq)
    assert per_src["a"] == [1, 2]  # 来源内单调递增且顺序与产生一致
    assert per_src["b"] == [1, 2]
    await svc.stop()


async def test_filter_does_not_change_positions(tmp_path):
    svc = await make_service(tmp_path)
    src = await svc.sources.add_injected_source("a")
    sub = await svc.subscribe("s1", SubscriptionFilter(keyword="hit"))
    await src.inject("miss one")
    await src.inject("hit two")
    await src.inject("hit three")
    events = await drain(sub, 2)
    assert [e.seq for e in events] == [2, 3]  # 被过滤的事件仍占用位点
    await svc.stop()


async def test_resume_after_disconnect(tmp_path):
    svc = await make_service(tmp_path)
    src = await svc.sources.add_injected_source("a")
    sub = await svc.subscribe("s1", SubscriptionFilter())
    for i in range(1, 6):
        await src.inject(f"m{i}")
    got = await drain(sub, 3)
    for e in got:
        svc.ack("s1", e.position)  # 只确认前 3 条
    svc.hub.remove_subscriber("s1")  # 模拟断线
    # 重连：从 checkpoint 续传，已确认的不重发，未确认的不跳过
    sub2 = await svc.subscribe("s1", SubscriptionFilter())
    rest = await drain(sub2, 2)
    assert [e.seq for e in rest] == [4, 5]
    await svc.stop()


async def test_resume_out_of_range_and_rollback(tmp_path):
    svc = await make_service(tmp_path)
    src = await svc.sources.add_injected_source("a")
    await src.inject("m1")
    with pytest.raises(ResumeOutOfRange):
        await svc.subscribe("s1", SubscriptionFilter(), resume={"a": 99})
    # 位点回退：负数按 0 处理，从头消费
    sub = await svc.subscribe("s2", SubscriptionFilter(), resume={"a": -5})
    e = await sub.next(timeout=1.0)
    assert e.seq == 1
    await svc.stop()


async def test_slow_subscriber_gap_and_fast_unaffected(tmp_path):
    svc = await make_service(tmp_path, queue_size=4)
    src = await svc.sources.add_injected_source("a")
    fast = await svc.subscribe("fast", SubscriptionFilter())
    slow = await svc.subscribe("slow", SubscriptionFilter())
    for i in range(1, 11):
        await src.inject(f"m{i}")
        await fast.next(timeout=1.0)  # 快订阅者实时消费
    first = await slow.next(timeout=1.0)
    assert isinstance(first, Gap) and first.up_to_seq == 6
    status = svc.status()
    assert status["subscribers"]["slow"]["dropped"] == 6
    assert status["subscribers"]["fast"]["lag"] == 0
    await svc.stop()


async def test_dynamic_source_add_remove(tmp_path):
    svc = await make_service(tmp_path)
    sub = await svc.subscribe("s1", SubscriptionFilter())
    a = await svc.sources.add_injected_source("a")
    await a.inject("a1")
    assert (await sub.next(timeout=1.0)).message == "a1"
    await svc.sources.remove_source("a")
    b = await svc.sources.add_injected_source("b")
    await b.inject("b1")
    e = await sub.next(timeout=1.0)
    assert (e.source_id, e.seq) == ("b", 1)
    # 重新加入 a：位点延续，不重置
    a2 = await svc.sources.add_injected_source("a")
    await a2.inject("a2")
    e = await sub.next(timeout=1.0)
    assert (e.source_id, e.seq, e.message) == ("a", 2, "a2")
    await svc.stop()


async def test_restart_recovery(tmp_path):
    data = tmp_path / "data"
    svc = await make_service(tmp_path)
    src = await svc.sources.add_injected_source("a")
    sub = await svc.subscribe("s1", SubscriptionFilter())
    for i in range(1, 4):
        await src.inject(f"m{i}")
    got = await drain(sub, 2)
    for e in got:
        svc.ack("s1", e.position)
    await svc.stop()

    # 重启服务：journal 与 checkpoint 从磁盘恢复
    svc2 = LogStreamService(data)
    await svc2.start()
    assert svc2.journal_for("a").last_seq() == 3  # 位点不重置
    sub2 = await svc2.subscribe("s1", SubscriptionFilter())  # 用持久化 checkpoint
    e = await sub2.next(timeout=1.0)
    assert e.seq == 3  # 只补发未确认的事件
    src2 = await svc2.sources.add_injected_source("a")
    await src2.inject("m4")
    e = await sub2.next(timeout=1.0)
    assert e.seq == 4  # 新事件位点延续
    await svc2.stop()


async def test_status_consistency(tmp_path):
    svc = await make_service(tmp_path)
    src = await svc.sources.add_injected_source("a")
    sub = await svc.subscribe("s1", SubscriptionFilter())
    for i in range(5):
        await src.inject(f"m{i}")
    status = svc.status()
    assert status["sources"]["a"] == {"last_seq": 5, "active": True}
    assert status["subscribers"]["s1"]["lag"] == 5
    await drain(sub, 5)
    assert svc.status()["subscribers"]["s1"]["lag"] == 0
    await svc.stop()
