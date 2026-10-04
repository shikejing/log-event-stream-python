"""订阅测试：过滤、位点语义、ack/seek、慢订阅者丢弃策略、滞后指标。"""

from __future__ import annotations

import pytest

from logstream import (
    Event,
    Filter,
    Gap,
    InvalidSeek,
    OffsetOutOfRange,
    OffsetUnavailable,
)

from .conftest import make_service


async def test_filter_by_source(tmp_dir):
    svc = await make_service(tmp_dir)
    try:
        svc.add_source("fake", source_id="a")
        svc.add_source("fake", source_id="b")
        svc.create_subscription(Filter(sources={"a"}), sub_id="sub", offsets={"a": 0, "b": 0})
        await svc.inject("a", "a0")
        await svc.inject("b", "b0")
        await svc.inject("a", "a1")
        items = await svc.fetch("sub", wait=0.3)
        assert [(i.source_id, i.offset) for i in items] == [("a", 0), ("a", 1)]
    finally:
        await svc.stop()


async def test_filter_by_keyword(tmp_dir):
    svc = await make_service(tmp_dir)
    try:
        svc.add_source("fake", source_id="s1")
        svc.create_subscription(Filter(keyword="error"), sub_id="sub", offsets={"s1": 0})
        for p in ["info0", "error0", "info1", "error1"]:
            await svc.inject("s1", p)
        items = await svc.fetch("sub", wait=0.3)
        assert [(i.offset, i.payload) for i in items] == [(1, "error0"), (3, "error1")]
    finally:
        await svc.stop()


async def test_filter_by_time_range(tmp_dir):
    svc = await make_service(tmp_dir)
    try:
        svc.add_source("fake", source_id="s1")
        import time as _t
        now = _t.time()
        svc.create_subscription(
            Filter(start_time=now - 10, end_time=now + 10), sub_id="sub", offsets={"s1": 0}
        )
        await svc.inject("s1", "early", timestamp=now - 100)
        await svc.inject("s1", "in", timestamp=now)
        await svc.inject("s1", "late", timestamp=now + 100)
        items = await svc.fetch("sub", wait=0.3)
        assert [(i.offset, i.payload) for i in items] == [(1, "in")]
    finally:
        await svc.stop()


async def test_filter_does_not_change_offsets(tmp_dir):
    """过滤不改变位点：被过滤事件仍占位点，订阅者 delivered 照常推进。"""
    svc = await make_service(tmp_dir)
    try:
        svc.add_source("fake", source_id="s1")
        svc.create_subscription(Filter(keyword="error"), sub_id="sub", offsets={"s1": 0})
        for p in ["info0", "error0", "info1", "error1", "info2", "error2"]:
            await svc.inject("s1", p)
        items = await svc.fetch("sub", wait=0.3)
        # 可见事件的位点仍是 1,3,5，没有被压缩成 0,1,2
        assert [i.offset for i in items] == [1, 3, 5]
        snap = svc.subscription_snapshot("sub")
        assert snap["delivered"]["s1"] == 5  # delivered 跳过 info 事件推进到 5
        assert snap["lag"]["s1"] == 6  # latest(5) - acked(-1) = 6
        svc.ack("sub", {"s1": 5})
        snap = svc.subscription_snapshot("sub")
        assert snap["lag"]["s1"] == 0
    finally:
        await svc.stop()


async def test_ack_skips_committed_events(tmp_dir):
    """已确认事件不重复推送。"""
    svc = await make_service(tmp_dir)
    try:
        svc.add_source("fake", source_id="s1")
        svc.create_subscription(Filter(), sub_id="sub", offsets={"s1": 0})
        for i in range(5):
            await svc.inject("s1", f"e{i}")
        items = await svc.fetch("sub", wait=0.3)
        assert [i.offset for i in items] == [0, 1, 2, 3, 4]
        svc.ack("sub", {"s1": 2})
        for i in range(5, 8):
            await svc.inject("s1", f"e{i}")
        items = await svc.fetch("sub", wait=0.3)
        # 只推送未确认的新事件 5,6,7，不重复 0,1,2
        assert [i.offset for i in items] == [5, 6, 7]
    finally:
        await svc.stop()


async def test_seek_rewind_redelivers(tmp_dir):
    """位点回退到已保留区间：明确允许，重新投递。"""
    svc = await make_service(tmp_dir)
    try:
        svc.add_source("fake", source_id="s1")
        svc.create_subscription(Filter(), sub_id="sub", offsets={"s1": 0})
        for i in range(5):
            await svc.inject("s1", f"e{i}")
        await svc.fetch("sub", wait=0.3)
        svc.ack("sub", {"s1": 4})
        # 回退到 2，重新投递 2,3,4
        svc.seek("sub", {"s1": 2})
        items = await svc.fetch("sub", wait=0.3)
        assert [i.offset for i in items] == [2, 3, 4]
    finally:
        await svc.stop()


async def test_seek_out_of_range(tmp_dir):
    """指向未来的位点请求：OffsetOutOfRange。"""
    svc = await make_service(tmp_dir)
    try:
        svc.add_source("fake", source_id="s1")
        svc.create_subscription(Filter(), sub_id="sub", offsets={"s1": 0})
        for i in range(3):
            await svc.inject("s1", f"e{i}")
        with pytest.raises(OffsetOutOfRange):
            svc.seek("sub", {"s1": 10})
    finally:
        await svc.stop()


async def test_seek_unavailable_after_compaction(tmp_dir):
    """指向已淘汰位点：OffsetUnavailable；seek 到最旧保留位点可重新投递。"""
    svc = await make_service(tmp_dir, log_retention_events=5)
    try:
        svc.add_source("fake", source_id="s1")
        svc.create_subscription(Filter(), sub_id="sub", offsets={"s1": 0})
        for i in range(10):
            await svc.inject("s1", f"e{i}")
        await svc.fetch("sub", wait=0.3)
        svc.ack("sub", {"s1": 3})
        # 日志只保留 5..9，seek 到 2 已淘汰
        with pytest.raises(OffsetUnavailable):
            svc.seek("sub", {"s1": 2})
        # seek 到最旧保留位点 5：重新投递 5..9（0..4 被有意跳过）
        svc.seek("sub", {"s1": 5})
        items = await svc.fetch("sub", wait=0.3)
        assert [i.offset for i in items if isinstance(i, Event)] == [5, 6, 7, 8, 9]
    finally:
        await svc.stop()


async def test_slow_subscriber_drops_with_gap(tmp_dir):
    """慢订阅者：有界队列满后丢弃最旧事件并给出缺口，内存有界、可观测。"""
    svc = await make_service(tmp_dir, sub_queue_maxsize=5)
    try:
        svc.add_source("fake", source_id="s1")
        svc.create_subscription(Filter(), sub_id="slow", offsets={"s1": 0})
        svc.create_subscription(Filter(), sub_id="fast", offsets={"s1": 0}, queue_maxsize=1000)
        for i in range(100):
            await svc.inject("s1", f"e{i}")
        snap = svc.subscription_snapshot("slow")
        assert snap["pending"] <= 5  # 队列长度有界
        assert snap["dropped_total"] == 95  # 丢弃了 0..94
        items = await svc.fetch("slow", wait=0.3)
        gaps = [(i.from_offset, i.to_offset) for i in items if isinstance(i, Gap)]
        events = [i.offset for i in items if isinstance(i, Event)]
        assert gaps == [(0, 94)], gaps
        assert events == [95, 96, 97, 98, 99], events
        # 缺口可观测
        snap = svc.subscription_snapshot("slow")
        assert snap["dropped_total"] == 95
    finally:
        await svc.stop()


async def test_fast_subscriber_unaffected_by_slow(tmp_dir):
    """慢订阅者不阻塞快订阅者：快订阅者收到全部事件，无丢弃。"""
    svc = await make_service(tmp_dir, sub_queue_maxsize=5)
    try:
        svc.add_source("fake", source_id="s1")
        svc.create_subscription(Filter(), sub_id="slow", offsets={"s1": 0})
        svc.create_subscription(Filter(), sub_id="fast", offsets={"s1": 0}, queue_maxsize=1000)
        for i in range(100):
            await svc.inject("s1", f"e{i}")
        fast_items = await svc.fetch("fast", limit=500, wait=0.3)
        assert [i.offset for i in fast_items] == list(range(100))
        fast_snap = svc.subscription_snapshot("fast")
        assert fast_snap["dropped_total"] == 0
        slow_snap = svc.subscription_snapshot("slow")
        assert slow_snap["dropped_total"] == 95
    finally:
        await svc.stop()


async def test_ack_beyond_latest_invalid(tmp_dir):
    """ack 超过最新位点：InvalidSeek。"""
    svc = await make_service(tmp_dir)
    try:
        svc.add_source("fake", source_id="s1")
        svc.create_subscription(Filter(), sub_id="sub", offsets={"s1": 0})
        for i in range(3):
            await svc.inject("s1", f"e{i}")
        with pytest.raises(InvalidSeek):
            svc.ack("sub", {"s1": 5})
    finally:
        await svc.stop()


async def test_new_subscription_starts_from_latest(tmp_dir):
    """新订阅默认从最新位点开始（不读历史），只收到之后产生的事件。"""
    svc = await make_service(tmp_dir)
    try:
        svc.add_source("fake", source_id="s1")
        for i in range(5):
            await svc.inject("s1", f"old{i}")
        svc.create_subscription(Filter(), sub_id="sub")  # 无 offsets
        items = await svc.fetch("sub", wait=0.2)
        assert items == []  # 不收历史
        await svc.inject("s1", "new")
        items = await svc.fetch("sub", wait=0.3)
        assert [(i.offset, i.payload) for i in items] == [(5, "new")]
    finally:
        await svc.stop()


async def test_ack_monotonic(tmp_dir):
    """ack 单调推进：回退 ack 不会把已确认位点拉回去。"""
    svc = await make_service(tmp_dir)
    try:
        svc.add_source("fake", source_id="s1")
        svc.create_subscription(Filter(), sub_id="sub", offsets={"s1": 0})
        for i in range(5):
            await svc.inject("s1", f"e{i}")
        await svc.fetch("sub", wait=0.3)
        svc.ack("sub", {"s1": 4})
        svc.ack("sub", {"s1": 2})  # 更旧的 ack
        snap = svc.subscription_snapshot("sub")
        assert snap["acked"]["s1"] == 4  # 不回退
    finally:
        await svc.stop()


async def test_multiple_subscribers_independent_speeds(tmp_dir):
    """多个订阅者共享同一份事件流，各自以不同速度推进，互不影响。"""
    svc = await make_service(tmp_dir)
    try:
        svc.add_source("fake", source_id="s1")
        svc.create_subscription(Filter(), sub_id="fast", offsets={"s1": 0}, queue_maxsize=1000)
        svc.create_subscription(Filter(), sub_id="slow", offsets={"s1": 0}, queue_maxsize=3)
        for i in range(20):
            await svc.inject("s1", f"e{i}")
        fast_items = await svc.fetch("fast", limit=500, wait=0.3)
        assert [i.offset for i in fast_items if isinstance(i, Event)] == list(range(20))
        slow_items = await svc.fetch("slow", limit=500, wait=0.3)
        # slow 队列只有 3 个事件 + 缺口
        slow_events = [i.offset for i in slow_items if isinstance(i, Event)]
        assert len(slow_events) == 3
        assert svc.subscription_snapshot("slow")["dropped_total"] == 17
        # fast 无丢弃
        assert svc.subscription_snapshot("fast")["dropped_total"] == 0
    finally:
        await svc.stop()


async def test_ordering_by_offset_not_timestamp(tmp_dir):
    """时间戳乱序不影响位点顺序：同一来源始终按 offset 顺序投递。"""
    import time as _t

    svc = await make_service(tmp_dir)
    try:
        svc.add_source("fake", source_id="s1")
        svc.create_subscription(Filter(), sub_id="sub", offsets={"s1": 0})
        base = _t.time()
        # 故意用乱序时间戳注入
        await svc.inject("s1", "e0", timestamp=base + 100)
        await svc.inject("s1", "e1", timestamp=base)
        await svc.inject("s1", "e2", timestamp=base + 50)
        items = await svc.fetch("sub", wait=0.3)
        assert [i.offset for i in items] == [0, 1, 2]
        assert [i.payload for i in items] == ["e0", "e1", "e2"]
    finally:
        await svc.stop()


async def test_filter_multiple_sources(tmp_dir):
    """按多个来源过滤。"""
    svc = await make_service(tmp_dir)
    try:
        svc.add_source("fake", source_id="a")
        svc.add_source("fake", source_id="b")
        svc.add_source("fake", source_id="c")
        svc.create_subscription(
            Filter(sources={"a", "c"}), sub_id="sub", offsets={"a": 0, "b": 0, "c": 0}
        )
        for src in ["a", "b", "c", "a", "b", "c"]:
            await svc.inject(src, f"{src}-msg")
        items = await svc.fetch("sub", wait=0.3)
        assert [(i.source_id, i.offset) for i in items] == [
            ("a", 0),
            ("c", 0),
            ("a", 1),
            ("c", 1),
        ]
    finally:
        await svc.stop()


async def test_subscription_created_before_source(tmp_dir):
    """订阅先于来源创建：来源加入后从位点 0 开始收到全部事件（订阅时来源不存在，无历史可跳）。"""
    svc = await make_service(tmp_dir)
    try:
        svc.create_subscription(Filter(), sub_id="sub")  # 无来源
        svc.add_source("fake", source_id="s1")
        for i in range(3):
            await svc.inject("s1", f"e{i}")
        items = await svc.fetch("sub", wait=0.3)
        assert [(i.offset, i.payload) for i in items] == [(0, "e0"), (1, "e1"), (2, "e2")]
    finally:
        await svc.stop()
