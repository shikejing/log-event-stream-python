"""来源采集测试：并发顺序、文件 tail、轮转、动态增删、来源间互不阻塞。"""

from __future__ import annotations

import asyncio
import os

import pytest

from logstream import Filter, UnknownSource

from .conftest import make_service


async def test_fake_source_concurrent_ordering(tmp_dir):
    """多任务并发注入：每个事件拿到唯一递增位点，投递顺序与位点顺序一致。"""
    svc = await make_service(tmp_dir)
    try:
        svc.add_source("fake", source_id="s1")
        svc.create_subscription(Filter(), sub_id="sub", offsets={"s1": 0})

        async def producer(base: int, count: int):
            for i in range(count):
                await svc.inject("s1", f"p{base}-{i}")

        tasks = [asyncio.create_task(producer(b, 50)) for b in range(4)]
        await asyncio.gather(*tasks)

        items = await svc.fetch("sub", limit=500, wait=1.0)
        offsets = [i.offset for i in items]
        assert offsets == list(range(200)), "offsets must be contiguous and in order"
        assert len({i.payload for i in items}) == 200
    finally:
        await svc.stop()


async def test_file_source_tailing(tmp_dir):
    """文件来源：写入的行按顺序采集，追加后继续读取，位点连续。"""
    svc = await make_service(tmp_dir, file_poll_interval=0.02)
    try:
        path = os.path.join(tmp_dir, "app.log")
        with open(path, "w") as f:
            f.write("line0\nline1\n")
        svc.add_source("file", source_id="f1", path=path)
        await asyncio.sleep(0.2)

        svc.create_subscription(Filter(), sub_id="sub", offsets={"f1": 0})
        items = await svc.fetch("sub", wait=0.3)
        assert [(i.source_id, i.offset, i.payload) for i in items] == [
            ("f1", 0, "line0"),
            ("f1", 1, "line1"),
        ]

        with open(path, "a") as f:
            f.write("line2\nline3\n")
        await asyncio.sleep(0.2)
        items = await svc.fetch("sub", wait=0.3)
        assert [(i.source_id, i.offset, i.payload) for i in items] == [
            ("f1", 2, "line2"),
            ("f1", 3, "line3"),
        ]
    finally:
        await svc.stop()


async def test_file_source_truncation_keeps_offsets(tmp_dir):
    """文件被截断（轮转）：从头读，但来源内位点继续递增、不重置。"""
    svc = await make_service(tmp_dir, file_poll_interval=0.02)
    try:
        path = os.path.join(tmp_dir, "app.log")
        with open(path, "w") as f:
            f.write("a0\na1\n")
        svc.add_source("file", source_id="f1", path=path)
        await asyncio.sleep(0.2)
        svc.create_subscription(Filter(), sub_id="sub", offsets={"f1": 0})
        items = await svc.fetch("sub", wait=0.3)
        assert [i.offset for i in items] == [0, 1]

        # 截断重写
        with open(path, "w") as f:
            f.write("b0\n")
        await asyncio.sleep(0.2)
        items = await svc.fetch("sub", wait=0.3)
        # 位点继续：b0 分配到 offset 2
        assert [(i.offset, i.payload) for i in items] == [(2, "b0")]
    finally:
        await svc.stop()


async def test_source_remove_and_readd_continues_offsets(tmp_dir):
    """来源移除后重新加入：历史位点保留，新事件位点继续，订阅者不错位。"""
    svc = await make_service(tmp_dir)
    try:
        svc.add_source("fake", source_id="s1")
        assert await svc.inject("s1", "e0") == 0
        assert await svc.inject("s1", "e1") == 1
        svc.create_subscription(Filter(), sub_id="sub", offsets={"s1": 0})
        await svc.fetch("sub", wait=0.2)

        await svc.remove_source("s1")
        # 移除后不能再注入
        with pytest.raises(UnknownSource):
            await svc.inject("s1", "late")

        # 重新加入：日志与位点保留
        svc.add_source("fake", source_id="s1")
        assert await svc.inject("s1", "e2") == 2
        items = await svc.fetch("sub", wait=0.3)
        assert [(i.offset, i.payload) for i in items] == [(2, "e2")]
    finally:
        await svc.stop()


async def test_sources_do_not_block_each_other(tmp_dir):
    """两个来源并发采集：一个来源的采集不影响另一个的进度。"""
    svc = await make_service(tmp_dir)
    try:
        svc.add_source("fake", source_id="fast")
        svc.add_source("fake", source_id="slow")
        svc.create_subscription(Filter(), sub_id="sub", offsets={"fast": 0, "slow": 0})

        async def fast_producer():
            for i in range(100):
                await svc.inject("fast", f"f{i}")

        async def slow_producer():
            for i in range(20):
                await asyncio.sleep(0.01)
                await svc.inject("slow", f"s{i}")

        await asyncio.gather(fast_producer(), slow_producer())
        snap = svc.snapshot()
        assert snap["sources"]["fast"]["latest_offset"] == 99
        assert snap["sources"]["slow"]["latest_offset"] == 19
    finally:
        await svc.stop()


async def test_file_source_partial_line(tmp_dir):
    """文件末尾不完整行：暂存不投递，换行后才采集。"""
    svc = await make_service(tmp_dir, file_poll_interval=0.02)
    try:
        path = os.path.join(tmp_dir, "app.log")
        with open(path, "w") as f:
            f.write("complete\npartial")  # 末尾无换行
        svc.add_source("file", source_id="f1", path=path)
        await asyncio.sleep(0.2)
        svc.create_subscription(Filter(), sub_id="sub", offsets={"f1": 0})
        items = await svc.fetch("sub", wait=0.2)
        assert [(i.offset, i.payload) for i in items] == [(0, "complete")]
        with open(path, "a") as f:
            f.write(" end\nnext\n")
        await asyncio.sleep(0.2)
        items = await svc.fetch("sub", wait=0.2)
        assert [(i.offset, i.payload) for i in items] == [(1, "partial end"), (2, "next")]
    finally:
        await svc.stop()


async def test_snapshot_consistent_during_push(tmp_dir):
    """并发推送时快照自洽：latest_offset 与 retained 等计数不出现矛盾。"""
    svc = await make_service(tmp_dir)
    try:
        svc.add_source("fake", source_id="s1")
        svc.create_subscription(Filter(), sub_id="sub", offsets={"s1": 0})

        async def producer():
            for i in range(200):
                await svc.inject("s1", f"e{i}")

        async def observer():
            for _ in range(50):
                snap = svc.snapshot()
                src = snap["sources"]["s1"]
                sub = snap["subscriptions"]["sub"]
                # 自洽：retained <= latest+1；acked <= latest；lag >= 0
                assert src["retained"] <= src["latest_offset"] + 1
                assert sub["acked"]["s1"] <= src["latest_offset"]
                assert sub["lag"]["s1"] >= 0
                await asyncio.sleep(0.001)

        await asyncio.gather(producer(), observer())
    finally:
        await svc.stop()


async def test_burst_injection_bounded_memory(tmp_dir):
    """突发注入：日志保留与订阅队列都有界，内存不无限增长。"""
    svc = await make_service(tmp_dir, log_retention_events=100, sub_queue_maxsize=10)
    try:
        svc.add_source("fake", source_id="s1")
        svc.create_subscription(Filter(), sub_id="sub", offsets={"s1": 0})
        for i in range(1000):
            await svc.inject("s1", f"e{i}")
        snap = svc.snapshot()
        assert snap["sources"]["s1"]["retained"] <= 100
        assert snap["subscriptions"]["sub"]["pending"] <= 10
        assert snap["subscriptions"]["sub"]["dropped_total"] == 990
    finally:
        await svc.stop()
