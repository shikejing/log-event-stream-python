import asyncio

from logstream.sources import SourceManager


async def collect(sink_list, source_id, ts, msg):
    sink_list.append((source_id, msg))


async def test_injected_source():
    got = []
    mgr = SourceManager(sink=lambda s, t, m: collect(got, s, t, m))
    src = await mgr.add_injected_source("inj")
    await src.inject("hello")
    assert got == [("inj", "hello")]
    assert mgr.active_sources() == ["inj"]
    await mgr.stop_all()


async def test_file_tail_source(tmp_path):
    got = []
    mgr = SourceManager(sink=lambda s, t, m: collect(got, s, t, m))
    f = tmp_path / "app.log"
    f.write_text("line1\n")
    await mgr.add_file_source("file", f, poll_interval=0.01)
    await asyncio.sleep(0.1)
    with f.open("a") as fh:
        fh.write("line2\n")
    await asyncio.sleep(0.1)
    assert [m for _, m in got] == ["line1", "line2"]
    await mgr.remove_source("file")
    assert mgr.active_sources() == []


async def test_sources_do_not_block_each_other(tmp_path):
    got = []
    mgr = SourceManager(sink=lambda s, t, m: collect(got, s, t, m))
    fa, fb = tmp_path / "a.log", tmp_path / "b.log"
    await mgr.add_file_source("a", fa, poll_interval=0.01)
    await mgr.add_file_source("b", fb, poll_interval=0.01)
    fb.write_text("b1\n")
    await asyncio.sleep(0.1)
    fa.write_text("a1\n")
    await asyncio.sleep(0.1)
    assert ("b", "b1") in got and ("a", "a1") in got
    await mgr.stop_all()


async def test_restart_source_keeps_collecting(tmp_path):
    got = []
    mgr = SourceManager(sink=lambda s, t, m: collect(got, s, t, m))
    f = tmp_path / "app.log"
    f.write_text("x\n")
    await mgr.add_file_source("f", f, poll_interval=0.01)
    await asyncio.sleep(0.1)
    await mgr.add_file_source("f", f, poll_interval=0.01)  # 重启
    with f.open("a") as fh:
        fh.write("y\n")
    await asyncio.sleep(0.1)
    assert ("f", "y") in got
    await mgr.stop_all()
