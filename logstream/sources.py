"""采集来源：本地文件 tail 与进程内注入假来源，支持动态增删与重启。"""
from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Awaitable, Callable

EventSink = Callable[[str, float, str], Awaitable[None]]


class FileTailSource:
    """尾随本地文件，每新增一行产生一条事件。

    - 从文件头开始读，之后增量尾随；
    - 文件被截断（如日志轮转）时回到文件头重读，新事件会获得新 seq，
      旧位点不受影响；
    - 通过 asyncio 任务取消来停止。
    """

    def __init__(self, source_id: str, path: Path, sink: EventSink,
                 poll_interval: float = 0.05) -> None:
        self.source_id = source_id
        self.path = Path(path)
        self.sink = sink
        self.poll_interval = poll_interval

    async def run(self) -> None:
        offset = 0
        buf = b""
        while True:
            try:
                size = self.path.stat().st_size
            except FileNotFoundError:
                await asyncio.sleep(self.poll_interval)
                continue
            if size < offset:
                # 文件被截断/轮转：从头重读
                offset = 0
                buf = b""
            if size > offset:
                with self.path.open("rb") as f:
                    f.seek(offset)
                    chunk = f.read(size - offset)
                offset = size
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    text = line.decode("utf-8", errors="replace").rstrip("\r")
                    if text:
                        await self.sink(self.source_id, time.time(), text)
            await asyncio.sleep(self.poll_interval)


class InjectedSource:
    """进程内注入的假来源：通过 inject() 投递事件。"""

    def __init__(self, source_id: str, sink: EventSink) -> None:
        self.source_id = source_id
        self.sink = sink

    async def inject(self, message: str, timestamp: float | None = None) -> None:
        await self.sink(self.source_id,
                        time.time() if timestamp is None else timestamp,
                        message)


class SourceManager:
    """管理来源的动态增删与重启。

    每个来源一个独立 asyncio 任务，来源间互不阻塞；
    位点由 journal 持久化，来源重启/重加不会重置历史位点。
    """

    def __init__(self, sink: EventSink) -> None:
        self.sink = sink
        self._tasks: dict[str, asyncio.Task] = {}
        self._injected: dict[str, InjectedSource] = {}

    async def add_file_source(self, source_id: str, path: Path,
                              poll_interval: float = 0.05) -> None:
        await self.remove_source(source_id)  # 重启语义：先停旧任务
        src = FileTailSource(source_id, path, self.sink, poll_interval)
        self._tasks[source_id] = asyncio.create_task(src.run())

    async def add_injected_source(self, source_id: str) -> InjectedSource:
        await self.remove_source(source_id)
        src = InjectedSource(source_id, self.sink)
        self._injected[source_id] = src
        return src

    async def remove_source(self, source_id: str) -> None:
        task = self._tasks.pop(source_id, None)
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._injected.pop(source_id, None)

    async def stop_all(self) -> None:
        for sid in list(self._tasks) + list(self._injected):
            await self.remove_source(sid)

    def active_sources(self) -> list[str]:
        return sorted(set(self._tasks) | set(self._injected))
