"""日志来源：本地文件来源与进程内注入的假来源。

每个来源在独立的 asyncio 任务中采集，来源之间互不阻塞（同一来源内严格 FIFO）。
来源只负责「按产生顺序读取/接收数据并调用 broker.append 分配位点」，
位点的单调性由 broker 保证。
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Callable, Dict, Optional, Tuple

from .models import SourceStopped


class Source:
    """来源基类。"""

    kind = "base"

    def __init__(self, source_id: str) -> None:
        self.source_id = source_id
        self._stop = asyncio.Event()
        self.on_change: Optional[Callable[[], None]] = None

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self, broker: Any) -> None:  # pragma: no cover - 子类实现
        raise NotImplementedError

    def snapshot(self) -> Dict[str, Any]:  # pragma: no cover - 子类实现
        raise NotImplementedError

    def _mark_dirty(self) -> None:
        if self.on_change is not None:
            self.on_change()


class FileSource(Source):
    """本地文件来源：轮询文件新增内容，按行采集。

    - 位点由 broker 在采集时分配，文件本身不存储位点；服务重启后从持久化的
      文件偏移处继续读，来源历史位点不重置。
    - 文件被截断（size < position）或换了 inode（轮转）时，从头开始读，
      但已分配的来源内位点继续递增、不回退。
    """

    kind = "file"

    def __init__(
        self,
        source_id: str,
        path: str,
        poll_interval: float = 0.2,
        start_from: str = "beginning",
        position: Optional[int] = None,
        inode: Optional[int] = None,
    ) -> None:
        super().__init__(source_id)
        self.path = path
        self.poll_interval = poll_interval
        self.start_from = start_from
        self.position: Optional[int] = position  # 已读字节偏移
        self.inode: Optional[int] = inode
        self._buffer = ""  # 不完整的行尾
        self._lines_read = 0

    async def run(self, broker: Any) -> None:
        while not self._stop.is_set():
            try:
                await self._poll(broker)
            except asyncio.CancelledError:
                raise
            except Exception:
                # 单次轮询失败不应杀死采集任务，下一轮重试。
                pass
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.poll_interval)
            except asyncio.TimeoutError:
                pass

    async def _poll(self, broker: Any) -> None:
        try:
            stat = os.stat(self.path)
        except FileNotFoundError:
            return  # 文件还没出现，下轮再来
        except OSError:
            return

        if self.inode is None:
            # 首次采集：持久化过 inode 则沿用，否则按 start_from 决定起点。
            self.inode = stat.st_ino
            if self.position is None:
                self.position = 0 if self.start_from == "beginning" else stat.st_size
                self._mark_dirty()
        elif stat.st_ino != self.inode:
            # 文件轮转：从头读，位点继续。
            self.inode = stat.st_ino
            self.position = 0
            self._buffer = ""
            self._mark_dirty()
        elif stat.st_size < (self.position or 0):
            # 文件被截断：从头读，位点继续。
            self.position = 0
            self._buffer = ""
            self._mark_dirty()

        assert self.position is not None
        if stat.st_size == self.position:
            return

        position = self.position

        def _read() -> bytes:
            with open(self.path, "rb") as f:
                f.seek(position)
                return f.read()

        data = await asyncio.to_thread(_read)
        if not data:
            return
        self.position = position + len(data)
        self._mark_dirty()

        text = data.decode("utf-8", errors="replace")
        lines = text.split("\n")
        # 最后一段可能是不完整行，留到下轮拼接。
        self._buffer += lines[0]
        for line in lines[1:-1]:
            self._emit(broker, self._buffer)
            self._buffer = line
        if len(lines) >= 2:
            self._emit(broker, self._buffer)
            self._buffer = lines[-1]

    def _emit(self, broker: Any, line: str) -> None:
        if line == "":
            # 空行也作为事件采集？这里跳过纯空行，避免无意义事件。
            return
        broker.append(self.source_id, line, time.time())
        self._lines_read += 1

    def snapshot(self) -> Dict[str, Any]:
        try:
            stat = os.stat(self.path)
            exists = True
            size = stat.st_size
        except OSError:
            exists = False
            size = None
        return {
            "id": self.source_id,
            "type": self.kind,
            "path": self.path,
            "status": "running" if exists else "waiting",
            "file_exists": exists,
            "file_size": size,
            "position": self.position,
            "inode": self.inode,
            "lines_read": self._lines_read,
        }


class FakeSource(Source):
    """进程内假来源：通过 inject 注入事件，用于测试与模拟。

    inject 是异步的：底层是有界队列，队列满时 inject 等待，形成对采集端的
    背压；事件由采集任务按 FIFO 顺序取出并分配位点，因此多任务并发注入也
    能保证「同一来源的顺序与产生顺序一致」（队列串行化）。
    """

    kind = "fake"

    def __init__(self, source_id: str, incoming_maxsize: int = 10_000) -> None:
        super().__init__(source_id)
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=incoming_maxsize)
        self._injected = 0
        self._assigned = 0

    async def inject(self, payload: str, timestamp: Optional[float] = None) -> int:
        """注入一条事件，返回分配到的来源内位点。

        如果采集队列已满，本协程会等待（背压）。来源停止后抛出 SourceStopped。
        """
        if self._stop.is_set():
            raise SourceStopped(f"source {self.source_id!r} has been stopped")
        loop = asyncio.get_event_loop()
        fut: "asyncio.Future[int]" = loop.create_future()
        try:
            await self._queue.put((payload, timestamp, fut))
        except asyncio.CancelledError:
            if not fut.done():
                fut.cancel()
            raise
        self._injected += 1
        return await fut

    async def run(self, broker: Any) -> None:
        while not self._stop.is_set():
            try:
                item = await self._queue.get()
            except asyncio.CancelledError:
                raise
            payload, timestamp, fut = item
            try:
                offset = broker.append(self.source_id, payload, timestamp)
                self._assigned += 1
                if not fut.done():
                    fut.set_result(offset)
            except Exception as exc:  # noqa: BLE001 - 要把异常回报给注入者
                if not fut.done():
                    fut.set_exception(exc)

    def snapshot(self) -> Dict[str, Any]:
        return {
            "id": self.source_id,
            "type": self.kind,
            "status": "running" if not self._stop.is_set() else "stopped",
            "incoming_qsize": self._queue.qsize(),
            "incoming_maxsize": self._queue.maxsize,
            "injected": self._injected,
            "assigned": self._assigned,
        }
