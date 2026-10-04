"""事件日志（journal）：每个来源一个追加式日志文件，负责分配位点并持久化。"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path

from .models import Event


class SourceJournal:
    """单来源的追加式事件日志。

    - seq 从 1 开始，来源内单调递增；
    - 重启后从磁盘恢复 last_seq，位点不重置；
    - 写文件加锁，保证同一来源内顺序与产生顺序一致。
    """

    def __init__(self, data_dir: Path, source_id: str) -> None:
        self.data_dir = Path(data_dir)
        self.source_id = source_id
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._path = self.data_dir / f"journal-{source_id}.jsonl"
        self._lock = threading.Lock()
        self._last_seq = self._recover_last_seq()

    @property
    def path(self) -> Path:
        return self._path

    def _recover_last_seq(self) -> int:
        if not self._path.exists():
            return 0
        last = 0
        with self._path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    last = max(last, int(json.loads(line)["seq"]))
                except (json.JSONDecodeError, KeyError, ValueError):
                    # 尾部可能存在崩溃残留的半行，忽略
                    break
        return last

    @property
    def next_seq(self) -> int:
        """下一条事件将分配的序号。"""
        return self._last_seq + 1

    def last_seq(self) -> int:
        """已分配的最大序号；无事件时返回 0。"""
        return self._last_seq

    def append(self, timestamp: float, message: str) -> Event:
        """追加一条事件并分配序号（先落盘再返回，保证不丢）。"""
        with self._lock:
            seq = self._last_seq + 1
            event = Event(source_id=self.source_id, seq=seq,
                          timestamp=timestamp, message=message)
            with self._path.open("a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "seq": event.seq,
                    "timestamp": event.timestamp,
                    "message": event.message,
                }) + "\n")
                f.flush()
                os.fsync(f.fileno())
            self._last_seq = seq
            return event

    def read_from(self, seq: int, limit: int | None = None) -> list[Event]:
        """读取 seq 及之后的事件（用于断点续传补发）。"""
        events: list[Event] = []
        if not self._path.exists():
            return events
        with self._lock, self._path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    break
                if rec["seq"] < seq:
                    continue
                events.append(Event(
                    source_id=self.source_id,
                    seq=int(rec["seq"]),
                    timestamp=float(rec["timestamp"]),
                    message=str(rec["message"]),
                ))
                if limit is not None and len(events) >= limit:
                    break
        return events
