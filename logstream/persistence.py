"""状态持久化。

持久化的内容（已提交位点）：
- sources: 来源规格 + 已分配的最后位点 last_offset + 文件来源的读取位置/inode；
- subscriptions: 订阅规格（过滤条件）+ 各来源已确认位点 acked。

不持久化的内容（未完成的分发状态，重启后重建/重新投递）：
- 订阅者的投递队列、已投递未确认（delivered 但未 ack）的状态、丢弃计数；
- 假来源采集队列中尚未分配位点的注入项。

重启后：来源从 last_offset+1 继续分配位点（历史位点不重置）；订阅者从 acked+1
继续投递，已确认事件不重复推送，未确认事件不跳过（at-least-once）。
"""

from __future__ import annotations

import json
import os
import tempfile
from typing import Any, Dict


class StateStore:
    def __init__(self, path: str) -> None:
        self.path = path

    def load(self) -> Dict[str, Any]:
        if not os.path.exists(self.path):
            return {"version": 1, "sources": {}, "subscriptions": {}}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            # 状态文件损坏不应让服务起不来：备份后从空状态开始。
            backup = self.path + ".corrupt"
            try:
                os.replace(self.path, backup)
            except OSError:
                pass
            return {"version": 1, "sources": {}, "subscriptions": {}}
        data.setdefault("version", 1)
        data.setdefault("sources", {})
        data.setdefault("subscriptions", {})
        return data

    def save(self, state: Dict[str, Any]) -> None:
        """原子写：先写同目录临时文件，再 os.replace，避免半写状态。"""
        state["version"] = 1
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(prefix=".logstream-state-", suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self.path)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise


class EventLogStore:
    """每个来源一个 append-only 事件文件，保证事件在重启后可重放。

    - 事件在 broker.append 时追加写入（flush 到 OS，不 fsync，权衡性能；
      优雅停止时会重写为保留区间）。
    - 文件在停止/落盘时按内存保留区间重写，避免无限增长。
    """

    def __init__(self, directory: str) -> None:
        self.directory = directory
        os.makedirs(directory, exist_ok=True)

    def _path(self, source_id: str) -> str:
        safe = source_id.replace("/", "_").replace(os.sep, "_")
        return os.path.join(self.directory, f"{safe}.events")

    def append(self, source_id: str, event: "Event") -> None:  # noqa: F821
        with open(self._path(source_id), "a", encoding="utf-8") as f:
            f.write(json.dumps(event.to_dict(), ensure_ascii=False) + "\n")
            f.flush()

    def read_events(self, source_id: str):
        from .models import Event

        events = []
        path = self._path(source_id)
        if not os.path.exists(path):
            return events
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    continue
                events.append(Event(d["source_id"], d["offset"], d["timestamp"], d["payload"]))
        return events

    def count(self, source_id: str) -> int:
        path = self._path(source_id)
        if not os.path.exists(path):
            return 0
        with open(path, "r", encoding="utf-8") as f:
            return sum(1 for line in f if line.strip())

    def rewrite(self, source_id: str, events) -> None:
        """用内存中保留的事件重写文件（压缩）。"""
        path = self._path(source_id)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            for ev in events:
                f.write(json.dumps(ev.to_dict(), ensure_ascii=False) + "\n")
            f.flush()
        os.replace(tmp, path)
