"""服务编排：采集 -> 日志持久化 -> 分发 -> 订阅确认位点持久化。"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

from .hub import FanoutHub, Subscriber
from .journal import SourceJournal
from .models import Position, SubscriptionFilter
from .sources import SourceManager


class ResumeOutOfRange(ValueError):
    """请求的续传位点超过该来源已持久化的最大位点。"""


class LogStreamService:
    """多路日志采集与实时订阅服务。

    持久化状态（重启可恢复）：journal（事件与位点）、checkpoints（订阅者已确认位点）。
    易失状态（重启即清空）：订阅者内存队列中未完成的分发。
    """

    def __init__(self, data_dir: Path, queue_size: int = 1024) -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.hub = FanoutHub(queue_size=queue_size)
        self.sources = SourceManager(sink=self._on_event)
        self._journals: dict[str, SourceJournal] = {}
        self._publish_lock = asyncio.Lock()
        self._checkpoint_path = self.data_dir / "checkpoints.json"
        self._checkpoints: dict[str, dict[str, int]] = self._load_checkpoints()

    # ---- 生命周期 ----

    async def start(self) -> None:
        pass  # 来源通过 sources.add_* 动态加入

    async def stop(self) -> None:
        await self.sources.stop_all()

    # ---- 采集回调 ----

    def journal_for(self, source_id: str) -> SourceJournal:
        j = self._journals.get(source_id)
        if j is None:
            j = SourceJournal(self.data_dir, source_id)
            self._journals[source_id] = j
        return j

    async def _on_event(self, source_id: str, timestamp: float, message: str) -> None:
        """采集回调：先写日志分配位点（持久化），再广播（易失分发）。"""
        event = self.journal_for(source_id).append(timestamp, message)
        async with self._publish_lock:
            self.hub.publish(event)

    # ---- 订阅 ----

    async def subscribe(self, subscriber_id: str, filt: SubscriptionFilter,
                        resume: dict[str, int] | None = None) -> Subscriber:
        """注册订阅者并按位点补发历史事件。

        resume: {source_id: 已确认的最大 seq}。None 时使用持久化的 checkpoint，
        没有 checkpoint 则从头消费。位点处理策略：
        - seq < 0：回退到 0（从头消费）；
        - seq > 来源当前最大位点：抛 ResumeOutOfRange（越界）；
        - 未知来源：忽略（可能已被移除）。
        """
        if resume is None:
            resume = dict(self._checkpoints.get(subscriber_id, {}))
        async with self._publish_lock:
            sub = self.hub.subscribe(subscriber_id, filt)
            for source_id, seq in resume.items():
                journal = self._journals.get(source_id)
                if journal is None:
                    continue
                if seq < 0:
                    seq = 0
                if seq > journal.last_seq():
                    self.hub.remove_subscriber(subscriber_id)
                    raise ResumeOutOfRange(
                        f"source {source_id}: resume seq {seq} > last_seq {journal.last_seq()}")
                sub.acked[source_id] = seq
            # 补发：resume 之后、当前已持久化的全部事件（过滤条件同样生效）
            for source_id, journal in self._journals.items():
                start = resume.get(source_id, 0) + 1
                for event in journal.read_from(start):
                    sub.offer(event)
            return sub

    def ack(self, subscriber_id: str, position: Position) -> None:
        """确认位点并持久化 checkpoint。"""
        sub = self.hub.get(subscriber_id)
        if sub is not None:
            sub.ack(position)
        ck = self._checkpoints.setdefault(subscriber_id, {})
        ck[position.source_id] = max(ck.get(position.source_id, 0), position.seq)
        self._save_checkpoints()

    # ---- 可观测 ----

    def status(self) -> dict:
        """来源采集进度、订阅者位点与滞后。查询基于一致性快照，与推送并发时自洽。"""
        sources = {}
        active = set(self.sources.active_sources())
        for sid, journal in self._journals.items():
            sources[sid] = {"last_seq": journal.last_seq(), "active": sid in active}
        subscribers = {}
        for sid, sub_stats in self.hub.stats().items():
            lag = 0
            for src, journal in self._journals.items():
                delivered = sub_stats["delivered"].get(src, 0)
                lag += max(0, journal.last_seq() - delivered)
            subscribers[sid] = {**sub_stats, "lag": lag}
        return {"sources": sources, "subscribers": subscribers}

    # ---- checkpoint 持久化 ----

    def _load_checkpoints(self) -> dict[str, dict[str, int]]:
        if not self._checkpoint_path.exists():
            return {}
        try:
            with self._checkpoint_path.open("r", encoding="utf-8") as f:
                return {k: {s: int(v) for s, v in vv.items()}
                        for k, vv in json.load(f).items()}
        except (json.JSONDecodeError, ValueError):
            return {}

    def _save_checkpoints(self) -> None:
        tmp = self._checkpoint_path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(self._checkpoints, f)
            f.flush()
            os.fsync(f.fileno())
        tmp.replace(self._checkpoint_path)
