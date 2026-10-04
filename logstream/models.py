"""核心数据模型：事件、位点、订阅过滤条件。"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Position:
    """位点：来源内单调递增的序号。"""

    source_id: str
    seq: int


@dataclass(frozen=True)
class Event:
    """一条日志事件。seq 由采集端在写入日志时分配，来源内单调递增。"""

    source_id: str
    seq: int
    timestamp: float
    message: str

    @property
    def position(self) -> Position:
        return Position(self.source_id, self.seq)


@dataclass(frozen=True)
class SubscriptionFilter:
    """订阅过滤条件。过滤只影响可见性，不改变位点语义。"""

    sources: frozenset[str] | None = None
    keyword: str | None = None
    start_ts: float | None = None
    end_ts: float | None = None

    def matches(self, event: Event) -> bool:
        if self.sources is not None and event.source_id not in self.sources:
            return False
        if self.keyword is not None and self.keyword not in event.message:
            return False
        if self.start_ts is not None and event.timestamp < self.start_ts:
            return False
        if self.end_ts is not None and event.timestamp > self.end_ts:
            return False
        return True
