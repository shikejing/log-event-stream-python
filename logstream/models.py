"""数据模型：事件、缺口标记、过滤器与异常类型。

位点模型（详见 README.md「位点模型」一节）：
- 每个事件由 (source_id, offset) 唯一标识。
- offset 在来源产生时由 broker 单调分配，同一来源内严格递增、从 0 开始、不重置。
- 过滤只决定事件对订阅者是否可见，不改变 offset 语义；被过滤掉的事件不投递、无需确认。
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set


# ---------------------------------------------------------------------------
# 异常
# ---------------------------------------------------------------------------


class StreamError(Exception):
    """流式服务错误基类。"""


class UnknownSource(StreamError):
    """来源不存在（或已被移除且日志已淘汰）。"""


class UnknownSubscription(StreamError):
    """订阅者不存在。"""


class SourceExists(StreamError):
    """来源 id 已存在。"""


class OffsetOutOfRange(StreamError):
    """请求的位点超过该来源当前最新位点（指向未来）。"""

    def __init__(self, source_id: str, requested: int, latest: int) -> None:
        self.source_id = source_id
        self.requested = requested
        self.latest = latest
        super().__init__(
            f"offset {requested} for source {source_id!r} is out of range; "
            f"latest available offset is {latest}"
        )


class OffsetUnavailable(StreamError):
    """请求的位点已因保留策略被淘汰（指向过去的空洞）。"""

    def __init__(self, source_id: str, requested: int, oldest_available: int) -> None:
        self.source_id = source_id
        self.requested = requested
        self.oldest_available = oldest_available
        super().__init__(
            f"offset {requested} for source {source_id!r} is no longer available; "
            f"oldest retained offset is {oldest_available}"
        )


class InvalidSeek(StreamError):
    """非法的位点请求（负数等）。"""


class SourceStopped(StreamError):
    """来源已停止，不能再注入。"""


# ---------------------------------------------------------------------------
# 事件与缺口
# ---------------------------------------------------------------------------


@dataclass
class Event:
    """一条日志事件。"""

    source_id: str
    offset: int
    timestamp: float  # 产生时间（epoch 秒），由采集端在分配位点时确定
    payload: str
    kind: str = "data"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "event",
            "source_id": self.source_id,
            "offset": self.offset,
            "timestamp": self.timestamp,
            "payload": self.payload,
        }


@dataclass
class Gap:
    """缺口标记：表示某来源 [from_offset, to_offset] 区间内的事件对该订阅者不可见。

    缺口产生于两种情况：
    1. 订阅者有界投递队列满后按丢弃策略丢弃了未确认事件（慢订阅者降级）；
    2. 订阅者断线期间来源日志按保留策略淘汰了旧事件，重连后请求的位点已不可用。
    缺口标记本身按普通事件的顺序投递，订阅者确认（ack）越过缺口即表示已知晓。
    """

    source_id: str
    from_offset: int
    to_offset: int
    kind: str = "gap"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "gap",
            "source_id": self.source_id,
            "from_offset": self.from_offset,
            "to_offset": self.to_offset,
        }


QueueItem = Any  # Event | Gap


# ---------------------------------------------------------------------------
# 过滤器
# ---------------------------------------------------------------------------


@dataclass
class Filter:
    """订阅过滤条件。

    sources: 允许的来源集合，None 表示所有来源。
    keyword: 关键字子串匹配（大小写由配置决定），None 表示不限制。
    时间范围为左闭右开 [start_time, end_time)，None 表示不限制。
    """

    sources: Optional[Set[str]] = None
    keyword: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    case_sensitive: bool = True

    def matches(self, event: Event) -> bool:
        if self.sources is not None and event.source_id not in self.sources:
            return False
        if self.start_time is not None and event.timestamp < self.start_time:
            return False
        if self.end_time is not None and event.timestamp >= self.end_time:
            return False
        if self.keyword is not None:
            if self.case_sensitive:
                if self.keyword not in event.payload:
                    return False
            else:
                if self.keyword.lower() not in event.payload.lower():
                    return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sources": sorted(self.sources) if self.sources is not None else None,
            "keyword": self.keyword,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "case_sensitive": self.case_sensitive,
        }

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]], case_sensitive: bool = True) -> "Filter":
        if not data:
            return Filter(case_sensitive=case_sensitive)
        sources = data.get("sources")
        return cls(
            sources=set(sources) if sources else None,
            keyword=data.get("keyword"),
            start_time=data.get("start_time"),
            end_time=data.get("end_time"),
            case_sensitive=bool(data.get("case_sensitive", case_sensitive)),
        )


# ---------------------------------------------------------------------------
# 规格描述（持久化用）
# ---------------------------------------------------------------------------


@dataclass
class SourceSpec:
    id: str
    type: str  # "file" | "fake"
    path: Optional[str] = None
    created_at: float = field(default_factory=lambda: time.time())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "path": self.path,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SourceSpec":
        return cls(
            id=data["id"],
            type=data["type"],
            path=data.get("path"),
            created_at=data.get("created_at", time.time()),
        )


@dataclass
class SubscriptionSpec:
    id: str
    filter: Filter
    created_at: float = field(default_factory=lambda: time.time())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "filter": self.filter.to_dict(),
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any], case_sensitive: bool = True) -> "SubscriptionSpec":
        return cls(
            id=data["id"],
            filter=Filter.from_dict(data.get("filter"), case_sensitive=case_sensitive),
            created_at=data.get("created_at", time.time()),
        )
