"""事件代理：来源日志、订阅者队列、位点推进与丢弃策略。

核心不变量：
- 每个来源的 offset 由 broker 在 append 时单调分配，同一来源内从 0 递增、不重置。
- 过滤只决定事件对订阅者是否可见；被过滤的事件不进队列、无需 ack，但 delivered
  位点照常推进（位点语义不因过滤而改变）。
- 每个订阅者的投递队列有界；满后按「丢弃最旧未确认事件 + 合并缺口标记」降级，
  dropped_total 可观测，快订阅者不受慢订阅者影响。
- 所有状态变更都是同步的（不 await），因此 snapshot() 与并发推送同时进行时结果自洽。
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from .config import StreamConfig
from .models import (
    Event,
    Filter,
    Gap,
    InvalidSeek,
    OffsetOutOfRange,
    OffsetUnavailable,
    SourceExists,
    UnknownSource,
    UnknownSubscription,
)

QueueItem = Any  # Event | Gap


# ---------------------------------------------------------------------------
# 来源日志（有界保留）
# ---------------------------------------------------------------------------


class EventLog:
    """单个来源的有界事件日志。"""

    def __init__(self, source_id: str, retention_events: int, retention_seconds: float) -> None:
        self.source_id = source_id
        self.retention_events = retention_events
        self.retention_seconds = retention_seconds
        self._events: deque = deque()
        self.next_offset = 0
        self.active = True  # 来源是否在采集；移除后日志保留但不再 append

    @property
    def oldest_offset(self) -> int:
        return self._events[0].offset if self._events else self.next_offset

    @property
    def latest_offset(self) -> int:
        return self.next_offset - 1

    @property
    def events(self) -> deque:
        return self._events

    def append(self, ev: Event) -> None:
        self._events.append(ev)
        self.next_offset = ev.offset + 1
        self.compact()

    def replay(self, ev: Event) -> None:
        """重启恢复时追加历史事件（不改变 next_offset，之后统一 compact）。"""
        self._events.append(ev)

    def get(self, offset: int) -> Optional[Event]:
        if not self._events:
            return None
        oldest = self._events[0].offset
        if offset < oldest or offset >= self.next_offset:
            return None
        return self._events[offset - oldest]

    def compact(self) -> None:
        now = time.time()
        while len(self._events) > self.retention_events:
            self._events.popleft()
        if self.retention_seconds > 0:
            while self._events and now - self._events[0].timestamp > self.retention_seconds:
                self._events.popleft()


# ---------------------------------------------------------------------------
# 订阅者
# ---------------------------------------------------------------------------


@dataclass
class Subscription:
    id: str
    filter: Filter
    created_at: float
    # 每个来源「已确认处理」的最后位点（含），-1 表示尚未确认任何事件。
    acked: Dict[str, int] = field(default_factory=dict)
    # 每个来源 broker 已投递/跳过的最后位点（含），-1 表示尚未开始。
    delivered: Dict[str, int] = field(default_factory=dict)
    # 待投递（已投递未确认）事件队列，只装 Event，长度有界。
    queue: deque = field(default_factory=deque)
    # 本订阅者队列长度上限（None 用全局配置）。
    queue_maxsize: Optional[int] = None
    # 缺口标记：{source_id: [from_offset, to_offset]}，按来源合并、递增。
    # 拉取时缺口先于队列事件返回（同来源内保持位点顺序）。
    gaps: Dict[str, List[List[int]]] = field(default_factory=dict)
    dropped_total: int = 0
    notify: asyncio.Event = field(default_factory=asyncio.Event)


# ---------------------------------------------------------------------------
# 代理
# ---------------------------------------------------------------------------


class Broker:
    def __init__(self, config: StreamConfig, event_store: Optional["EventLogStore"] = None) -> None:  # noqa: F821
        self.config = config
        self.event_store = event_store
        self._logs: Dict[str, EventLog] = {}
        self._subs: Dict[str, Subscription] = {}
        self.on_change: Optional[Callable[[], None]] = None

    def _mark_dirty(self) -> None:
        if self.on_change is not None:
            self.on_change()

    # -- 来源管理 -----------------------------------------------------------

    def add_log(self, source_id: str) -> EventLog:
        if source_id in self._logs:
            log = self._logs[source_id]
            log.active = True  # 重新加入：日志与历史位点保留
            return log
        log = EventLog(source_id, self.config.log_retention_events, self.config.log_retention_seconds)
        self._logs[source_id] = log
        # 对已有订阅者：新来源加入时尚无事件，默认从位点 0 开始投递。
        for sub in self._subs.values():
            if source_id not in sub.delivered:
                sub.delivered[source_id] = -1
                sub.acked.setdefault(source_id, -1)
        self._mark_dirty()
        return log

    def remove_log(self, source_id: str) -> None:
        log = self._logs.get(source_id)
        if log is None:
            raise UnknownSource(source_id)
        log.active = False  # 停止采集，日志保留供订阅者继续追赶
        self._mark_dirty()

    def has_log(self, source_id: str) -> bool:
        return source_id in self._logs

    # -- 事件写入 -----------------------------------------------------------

    def append(self, source_id: str, payload: str, timestamp: Optional[float] = None) -> int:
        log = self._logs.get(source_id)
        if log is None or not log.active:
            raise UnknownSource(f"source {source_id!r} is not available")
        offset = log.next_offset
        ts = timestamp if timestamp is not None else time.time()
        ev = Event(source_id, offset, ts, payload)
        log.append(ev)
        if self.event_store is not None:
            self.event_store.append(source_id, ev)
        for sub in self._subs.values():
            self._catch_up(sub, source_id, up_to=offset)
        self._mark_dirty()
        return offset

    def _catch_up(self, sub: Subscription, source_id: str, up_to: Optional[int] = None) -> None:
        """把订阅者在某来源上的位点从 delivered 推进到 up_to，期间事件按过滤条件入队。"""
        log = self._logs.get(source_id)
        if log is None:
            return
        if up_to is None:
            up_to = log.latest_offset
        if up_to < 0:
            return
        if source_id not in sub.delivered:
            # 兜底：正常情况下 add_log / create_subscription 已设置位点。
            sub.delivered[source_id] = -1
        start = sub.delivered.get(source_id, -1) + 1
        if start > up_to:
            return
        oldest = log.oldest_offset
        if start < oldest:
            # 断线/落后期间日志已淘汰：记录缺口，再从最旧保留位点继续。
            self._add_gap(sub, source_id, start, oldest - 1)
            start = oldest
        for off in range(start, up_to + 1):
            ev = log.get(off)
            if ev is None:
                continue
            if sub.filter.matches(ev):
                self._enqueue(sub, ev)
            sub.delivered[source_id] = off

    def _enqueue(self, sub: Subscription, item: QueueItem) -> None:
        """入队；队列满时按丢弃策略降级：丢最旧未确认事件，并入缺口列表。

        队列只装事件（长度有界）；被丢弃事件的位点区间合并进 sub.gaps，
        拉取时缺口先于队列事件返回，保证同来源内位点顺序。
        """
        q = sub.queue
        maxsize = sub.queue_maxsize or self.config.sub_queue_maxsize
        while len(q) >= maxsize:
            old = q.popleft()
            sub.dropped_total += 1
            self._add_gap(sub, old.source_id, old.offset, old.offset)
        q.append(item)
        sub.notify.set()

    @staticmethod
    def _add_gap(sub: Subscription, source_id: str, lo: int, hi: int) -> None:
        """合并缺口区间到 sub.gaps[source_id]（保持递增、相邻/重叠则合并）。"""
        ranges = sub.gaps.get(source_id)
        if ranges is None:
            sub.gaps[source_id] = [[lo, hi]]
            return
        # 找到插入位置
        idx = 0
        while idx < len(ranges) and ranges[idx][1] < lo - 1:
            idx += 1
        # 合并重叠/相邻区间
        while idx < len(ranges) and ranges[idx][0] <= hi + 1:
            lo = min(lo, ranges[idx][0])
            hi = max(hi, ranges[idx][1])
            ranges.pop(idx)
        ranges.insert(idx, [lo, hi])

    @staticmethod
    def _pop_due_gaps(sub: Subscription) -> List[Gap]:
        """取出所有已到期的缺口（缺口比队列事件更旧，全部到期）。"""
        out: List[Gap] = []
        for source_id in sorted(sub.gaps):
            for lo, hi in sub.gaps[source_id]:
                out.append(Gap(source_id, lo, hi))
        sub.gaps.clear()
        return out

    @staticmethod
    def _discard_gaps_before(sub: Subscription, source_id: str, before: int) -> None:
        """丢弃某来源上端点 < before 的缺口（已被 ack/seek 覆盖）。"""
        ranges = sub.gaps.get(source_id)
        if ranges is None:
            return
        kept = [[lo, hi] for lo, hi in ranges if hi >= before]
        if kept:
            sub.gaps[source_id] = kept
        else:
            sub.gaps.pop(source_id, None)

    # -- 订阅管理 -----------------------------------------------------------

    def create_subscription(
        self,
        sub_id: str,
        flt: Filter,
        offsets: Optional[Dict[str, int]] = None,
        queue_maxsize: Optional[int] = None,
    ) -> Subscription:
        if sub_id in self._subs:
            raise SourceExists(f"subscription {sub_id!r} already exists")
        sub = Subscription(
            id=sub_id,
            filter=flt,
            created_at=time.time(),
            queue_maxsize=queue_maxsize,
        )
        offsets = offsets or {}
        for source_id, log in self._logs.items():
            nxt = self._validate_next(source_id, log, offsets.get(source_id, log.latest_offset + 1))
            sub.acked[source_id] = nxt - 1
            sub.delivered[source_id] = nxt - 1
        self._subs[sub_id] = sub
        for source_id in self._logs:
            self._catch_up(sub, source_id)
        sub.notify.set()
        self._mark_dirty()
        return sub

    def remove_subscription(self, sub_id: str) -> None:
        if sub_id not in self._subs:
            raise UnknownSubscription(sub_id)
        del self._subs[sub_id]
        self._mark_dirty()

    def restore_subscription(self, sub_id: str, flt: Filter, acked: Dict[str, int]) -> Subscription:
        """重启恢复：只恢复已确认位点（ack），投递队列重建。

        已确认位点不重置历史；若确认位点之后的日志已被淘汰，_catch_up 会在
        delivered -> acked+1 的追赶中自动投递缺口标记（未确认事件不跳过）。
        """
        if sub_id in self._subs:
            raise SourceExists(f"subscription {sub_id!r} already exists")
        sub = Subscription(id=sub_id, filter=flt, created_at=time.time())
        for source_id, log in self._logs.items():
            a = int(acked.get(source_id, -1))
            if a > log.latest_offset:
                a = log.latest_offset
            sub.acked[source_id] = a
            sub.delivered[source_id] = a
        self._subs[sub_id] = sub
        for source_id in self._logs:
            self._catch_up(sub, source_id)
        sub.notify.set()
        return sub

    def resume(self, sub_id: str) -> None:
        """断线重连：把 delivered 重置到 acked，清空在途队列，重新追赶。

        已确认事件不重复推送（从 acked+1 开始）；未确认事件重新投递，
        若期间日志已淘汰则通过缺口标记告知。
        """
        sub = self._subs.get(sub_id)
        if sub is None:
            raise UnknownSubscription(sub_id)
        for source_id in self._logs:
            sub.delivered[source_id] = sub.acked.get(source_id, -1)
        sub.queue.clear()
        sub.gaps.clear()
        for source_id in self._logs:
            self._catch_up(sub, source_id)
        sub.notify.set()
        self._mark_dirty()

    def _validate_next(self, source_id: str, log: EventLog, nxt: int) -> int:
        if nxt < 0:
            raise InvalidSeek(f"next offset must be >= 0, got {nxt}")
        if nxt > log.latest_offset + 1:
            raise OffsetOutOfRange(source_id, nxt, log.latest_offset)
        if nxt < log.oldest_offset:
            raise OffsetUnavailable(source_id, nxt, log.oldest_offset)
        return nxt

    def seek(self, sub_id: str, offsets: Dict[str, int]) -> None:
        """断点续传/回退：offsets 为各来源「下一个要投递的位点」。

        - 回退到已保留区间内的位点是明确允许的（重新投递）。
        - 指向未来（> latest+1）抛 OffsetOutOfRange。
        - 指向已淘汰位点（< oldest）抛 OffsetUnavailable。
        全部校验通过后才修改状态（原子性）。
        """
        sub = self._subs.get(sub_id)
        if sub is None:
            raise UnknownSubscription(sub_id)
        validated: Dict[str, int] = {}
        for source_id, nxt in offsets.items():
            log = self._logs.get(source_id)
            if log is None:
                raise UnknownSource(source_id)
            validated[source_id] = self._validate_next(source_id, log, nxt)
        for source_id, nxt in validated.items():
            sub.acked[source_id] = nxt - 1
            sub.delivered[source_id] = nxt - 1
            # 清掉该来源队列中位点 >= nxt 的项，之后重新投递。
            sub.queue = deque(
                item
                for item in sub.queue
                if not (item.source_id == source_id and self._item_lo(item) >= nxt)
            )
            # 清掉该来源的缺口，交由 catch_up 按日志保留情况重新生成。
            sub.gaps.pop(source_id, None)
        for source_id in self._logs:
            self._catch_up(sub, source_id)
        sub.notify.set()
        self._mark_dirty()

    def ack(self, sub_id: str, offsets: Dict[str, int]) -> None:
        """确认投递：offsets 为各来源「已处理的最后位点」（含），单调推进。"""
        sub = self._subs.get(sub_id)
        if sub is None:
            raise UnknownSubscription(sub_id)
        for source_id, off in offsets.items():
            log = self._logs.get(source_id)
            if log is None:
                raise UnknownSource(source_id)
            if off < -1 or off > log.latest_offset:
                raise InvalidSeek(f"ack offset must be in [-1, {log.latest_offset}], got {off}")
            if off > sub.acked.get(source_id, -1):
                sub.acked[source_id] = off
            if off > sub.delivered.get(source_id, -1):
                # 确认了尚未投递的位点：视为跳过（等价于 seek 到 off+1）。
                sub.delivered[source_id] = off
            # 移除队列中已确认的项（缺口标记按区间结束位点判定）。
            sub.queue = deque(
                item
                for item in sub.queue
                if not (item.source_id == source_id and self._item_end(item) <= off)
            )
            # 移除已确认覆盖的缺口。
            self._discard_gaps_before(sub, source_id, off + 1)
        sub.notify.set()
        self._mark_dirty()

    @staticmethod
    def _item_lo(item: QueueItem) -> int:
        return item.from_offset if isinstance(item, Gap) else item.offset

    @staticmethod
    def _item_end(item: QueueItem) -> int:
        return item.to_offset if isinstance(item, Gap) else item.offset

    # -- 拉取 ----------------------------------------------------------------

    async def fetch(self, sub_id: str, limit: Optional[int] = None, wait: Optional[float] = None) -> List[QueueItem]:
        sub = self._subs.get(sub_id)
        if sub is None:
            raise UnknownSubscription(sub_id)
        if limit is None:
            limit = self.config.fetch_limit
        if wait is None:
            wait = self.config.fetch_default_wait
        if not sub.queue and not sub.gaps and wait > 0:
            try:
                await asyncio.wait_for(sub.notify.wait(), timeout=wait)
            except asyncio.TimeoutError:
                pass
            sub.notify.clear()
        return self._pop_items(sub, limit)

    def _pop_items(self, sub: Subscription, limit: int) -> List[QueueItem]:
        """弹出待投递项：缺口先于队列事件（同来源内位点顺序）。"""
        items: List[QueueItem] = self._pop_due_gaps(sub)
        while sub.queue and len(items) < limit:
            items.append(sub.queue.popleft())
        return items

    # -- 可观测查询 -----------------------------------------------------------

    def snapshot(self) -> Dict[str, Any]:
        """同步快照：不 await，与并发推送同时进行时结果自洽。"""
        sources: Dict[str, Any] = {}
        for sid, log in self._logs.items():
            sources[sid] = {
                "id": sid,
                "active": log.active,
                "latest_offset": log.latest_offset,
                "oldest_offset": log.oldest_offset,
                "next_offset": log.next_offset,
                "retained": len(log.events),
            }
        subs: Dict[str, Any] = {}
        for sid, sub in self._subs.items():
            lag: Dict[str, int] = {}
            for src, log in self._logs.items():
                lag[src] = log.latest_offset - sub.acked.get(src, -1)
            subs[sid] = {
                "id": sid,
                "filter": sub.filter.to_dict(),
                "acked": dict(sub.acked),
                "delivered": dict(sub.delivered),
                "pending": len(sub.queue),
                "pending_gaps": sum(len(r) for r in sub.gaps.values()),
                "dropped_total": sub.dropped_total,
                "lag": lag,
            }
        return {"sources": sources, "subscriptions": subs}

    def get_subscription(self, sub_id: str) -> Subscription:
        sub = self._subs.get(sub_id)
        if sub is None:
            raise UnknownSubscription(sub_id)
        return sub

    def get_log(self, source_id: str) -> Optional[EventLog]:
        return self._logs.get(source_id)

    def list_subscription_ids(self) -> List[str]:
        return list(self._subs)

    def list_source_ids(self) -> List[str]:
        return list(self._logs)
