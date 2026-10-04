"""分发中心：多订阅者共享事件流，各自按自己的进度消费，慢订阅者不拖垮整体。"""
from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass

from .models import Event, Position, SubscriptionFilter


@dataclass(frozen=True)
class Gap:
    """空洞标记：该订阅者因队列满被丢弃了 source_id 来源中 seq <= up_to_seq 的事件。

    收到 Gap 后订阅者可以选择从 journal 补读，或接受丢失继续。
    """

    source_id: str
    up_to_seq: int


class Subscriber:
    """单个订阅者：有界队列 + 过滤条件 + 已确认位点。

    慢订阅者策略：队列满时丢弃最旧事件，插入 Gap 标记并计数，
    绝不阻塞 publish，因此不会拖垮快订阅者，内存占用有界。
    """

    def __init__(self, subscriber_id: str, filt: SubscriptionFilter, queue_size: int = 1024) -> None:
        self.subscriber_id = subscriber_id
        self.filter = filt
        self.maxsize = queue_size
        self.dropped = 0
        self._queue: deque[Event | Gap] = deque()
        self._event_count = 0  # 队列中 Event 的数量（Gap 不占额度）
        self._not_empty = asyncio.Event()
        self.acked: dict[str, int] = {}       # 每来源已确认的最大 seq
        self.delivered: dict[str, int] = {}   # 每来源已投递（取出）的最大 seq
        self._enqueued_hi: dict[str, int] = {}  # 每来源已入队的最大 seq（去重高水位）

    @property
    def queued(self) -> int:
        return self._event_count

    def offer(self, event: Event) -> None:
        """投递事件；队列满时丢弃最旧事件并留下 Gap 标记。

        同一来源 seq 不大于已入队高水位的事件视为重复（补发与实时推送
        的竞争窗口），直接跳过。
        """
        if event.seq <= self._enqueued_hi.get(event.source_id, 0):
            return
        if not self.filter.matches(event):
            return
        self._enqueued_hi[event.source_id] = event.seq
        if self._event_count >= self.maxsize:
            gap_hi: dict[str, int] = {}
            while True:
                item = self._queue.popleft()
                if isinstance(item, Gap):
                    gap_hi[item.source_id] = max(gap_hi.get(item.source_id, 0), item.up_to_seq)
                else:
                    gap_hi[item.source_id] = max(gap_hi.get(item.source_id, 0), item.seq)
                    self._event_count -= 1
                    self.dropped += 1
                    break
            for src, hi in gap_hi.items():
                self._queue.appendleft(Gap(src, hi))
        self._queue.append(event)
        self._event_count += 1
        self._not_empty.set()

    async def next(self, timeout: float | None = None) -> Event | Gap:
        """取下一条消息（Event 或 Gap）。"""

        async def _take() -> Event | Gap:
            while not self._queue:
                self._not_empty.clear()
                await self._not_empty.wait()
            item = self._queue.popleft()
            if isinstance(item, Event):
                self._event_count -= 1
                prev = self.delivered.get(item.source_id, 0)
                self.delivered[item.source_id] = max(prev, item.seq)
            return item

        if timeout is None:
            return await _take()
        return await asyncio.wait_for(_take(), timeout)

    def ack(self, position: Position) -> None:
        """确认位点：该位点及之前（同一来源）的事件不再重发。"""
        prev = self.acked.get(position.source_id, 0)
        self.acked[position.source_id] = max(prev, position.seq)


class FanoutHub:
    """向所有订阅者广播事件。offer 是同步内存操作，订阅者之间互不阻塞。"""

    def __init__(self, queue_size: int = 1024) -> None:
        self.queue_size = queue_size
        self._subscribers: dict[str, Subscriber] = {}

    def subscribe(self, subscriber_id: str, filt: SubscriptionFilter) -> Subscriber:
        """创建并注册订阅者，队列大小由 hub 统一配置。"""
        sub = Subscriber(subscriber_id, filt, queue_size=self.queue_size)
        self.add_subscriber(sub)
        return sub

    def add_subscriber(self, sub: Subscriber) -> None:
        self._subscribers[sub.subscriber_id] = sub

    def remove_subscriber(self, subscriber_id: str) -> None:
        self._subscribers.pop(subscriber_id, None)

    def get(self, subscriber_id: str) -> Subscriber | None:
        return self._subscribers.get(subscriber_id)

    def publish(self, event: Event) -> None:
        for sub in self._subscribers.values():
            sub.offer(event)

    def stats(self) -> dict:
        return {
            sid: {
                "queued": sub.queued,
                "dropped": sub.dropped,
                "acked": dict(sub.acked),
                "delivered": dict(sub.delivered),
            }
            for sid, sub in self._subscribers.items()
        }
