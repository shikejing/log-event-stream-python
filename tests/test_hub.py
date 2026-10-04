import asyncio

import pytest

from logstream.hub import FanoutHub, Gap, Subscriber
from logstream.models import Event, Position, SubscriptionFilter


def ev(seq, source="s", msg="m"):
    return Event(source_id=source, seq=seq, timestamp=float(seq), message=msg)


def test_publish_reaches_all_subscribers():
    hub = FanoutHub()
    a = Subscriber("a", SubscriptionFilter())
    b = Subscriber("b", SubscriptionFilter())
    hub.add_subscriber(a)
    hub.add_subscriber(b)
    hub.publish(ev(1))
    assert a.queued == 1 and b.queued == 1


def test_filter_is_per_subscriber():
    hub = FanoutHub()
    a = Subscriber("a", SubscriptionFilter(keyword="keep"))
    b = Subscriber("b", SubscriptionFilter())
    hub.add_subscriber(a)
    hub.add_subscriber(b)
    hub.publish(ev(1, msg="keep this"))
    hub.publish(ev(2, msg="drop this"))
    assert a.queued == 1 and b.queued == 2


@pytest.mark.asyncio
async def test_slow_subscriber_drops_oldest_with_gap():
    slow = Subscriber("slow", SubscriptionFilter(), queue_size=3)
    for i in range(1, 6):
        slow.offer(ev(i))
    assert slow.dropped == 2
    first = await slow.next()
    assert isinstance(first, Gap) and first.up_to_seq == 2
    rest = [await slow.next() for _ in range(3)]
    assert [e.seq for e in rest] == [3, 4, 5]


@pytest.mark.asyncio
async def test_slow_subscriber_does_not_block_fast():
    hub = FanoutHub(queue_size=2)
    fast = hub.subscribe("fast", SubscriptionFilter())
    slow = hub.subscribe("slow", SubscriptionFilter())
    for i in range(1, 10):
        hub.publish(ev(i))
        assert (await fast.next()).seq == i  # 快订阅者实时拿到全部
    assert slow.dropped == 7
    assert slow.queued == 2


def test_ack_monotonic():
    sub = Subscriber("s", SubscriptionFilter())
    sub.ack(Position("src", 5))
    sub.ack(Position("src", 3))  # 回退的 ack 不得降低位点
    assert sub.acked == {"src": 5}
