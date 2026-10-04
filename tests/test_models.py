from logstream.models import Event, SubscriptionFilter


def ev(source="a", seq=1, ts=10.0, msg="hello world"):
    return Event(source_id=source, seq=seq, timestamp=ts, message=msg)


def test_filter_sources():
    f = SubscriptionFilter(sources=frozenset({"a"}))
    assert f.matches(ev(source="a"))
    assert not f.matches(ev(source="b"))


def test_filter_keyword_and_time_range():
    f = SubscriptionFilter(keyword="hello", start_ts=5.0, end_ts=15.0)
    assert f.matches(ev(ts=10.0))
    assert not f.matches(ev(ts=20.0))
    assert not f.matches(ev(ts=1.0))
    assert not f.matches(ev(msg="bye"))


def test_filter_none_matches_all():
    assert SubscriptionFilter().matches(ev())
