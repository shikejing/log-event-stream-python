from logstream.models import Event, Position, SubscriptionFilter


def test_skeleton_imports():
    e = Event(source_id="s", seq=1, timestamp=0.0, message="m")
    assert e.position == Position("s", 1)
