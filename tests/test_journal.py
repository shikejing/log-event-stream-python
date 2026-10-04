from logstream.journal import SourceJournal


def test_append_assigns_monotonic_seq(tmp_path):
    j = SourceJournal(tmp_path, "s1")
    e1 = j.append(1.0, "a")
    e2 = j.append(2.0, "b")
    assert (e1.seq, e2.seq) == (1, 2)
    assert j.last_seq() == 2
    assert j.next_seq == 3


def test_restart_keeps_position(tmp_path):
    j = SourceJournal(tmp_path, "s1")
    j.append(1.0, "a")
    j.append(2.0, "b")
    # 模拟重启：重新打开同一目录
    j2 = SourceJournal(tmp_path, "s1")
    assert j2.last_seq() == 2
    e = j2.append(3.0, "c")
    assert e.seq == 3  # 位点不因重启而重置


def test_read_from_for_resume(tmp_path):
    j = SourceJournal(tmp_path, "s1")
    for i in range(1, 6):
        j.append(float(i), f"m{i}")
    events = j.read_from(3)
    assert [e.seq for e in events] == [3, 4, 5]
    assert [e.seq for e in j.read_from(4, limit=1)] == [4]
    assert j.read_from(99) == []


def test_corrupt_tail_ignored(tmp_path):
    j = SourceJournal(tmp_path, "s1")
    j.append(1.0, "a")
    with j.path.open("a") as f:
        f.write('{"seq": 2, "times')  # 崩溃残留的半行
    j2 = SourceJournal(tmp_path, "s1")
    assert j2.last_seq() == 1
