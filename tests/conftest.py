"""测试公共辅助。"""

from __future__ import annotations

import os
import tempfile
from typing import Optional

import pytest

from logstream import Service, StreamConfig


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


def make_config(tmp_dir: str, **overrides) -> StreamConfig:
    defaults = dict(
        state_path=os.path.join(tmp_dir, "state.json"),
        file_poll_interval=0.05,
        log_retention_events=10_000,
        log_retention_seconds=3_600.0,
        sub_queue_maxsize=1_000,
        source_incoming_maxsize=10_000,
        fetch_default_wait=0.5,
    )
    defaults.update(overrides)
    return StreamConfig(**defaults)


async def make_service(tmp_dir: str, **overrides) -> Service:
    svc = Service(make_config(tmp_dir, **overrides))
    await svc.start()
    return svc
