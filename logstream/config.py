"""可调参数配置。

所有缓冲/保留策略都在这里集中管理，参数语义见 README.md 的「可调参数」一节。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class StreamConfig:
    """流式服务配置。

    所有队列/日志容量都是有界的，以保证「来源生产快于消费时内存也不会无界增长」。
    """

    # 状态持久化文件路径（JSON，原子写）。
    state_path: str = "logstream-state.json"

    # 文件来源轮询间隔（秒）。
    file_poll_interval: float = 0.2

    # 文件来源首次加入时从哪里开始读："beginning"（从头）或 "end"（只追新行）。
    file_start_from: str = "beginning"

    # 每个来源保留事件条数（超出后最旧事件被淘汰）。
    log_retention_events: int = 10_000

    # 每个来源保留事件的最长年龄（秒），超过年龄的最旧事件被淘汰。
    log_retention_seconds: float = 3_600.0

    # 每个订阅者的有界投递队列长度（条）。满后按丢弃策略处理。
    sub_queue_maxsize: int = 1_000

    # 假来源注入队列长度（条）。满后 inject 协程等待，形成对采集端的背压。
    source_incoming_maxsize: int = 10_000

    # 关键字过滤是否大小写敏感。
    keyword_case_sensitive: bool = True

    # 拉取事件接口的默认长轮询等待（秒）。
    fetch_default_wait: float = 0.5

    # 持久化落盘的防抖间隔（秒）。
    persist_debounce: float = 0.5

    # 每次投递/拉取的最大条数。
    fetch_limit: int = 500

    def __post_init__(self) -> None:
        if self.file_start_from not in ("beginning", "end"):
            raise ValueError("file_start_from must be 'beginning' or 'end'")
        if self.log_retention_events <= 0:
            raise ValueError("log_retention_events must be positive")
        if self.sub_queue_maxsize <= 0:
            raise ValueError("sub_queue_maxsize must be positive")
        if self.source_incoming_maxsize <= 0:
            raise ValueError("source_incoming_maxsize must be positive")
