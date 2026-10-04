"""流式服务门面：组装来源、代理与持久化，管理生命周期。

用法：
    service = Service(StreamConfig(state_path="state.json"))
    await service.start()
    ...
    await service.stop()
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Dict, List, Optional

from .broker import Broker
from .config import StreamConfig
from .models import (
    Filter,
    SourceExists,
    SourceSpec,
    SourceStopped,
    SubscriptionSpec,
    UnknownSource,
)
from .persistence import StateStore
from .sources import FakeSource, FileSource, Source

logger = logging.getLogger("logstream")


class Service:
    def __init__(self, config: Optional[StreamConfig] = None) -> None:
        self.config = config or StreamConfig()
        from .persistence import EventLogStore

        state_dir = os.path.dirname(os.path.abspath(self.config.state_path))
        self.event_store = EventLogStore(os.path.join(state_dir, "events"))
        self.broker = Broker(self.config, event_store=self.event_store)
        self.store = StateStore(self.config.state_path)
        self._sources: Dict[str, Source] = {}
        self._specs: Dict[str, SourceSpec] = {}
        self._sub_specs: Dict[str, SubscriptionSpec] = {}
        self._tasks: Dict[str, "asyncio.Task"] = {}
        self._flush_task: Optional["asyncio.Task"] = None
        self._stop: Optional[asyncio.Event] = None
        self._dirty: Optional[asyncio.Event] = None
        self._started = False

    # -- 生命周期 -------------------------------------------------------------

    async def start(self) -> None:
        if self._started:
            return
        self._stop = asyncio.Event()
        self._dirty = asyncio.Event()
        self.broker.on_change = self._mark_dirty
        state = self.store.load()

        # 恢复来源：日志位点从 last_offset+1 继续，文件来源恢复读取位置。
        for sid, sentry in state.get("sources", {}).items():
            spec = SourceSpec.from_dict(sentry["spec"])
            log = self.broker.add_log(sid)
            log.next_offset = int(sentry.get("last_offset", -1)) + 1
            # 重放持久化的事件（只保留权威位点范围内的），使未确认事件可重新投递。
            for ev in self.event_store.read_events(sid):
                if ev.offset < log.next_offset:
                    log.replay(ev)
            log.compact()
            source = self._make_source(spec, sentry)
            self._specs[sid] = spec
            self._sources[sid] = source
            source.on_change = self._mark_dirty
            self._tasks[sid] = asyncio.create_task(self._run_source(sid))

        # 恢复订阅：只恢复已确认位点（ack），投递队列重建（未完成分发状态不持久化）。
        for sid, sentry in state.get("subscriptions", {}).items():
            spec = SubscriptionSpec.from_dict(
                sentry["spec"], case_sensitive=self.config.keyword_case_sensitive
            )
            self._sub_specs[sid] = spec
            self.broker.restore_subscription(sid, spec.filter, sentry.get("acked", {}))

        self._flush_task = asyncio.create_task(self._flush_loop())
        self._started = True

    async def stop(self) -> None:
        if not self._started or self._stop is None:
            return
        self._stop.set()
        for source in self._sources.values():
            source.request_stop()
        tasks = list(self._tasks.values())
        for task in tasks:
            task.cancel()
        if self._flush_task is not None:
            self._flush_task.cancel()
        await asyncio.gather(*tasks, self._flush_task, return_exceptions=True)
        self._tasks.clear()
        # 最后落盘一次，并用内存保留区间压缩事件文件。
        if self._dirty is not None:
            self._dirty.set()
        try:
            self.store.save(self._build_state())
            self._compact_event_files()
        except Exception:
            logger.exception("failed to persist state on stop")
        self._started = False

    def _compact_event_files(self) -> None:
        """用各来源内存中保留的事件重写事件文件，避免文件无限增长。"""
        for sid in self.broker.list_source_ids():
            log = self.broker.get_log(sid)
            if log is not None:
                self.event_store.rewrite(sid, list(log.events))

    async def _run_source(self, source_id: str) -> None:
        source = self._sources[source_id]
        try:
            await source.run(self.broker)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("source %r crashed", source_id)

    def _mark_dirty(self) -> None:
        if self._dirty is not None:
            self._dirty.set()

    async def _flush_loop(self) -> None:
        assert self._stop is not None
        assert self._dirty is not None
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.config.persist_debounce)
            except asyncio.TimeoutError:
                pass
            if self._dirty.is_set():
                self._dirty.clear()
                try:
                    self.store.save(self._build_state())
                    self._maybe_compact_event_files()
                except Exception:
                    logger.exception("failed to persist state")

    def _maybe_compact_event_files(self) -> None:
        """事件文件过大（超过保留条数 2 倍）时，用内存保留区间压缩。"""
        for sid in self.broker.list_source_ids():
            log = self.broker.get_log(sid)
            if log is None:
                continue
            try:
                count = self.event_store.count(sid)
            except OSError:
                continue
            if count > self.config.log_retention_events * 2:
                self.event_store.rewrite(sid, list(log.events))

    # -- 来源管理 -------------------------------------------------------------

    def add_source(
        self,
        source_type: str,
        source_id: Optional[str] = None,
        path: Optional[str] = None,
    ) -> Source:
        if source_id is None:
            source_id = self._new_id("source", self._specs)
        if source_id in self._sources:
            raise SourceExists(f"source {source_id!r} already exists")
        if source_type not in ("file", "fake"):
            raise ValueError("source_type must be 'file' or 'fake'")
        if source_type == "file" and path is None:
            raise ValueError("file source requires path")
        spec = SourceSpec(id=source_id, type=source_type, path=path)
        self.broker.add_log(source_id)
        source = self._make_source(spec, None)
        self._specs[source_id] = spec
        self._sources[source_id] = source
        source.on_change = self._mark_dirty
        self._tasks[source_id] = asyncio.create_task(self._run_source(source_id))
        self._mark_dirty()
        return source

    async def remove_source(self, source_id: str) -> None:
        source = self._sources.get(source_id)
        if source is None:
            raise UnknownSource(f"source {source_id!r} does not exist")
        source.request_stop()
        task = self._tasks.pop(source_id, None)
        if task is not None:
            try:
                await asyncio.wait_for(task, timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                task.cancel()
        # 从活动来源移除（规格与日志保留，位点不重置）；重新加入时复用。
        self._sources.pop(source_id, None)
        self.broker.remove_log(source_id)
        self._mark_dirty()

    def _make_source(self, spec: SourceSpec, sentry: Optional[Dict[str, Any]]) -> Source:
        if spec.type == "file":
            position = sentry.get("file_position") if sentry else None
            inode = sentry.get("file_inode") if sentry else None
            return FileSource(
                spec.id,
                spec.path,  # type: ignore[arg-type]
                poll_interval=self.config.file_poll_interval,
                start_from=self.config.file_start_from,
                position=position,
                inode=inode,
            )
        return FakeSource(spec.id, incoming_maxsize=self.config.source_incoming_maxsize)

    async def inject(self, source_id: str, payload: str, timestamp: Optional[float] = None) -> int:
        source = self._sources.get(source_id)
        if not isinstance(source, FakeSource):
            raise UnknownSource(f"source {source_id!r} is not a fake source or does not exist")
        offset = await source.inject(payload, timestamp)
        self._mark_dirty()
        return offset

    # -- 订阅管理 -------------------------------------------------------------

    def create_subscription(
        self,
        flt: Optional[Filter] = None,
        sub_id: Optional[str] = None,
        offsets: Optional[Dict[str, int]] = None,
        queue_maxsize: Optional[int] = None,
    ):
        if sub_id is None:
            sub_id = self._new_id("sub", self._sub_specs)
        if sub_id in self._sub_specs:
            raise SourceExists(f"subscription {sub_id!r} already exists")
        flt = flt if flt is not None else Filter(case_sensitive=self.config.keyword_case_sensitive)
        self._sub_specs[sub_id] = SubscriptionSpec(id=sub_id, filter=flt)
        return self.broker.create_subscription(
            sub_id, flt, offsets=offsets, queue_maxsize=queue_maxsize
        )

    def remove_subscription(self, sub_id: str) -> None:
        self.broker.remove_subscription(sub_id)
        self._sub_specs.pop(sub_id, None)
        self._mark_dirty()

    def seek(self, sub_id: str, offsets: Dict[str, int]) -> None:
        self.broker.seek(sub_id, offsets)

    def resume(self, sub_id: str) -> None:
        self.broker.resume(sub_id)

    def ack(self, sub_id: str, offsets: Dict[str, int]) -> None:
        self.broker.ack(sub_id, offsets)

    async def fetch(
        self,
        sub_id: str,
        limit: Optional[int] = None,
        wait: Optional[float] = None,
    ) -> List[Any]:
        return await self.broker.fetch(sub_id, limit=limit, wait=wait)

    # -- 可观测查询 -----------------------------------------------------------

    def snapshot(self) -> Dict[str, Any]:
        snap = self.broker.snapshot()
        for sid, src in list(snap["sources"].items()):
            source = self._sources.get(sid)
            if source is not None:
                src.update(source.snapshot())
            else:
                src["status"] = "removed"
        return snap

    def source_snapshot(self, source_id: str) -> Dict[str, Any]:
        snap = self.snapshot()
        if source_id not in snap["sources"]:
            raise UnknownSource(f"source {source_id!r} does not exist")
        return snap["sources"][source_id]

    def subscription_snapshot(self, sub_id: str) -> Dict[str, Any]:
        snap = self.snapshot()
        if sub_id not in snap["subscriptions"]:
            from .models import UnknownSubscription

            raise UnknownSubscription(sub_id)
        return snap["subscriptions"][sub_id]

    # -- 持久化 ---------------------------------------------------------------

    def _build_state(self) -> Dict[str, Any]:
        sources_state: Dict[str, Any] = {}
        for sid, spec in self._specs.items():
            log = self.broker.get_log(sid)
            entry: Dict[str, Any] = {
                "spec": spec.to_dict(),
                "last_offset": log.latest_offset if log is not None else -1,
            }
            source = self._sources.get(sid)
            if isinstance(source, FileSource):
                entry["file_position"] = source.position
                entry["file_inode"] = source.inode
            sources_state[sid] = entry
        subs_state: Dict[str, Any] = {}
        for sid, spec in self._sub_specs.items():
            sub = self.broker.get_subscription(sid)
            subs_state[sid] = {
                "spec": spec.to_dict(),
                "acked": dict(sub.acked) if sub is not None else {},
            }
        return {"version": 1, "sources": sources_state, "subscriptions": subs_state}

    @staticmethod
    def _new_id(prefix: str, existing: Dict[str, Any]) -> str:
        i = 1
        while True:
            candidate = f"{prefix}-{i}"
            if candidate not in existing:
                return candidate
            i += 1
