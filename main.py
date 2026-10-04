"""log-event-stream 服务入口。

启动：
    uv run python main.py            # 默认 127.0.0.1:8000
    uv run python main.py --port 9000
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from contextlib import asynccontextmanager

import uvicorn

from logstream import Service, StreamConfig
from logstream.api import create_app


def main() -> None:
    parser = argparse.ArgumentParser(description="log-event-stream service")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--state", default="logstream-state.json", help="状态文件路径")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    config = StreamConfig(state_path=args.state)
    service = Service(config)

    @asynccontextmanager
    async def lifespan(app):
        await service.start()
        try:
            yield
        finally:
            await service.stop()

    app = create_app(service)
    app.router.lifespan_context = lifespan

    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
