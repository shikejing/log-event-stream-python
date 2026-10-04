import os
from pathlib import Path

import uvicorn

from logstream.api import create_app
from logstream.service import LogStreamService

DATA_DIR = Path(os.environ.get("LOGSTREAM_DATA_DIR", "./logstream-data"))
QUEUE_SIZE = int(os.environ.get("LOGSTREAM_QUEUE_SIZE", "1024"))

service = LogStreamService(DATA_DIR, queue_size=QUEUE_SIZE)
app = create_app(service)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
