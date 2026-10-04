# log-event-stream

多路日志采集与实时订阅的流式服务。日志来自多个可配置的本地来源（本地文件 tail、
进程内注入的假来源），下游订阅者各自按自己的进度消费，互不阻塞。

## 快速开始

```bash
uv sync
uv run python main.py            # 启动服务（默认 127.0.0.1:8000）
uv run pytest                    # 运行测试
```

## 架构

```
来源(文件tail/注入) → SourceManager → Journal(追加式日志, 分配位点, 落盘)
                                     → FanoutHub → Subscriber(有界队列+过滤) → WebSocket 推送
订阅者 ack → checkpoints.json(持久化)
```

- `logstream/models.py`：Event / Position / SubscriptionFilter
- `logstream/journal.py`：每来源一个追加式 JSONL 日志，负责分配位点并持久化
- `logstream/sources.py`：文件 tail 来源、进程内注入来源、动态增删
- `logstream/hub.py`：分发中心与订阅者有界队列
- `logstream/service.py`：编排、断点续传、checkpoint 持久化、状态查询
- `logstream/api.py`：REST + WebSocket API

## 顺序语义

- 每个事件在**写入 journal 时**分配来源内单调递增的 `seq`（从 1 开始）。
- 同一来源的事件顺序与产生顺序一致（journal 追加有 per-source 锁）。
- 不同来源之间**没有**全局顺序；来源各自独立采集，互不阻塞。
- 过滤只影响事件对某个订阅者的可见性，**不改变位点语义**：被过滤掉的
  事件仍然占用 seq，订阅者看到的 seq 可能不连续，这是正常的。

## 位点模型

- 位点 `Position = (source_id, seq)`，订阅者的消费进度是
  `{source_id: 已确认的最大 seq}`。
- 订阅时可传 `resume={source_id: seq}` 断点续传；不传则使用服务端持久化
  的 checkpoint；两者都没有则从头消费。
- 已 ack 的事件不会重发；未 ack 的事件在重连后会被补发。
- 位点异常处理：
  - `seq < 0`：回退为 0（从头消费）；
  - `seq > 来源当前最大位点`：拒绝，WebSocket 返回 `resume_out_of_range` 错误；
  - 未知来源：忽略（可能已被移除）。
- 来源重启/移除/重加**不会重置位点**：journal 持久化在磁盘，重加来源后
  seq 延续。

## 慢订阅者与背压

- 每个订阅者一个有界内存队列（默认 1024 条）。队列满时**丢弃最旧事件**，
  向该订阅者推送 `gap` 消息（`{"type":"gap","source_id","up_to_seq"}`），
  并累计 `dropped` 计数（可在 `/status` 查询）。
- 慢订阅者只影响自己：publish 是同步内存操作，绝不阻塞其他订阅者。
- 采集端背压：事件先落盘（journal append + fsync）再分发，内存中不会
  无界堆积；订阅者队列是唯一的有界缓冲。
- 重启后状态区分：journal（事件与位点）和 checkpoints（订阅者 ack）持久化；
  订阅者内存队列中未完成的分发是易失状态，重启后按 checkpoint 重新补发。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/status` | 每个来源的 `last_seq`/active，每个订阅者的 acked/delivered/queued/dropped/lag |
| POST | `/sources` | 动态加来源：`{"source_id","type":"injected"|"file","path"?}` |
| DELETE | `/sources/{id}` | 移除来源 |
| POST | `/sources/{id}/inject` | 向注入来源投递事件：`{"message","timestamp"?}` |
| WS | `/subscribe/{id}` | 订阅推送，query 参数见下 |

WebSocket 订阅参数：`sources=a,b`、`keyword`、`start_ts`、`end_ts`、
`resume={"a":3}`（JSON）。服务端推送 `event` / `gap` / `error` 消息；
客户端发送 `{"ack":{"source_id":"a","seq":3}}` 确认位点。

## 可调参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `LOGSTREAM_DATA_DIR` | `./logstream-data` | journal 与 checkpoint 的存储目录 |
| `LOGSTREAM_QUEUE_SIZE` | `1024` | 每个订阅者的有界队列大小 |
| `poll_interval`（文件来源） | `0.05s` | 文件 tail 的轮询间隔 |

## 测试

```bash
uv run pytest -q
```

覆盖：乱序/交错采集下的来源内顺序、过滤不改变位点、断线续传（已确认不重发、
未确认不跳过）、位点回退与越界、慢订阅者丢弃与 gap 提示、来源动态增删与
重启恢复、服务重启后 journal/checkpoint 恢复、状态查询自洽、WebSocket 端到端。
