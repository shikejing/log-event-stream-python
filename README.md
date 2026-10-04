# log-event-stream

多路日志采集与实时订阅的流式服务。日志来自多个可配置的本地来源，下游订阅者
各自按自己的进度消费；来源可能中途重启或短暂中断，服务保证不丢事件、顺序可解释，
并把事件实时推送给多个订阅者，慢订阅者不会拖垮整体。

数据来源用「本地文件」与「进程内注入的假来源」模拟，不依赖任何外部服务。

## 核心概念

### 事件与位点模型

- 每个事件由 `(source_id, offset)` 唯一标识。
- `offset` 是**来源内单调递增**的整数，从 `0` 开始，由 broker 在采集（`append`）时
  分配。同一来源内严格递增、不重置、不回退；不同来源的位点互相独立、不可比。
- 事件顺序按 `offset` 排列，与时间戳无关（时间戳只用于过滤与保留）。即使注入时
  时间戳乱序，同一来源仍按 offset 顺序投递。
- 来源重启、重新加入、文件轮转都**不会重置历史位点**：新事件的 offset 接续最后
  一个已分配位点。

### 订阅进度：acked 与 delivered

每个订阅者对每个来源维护两个位点：

- `acked`（已确认位点）：订阅者已处理并确认的最后位点（含），`-1` 表示尚未确认。
  这是「已提交」进度，持久化到状态文件。
- `delivered`（已投递位点）：broker 已推送到订阅者队列（或被过滤跳过）的最后位点。
  这是「在途」进度，**不持久化**，重启/重连时重建。

不变量：`acked <= delivered <= latest`。

### 过滤不改变位点语义

订阅者可按 `sources`（来源集合）、`keyword`（关键字子串）、`start_time/end_time`
（时间范围，左闭右开）过滤。过滤只决定事件对该订阅者是否可见：

- 被过滤掉的事件不进投递队列、无需 ack，但该来源的 `delivered` 位点照常推进。
- 同一事件在不同订阅者处按各自过滤条件独立决定可见性，位点本身不变。

### 投递语义：at-least-once + 缺口告知

- 事件推送到订阅者的有界队列，订阅者拉取（REST 长轮询 / WebSocket）后处理，处理完
  发送 `ack`。
- **已确认事件不重复推送**；**未确认事件不跳过**。断线重连（或服务重启）时，
  `delivered` 重置到 `acked`，从 `acked+1` 重新追赶投递。
- 若断线期间来源日志按保留策略淘汰了旧事件，重新追赶时会投递**缺口标记**
  （`Gap`：`source_id` + `from_offset` + `to_offset`），明确告知哪段位点区间已丢失，
  订阅者 ack 越过缺口即表示已知晓。

### 慢订阅者降级：丢弃 + 缺口，不阻塞整体

- 每个订阅者的投递队列长度有界（`sub_queue_maxsize`，可按订阅者覆盖）。
- 队列满后，新事件入队时丢弃**最旧的未确认事件**，被丢事件的位点区间合并成缺口
  标记（`dropped_total` 计数，可观测）。
- 快订阅者队列独立、不受影响；采集端也不会因为个别订阅者慢而阻塞（入队是 O(1)
  纯内存操作）。
- 内存有界：来源日志按「条数 + 年龄」保留，订阅队列按条数保留，任何情况下都不
  无界堆积。

### 背压

- 假来源（FakeSource）的注入队列有界（`source_incoming_maxsize`）：队列满时
  `inject` 协程等待，对采集端形成背压。
- 来源产生快于消费时，内存占用由「来源保留日志 + 各订阅者队列」两级上界封死，
  不会无限增长。

### 持久化：已提交位点 vs 在途分发状态

状态文件（`state.json`，原子写）只持久化**已提交位点**：

- `sources`：来源规格、最后分配位点 `last_offset`、文件来源的读取位置 `file_position`
  与 inode。
- `subscriptions`：订阅规格（过滤条件）、各来源已确认位点 `acked`。

**不持久化**（重启后重建/重新投递）：订阅者投递队列、`delivered` 在途进度、缺口
列表、丢弃计数、假来源采集队列中尚未分配位点的注入项。

重启恢复：

- 来源从 `last_offset+1` 继续分配位点；文件来源从持久化的文件位置继续读（inode
  变了则按轮转从头读，位点继续）。
- 订阅者从 `acked+1` 重新追赶：已确认不重复，未确认重新投递，已淘汰区间通过缺口
  告知。

事件本身持久化在 `events/<source_id>.events`（append-only JSON Lines），重启时
重放进内存日志；服务停止时按内存保留区间重写压缩。

## 来源类型

| 类型 | 说明 |
| --- | --- |
| `file` | 本地文件来源。轮询文件新增行，按行采集；支持截断/轮转（从头读，位点继续）。文件不存在时状态为 `waiting`，出现后自动开始采集。 |
| `fake` | 进程内假来源。通过 `POST /api/sources/{id}/inject` 或 `FakeSource.inject()` 注入事件，返回分配到的位点。多任务并发注入时由采集队列串行化，保证顺序。 |

## 可调参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `state_path` | `logstream-state.json` | 状态持久化文件路径。 |
| `file_poll_interval` | `0.2` 秒 | 文件来源轮询间隔。 |
| `file_start_from` | `beginning` | 文件来源首次加入时从哪读：`beginning`（从头）或 `end`（只追新行）。 |
| `log_retention_events` | `10000` | 每个来源保留事件条数，超出淘汰最旧。 |
| `log_retention_seconds` | `3600` | 每个来源保留事件最长年龄（秒），超出淘汰。 |
| `sub_queue_maxsize` | `1000` | 每个订阅者投递队列长度上限（可被订阅级 `queue_maxsize` 覆盖）。 |
| `source_incoming_maxsize` | `10000` | 假来源注入队列长度上限，满后 inject 等待（背压）。 |
| `keyword_case_sensitive` | `True` | 关键字过滤是否大小写敏感。 |
| `fetch_default_wait` | `0.5` 秒 | 拉取事件接口默认长轮询等待。 |
| `fetch_limit` | `500` | 单次拉取/推送的最大条数。 |
| `persist_debounce` | `0.5` 秒 | 状态落盘防抖间隔。 |

## HTTP API

### 来源

- `POST /api/sources`：新增来源。body：`{id?, type: "file"|"fake", path?}`。
- `DELETE /api/sources/{source_id}`：移除来源（日志保留，位点不重置；可重新加入）。
- `GET /api/sources`：列出来源及采集进度。
- `GET /api/sources/{source_id}`：单个来源进度。
- `POST /api/sources/{source_id}/inject`：向假来源注入事件。body：`{payload, timestamp?}`。

### 订阅

- `POST /api/subscriptions`：创建订阅。body：
  `{id?, filter?: {sources?, keyword?, start_time?, end_time?}, offsets?: {source: next_offset}, queue_maxsize?}`。
  `offsets` 为各来源「下一个要投递的位点」；缺省从最新（只看未来）开始。
- `GET /api/subscriptions`：列出订阅及位点/滞后。
- `GET /api/subscriptions/{sub_id}`：单个订阅状态。
- `DELETE /api/subscriptions/{sub_id}`：删除订阅。
- `POST /api/subscriptions/{sub_id}/seek`：断点续传/回退。body：`{offsets: {source: next_offset}}`。
  - 回退到已保留区间内：允许，重新投递。
  - 指向未来（`> latest+1`）：`400 OffsetOutOfRange`。
  - 指向已淘汰位点（`< oldest`）：`400 OffsetUnavailable`。
- `POST /api/subscriptions/{sub_id}/resume`：断线重连，从 `acked+1` 继续。
- `POST /api/subscriptions/{sub_id}/ack`：确认投递。body：`{offsets: {source: last_offset}}`（含）。
- `GET /api/subscriptions/{sub_id}/events?limit=&wait=`：长轮询拉取事件（缺口先于事件返回）。

### WebSocket

- `WS /ws/subscriptions/{sub_id}`：订阅流实时推送。
  - 连接后先收到 `{"type": "hello", "subscription": ...}`，随后服务端推送
    `{"type": "items", "items": [...]}`（事件 `{"type":"event",...}` 或缺口
    `{"type":"gap",...}`）。
  - 客户端发送 `{"type": "ack", "offsets": {...}}` 或 `{"type": "seek", "offsets": {...}}`，
    服务端回 `{"type": "ok"}`。
  - 重连（新连接）自动从 `acked+1` 继续。

## 可观测查询

`GET /api/sources` 与 `GET /api/subscriptions` 返回运行快照，查询与推送并发进行时
结果自洽（快照为同步计算，不 await）：

- 来源：`latest_offset`（最新位点）、`oldest_offset`（最旧保留位点）、`next_offset`、
  `retained`（保留条数）、`status`（running/waiting/removed）、文件位置等。
- 订阅：`acked`、`delivered`、`pending`（队列长度）、`pending_gaps`（缺口数）、
  `dropped_total`（累计丢弃事件数）、`lag`（各来源 `latest - acked` 滞后位点距离）。

## 运行

```bash
uv sync
uv run pytest -q
uv run python main.py   # 默认 127.0.0.1:8000，可改端口
```

## 测试覆盖

- 乱序采集：多任务并发注入，位点严格递增、按 offset 顺序投递；时间戳乱序不影响顺序。
- 断线续传：ack 后重连不重复；未确认事件重新投递；缺口告知淘汰区间。
- 慢订阅者：有界队列丢弃最旧事件 + 缺口，快订阅者不受影响，内存有界。
- 来源动态增删与重启恢复：移除后重新加入位点继续；重启后来源位点与订阅进度恢复。
- 文件来源：tail 追加、截断/轮转位点继续、不完整行暂存。
- API：REST 各端点与 WebSocket 实时推送/重连。
