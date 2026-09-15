# CSSD 灭菌柜卸载窗口裁决服务

面向消毒供应中心（CSSD）设备集成的纯后端服务。灭菌柜结束程序后，设备必须在
限定窗口内完成开门确认：**确认时限按设备型号逐台配置（1–600 秒整数，未配置
设备默认 30 秒）**；确认请求与超时扫描几乎同时到达时，系统对同一窗口只形成
一个稳定的终态：`confirmed`（正常卸载）或 `quarantined`（隔离）。

## 架构

两个应用进程共享同一个 SQLite 数据库文件，**数据库是两进程的唯一裁决依据**：

```
                ┌─────────────┐
  设备/集成方 ──▶│  api (Gin)  │──┐  登记窗口 / 开门确认 / 查询
                └─────────────┘  │        ┌──────────────────┐
                                 ├───────▶│  SQLite (WAL)    │
                ┌─────────────┐  │        │  devices         │
                │   worker    │──┘        │  unload_windows  │
                └─────────────┘           └──────────────────┘
                 周期扫描到期窗口
```

- `cmd/api`：HTTP API，登记卸载窗口、接收开门确认、查询状态。
- `cmd/worker`：独立扫描进程，把到期未确认的 `open` 窗口写入 `quarantined`。
- `cmd/verify`：一次性验收客户端（见下文「验收」）。
- `internal/store`：SQLite 持久化层（`database/sql` + `modernc.org/sqlite`，纯 Go，无 CGO）。
- `internal/api`：Gin HTTP 层，只做参数校验、错误映射与序列化，裁决全部下沉到 SQL。

### 状态机

```
                 数据库 UTC now <  deadline        ┌─────────────┐
              ┌──────────────────────────────────▶ │  confirmed  │ 终态
   ┌───────┐  │  （开门确认到达）                    └─────────────┘
   │ open  │──┤
   └───────┘  │  数据库 UTC now >= deadline        ┌─────────────┐
              ├──────────────────────────────────▶ │ quarantined │ 终态
                 （确认迟到 confirm_after_deadline   └─────────────┘
                   或扫描超时 scan_timeout）
```

`open` 是唯一非终态；终态之后不存在任何迁移。

## UTC 时间边界规则

所有参与裁决的时刻**一律取自数据库自身的 UTC 当前时间**，客户端与应用进程
的时钟不参与裁决：

- 时间由 SQLite 的 `strftime('%Y-%m-%dT%H:%M:%fZ','now')` 生成，UTC、毫秒
  精度，形如 `2026-09-15T00:19:26.197Z`；定长格式下字符串字典序即时间序。
- **登记**：在**同一条 `INSERT...SELECT`（单事务）**内读取设备当前策略
  `devices.unload_ttl_seconds`，并令 `deadline = 数据库 UTC now + 策略秒数`，
  与 `opened_at` 一起生成，同时把采用的秒数**快照**到窗口行 `ttl_seconds`。
  客户端不得指定截止时刻——登记/确认接口拒绝任何请求字段
  （返回 `400 INVALID_REQUEST`）。此后策略如何修改都不影响已登记窗口：裁决
  只使用窗口行上的快照值，不与 devices 表做运行期联接。
- **确认**：单条 UPDATE 内取数据库时间 `now`：
  - `now < deadline`（严格早于）→ 写入 `confirmed`，原因 `door_open_confirmed`；
  - `now >= deadline`（等于或晚于）→ 写入 `quarantined`，原因 `confirm_after_deadline`。
- **扫描**：worker 把满足 `now >= deadline` 的 `open` 窗口写入 `quarantined`，
  原因 `scan_timeout`——与确认使用**完全相同的边界**，等号恒归属隔离侧。
- SQLite 保证同一语句内多次调用 `'now'` 返回完全相同的值，因此同一条
  UPDATE 里的边界判定与 `closed_at` 使用的是同一个数据库时刻。

## 并发裁决保证

- 确认与扫描的终态写入都是**单条、带 `WHERE state='open'` 前置条件的原子
  UPDATE**。SQLite 将两条写语句串行化，只有先执行的一条能命中目标行；
  后到者命中 0 行，确认侧据此返回 `409 NO_OPEN_WINDOW`。因此无论竞争顺序
  如何，同一窗口只会提交一个终态。
- 同一设备至多一个 `open` 窗口，由数据库**部分唯一索引**
  `ux_unload_windows_open_device` 强制保证；并发登记时第二个 INSERT 必然
  因唯一约束失败，映射为 `409 WINDOW_ALREADY_OPEN`。
- 两个进程均以 `busy_timeout=5000` + WAL 打开同一数据库文件：写冲突在驱动
  层自动重试，读写可并发。

## HTTP API

基础地址：`http://localhost:8080`（可用 `API_PORT` 覆盖宿主端口）。

| 方法 | 路径 | 说明 | 成功 | 主要错误 |
|------|------|------|------|----------|
| GET | `/health` | 健康检查（含数据库连通性） | 200 | 503 |
| POST | `/devices` | 登记设备 `{device_id, name}` | 201 | 409 `DEVICE_ALREADY_EXISTS` |
| PUT | `/devices/{id}/policy` | 更新设备开门确认时限 `{"unload_ttl_seconds": 1..600 的整数}` | 200 | 404 `DEVICE_NOT_FOUND`、400 `INVALID_REQUEST` |
| POST | `/devices/{id}/windows` | 登记卸载窗口（空请求体或 `{}`）；同事务读取当前策略生成截止时刻并快照秒数 | 201 | 404 `DEVICE_NOT_FOUND`、409 `WINDOW_ALREADY_OPEN`、400 `INVALID_REQUEST` |
| POST | `/devices/{id}/confirm` | 开门确认（空请求体或 `{}`） | 200 | 404 `DEVICE_NOT_FOUND`、409 `NO_OPEN_WINDOW` |
| GET | `/devices/{id}/status` | 查询设备（含当前策略 `unload_ttl_seconds`）及最近窗口（含快照 `ttl_seconds`；无窗口时 `window` 为 `null`） | 200 | 404 `DEVICE_NOT_FOUND` |

设备对象：

```json
{
  "device_id": "STER-DEMO",
  "name": "1# 灭菌柜",
  "created_at": "2026-09-15T00:19:00.000Z",
  "unload_ttl_seconds": 30
}
```

窗口对象（`ttl_seconds` 是登记瞬间采用的策略快照，解释截止时刻来源；之后
改策不改变它）：

```json
{
  "id": 3,
  "device_id": "STER-DEMO",
  "state": "open",
  "opened_at": "2026-09-15T00:19:26.197Z",
  "deadline":  "2026-09-15T00:19:56.197Z",
  "ttl_seconds": 30,
  "closed_at": null,
  "close_reason": null
}
```

错误一律为结构化响应：

```json
{
  "error": {
    "code": "WINDOW_ALREADY_OPEN",
    "message": "device \"STER-DEMO\" already has an open unload window",
    "details": { "deadline": "2026-09-15T00:19:56.197Z", "open_window_id": 3 }
  }
}
```

### 示例

```bash
curl -X POST localhost:8080/devices -H 'Content-Type: application/json' \
     -d '{"device_id":"STER-01","name":"灭菌柜 1"}'

# 按设备型号配置开门确认时限（1–600 秒整数；未配置的设备默认 30 秒）
curl -X PUT localhost:8080/devices/STER-01/policy -H 'Content-Type: application/json' \
     -d '{"unload_ttl_seconds":45}'

curl -X POST localhost:8080/devices/STER-01/windows          # 201，deadline 按当前策略由数据库生成
curl -X POST localhost:8080/devices/STER-01/confirm          # 200，confirmed 或 quarantined
curl     localhost:8080/devices/STER-01/status               # 查询当前策略、窗口快照与最终状态
```

## 配置

| 变量 | 组件 | 默认值 | 说明 |
|------|------|--------|------|
| `DATABASE_PATH` | api / worker | `cssd.db`（容器内 `/data/cssd.db`） | SQLite 文件路径，两进程必须一致 |
| `PORT` | api | `8080` | 容器内监听端口 |
| `API_PORT` | compose | `8080` | 映射到宿主的端口 |
| `SCAN_INTERVAL` | worker | `1s` | 扫描周期（Go duration，如 `500ms`） |
| `API_BASE_URL` | verify | `http://localhost:8080` | 验收目标地址 |

## 本地运行与测试

需要 Go 1.25。

```bash
go test ./...            # 单元/并发裁决测试（可加 -race）
go run ./cmd/api &       # 启动 API（:8080）
go run ./cmd/worker &    # 启动扫描 worker
```

测试覆盖：及时确认、无人确认超时隔离、数据库时间恰好位于边界时确认与扫描
的并发裁决（恰好一方提交终态且恒为 quarantined）、活动窗口冲突（含并发
登记恰好一个成功），并通过查询断言唯一最终状态；此外覆盖默认设备 30 秒
窗口、策略边界（1/600 接受、0/601/负数拒绝）、改策只影响新窗口而活动
窗口快照不变、策略更新不触碰既有窗口、自定义时限下的确认/扫描竞争回归，
以及旧版数据库的追加列迁移（默认回填 30 秒、迁移幂等）。

## Docker Compose 运行

```bash
docker compose up --build          # 仅启动 api 与 worker 两个应用组件
API_PORT=9090 docker compose up    # 覆盖宿主端口
```

### 验收（一次性 verify 服务）

`verify` 位于独立 profile，不影响默认启动。它会等待 API 健康后执行完整
验收（及时确认、30 秒超时隔离、活动窗口冲突、客户端禁止指定截止时刻、
终态后重复确认、默认策略 30 秒、非法策略结构化错误、改策后新窗口采用新
时限、活动窗口快照不受再次改策影响并由 worker 按新时限隔离、确认与
`NO_OPEN_WINDOW` 回归），全部通过输出 `VERIFY: PASS` 并以退出码 0 结束：

```bash
docker compose --profile verify up --build --abort-on-container-exit --exit-code-from verify
docker compose down                # 清理（保留数据卷；加 -v 一并删除）
```

## 目录结构

```
cmd/api/main.go        HTTP API 进程入口
cmd/worker/main.go     到期扫描进程入口
cmd/verify/main.go     一次性验收客户端
internal/api/          Gin 路由、参数校验、结构化错误
internal/store/        SQLite 持久化与全部裁决 SQL（含并发测试）
Dockerfile             单一镜像构建三个二进制
docker-compose.yml     api + worker（默认）与 verify（profile）
```
