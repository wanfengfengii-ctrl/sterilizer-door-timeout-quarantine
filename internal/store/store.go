// Package store 提供灭菌柜卸载窗口的 SQLite 持久化层。
//
// API 进程与扫描 worker 进程共享同一个 SQLite 数据库文件，数据库是两个
// 进程对窗口状态（open / confirmed / quarantined）进行裁决的唯一依据：
// 所有终态写入都是带 state='open' 前置条件的单条原子 UPDATE，并发竞争时
// 只有一条语句能命中目标行，因此一台设备的同一窗口只会提交一个终态。
//
// 时间规则：所有参与裁决的时刻一律取自数据库自身的 UTC 当前时间
// （SQLite 的 strftime('%Y-%m-%dT%H:%M:%fZ','now')，毫秒精度），
// 客户端与应用进程的时钟不参与裁决。SQLite 保证同一语句内多次调用
// 'now' 返回完全相同的值，因此同一条 UPDATE 里的判定与 closed_at
// 使用的是同一个数据库时刻。
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"modernc.org/sqlite"
)

// 窗口状态机：open 是唯一非终态；confirmed 与 quarantined 是终态，
// 终态之后不存在任何迁移。
const (
	StateOpen        = "open"
	StateConfirmed   = "confirmed"
	StateQuarantined = "quarantined"
)

// 终态关闭原因。
const (
	ReasonDoorOpenConfirmed    = "door_open_confirmed"    // 截止时刻之前收到开门确认
	ReasonConfirmAfterDeadline = "confirm_after_deadline" // 确认到达时数据库时间已不早于截止时刻
	ReasonScanTimeout          = "scan_timeout"           // worker 扫描发现窗口已到期
)

// 卸载窗口确认时限策略：未单独配置的设备一律使用默认 30 秒；集成方可通过
// UpdateDevicePolicy 提交 [MinWindowTTLSeconds, MaxWindowTTLSeconds] 区间内
// 的整数秒数。登记窗口时在同一事务内读取设备当前策略并把秒数快照到窗口行，
// 之后策略如何变化都不影响已登记窗口的截止时刻。
const (
	// DefaultWindowTTLSeconds 是未配置设备的默认确认时限（秒）。
	DefaultWindowTTLSeconds = 30
	// MinWindowTTLSeconds / MaxWindowTTLSeconds 是策略允许的整数秒边界。
	MinWindowTTLSeconds = 1
	MaxWindowTTLSeconds = 600
)

// WindowTTLSeconds 保留为默认时限的别名（历史名称）。
const WindowTTLSeconds = DefaultWindowTTLSeconds

var (
	ErrDeviceNotFound    = errors.New("device not found")
	ErrDeviceExists      = errors.New("device already exists")
	ErrWindowAlreadyOpen = errors.New("device already has an open unload window")
	ErrNoOpenWindow      = errors.New("device has no open unload window")
	ErrWindowNotFound    = errors.New("unload window not found")
	// ErrInvalidPolicy 表示策略秒数不是 [1,600] 区间内的整数。
	ErrInvalidPolicy = errors.New("unload ttl seconds must be an integer between 1 and 600")
)

// Device 是一台灭菌柜设备。UnloadTTLSeconds 是该设备当前的开门确认时限
// 策略（秒）；未配置时为 DefaultWindowTTLSeconds。
type Device struct {
	ID               string `json:"device_id"`
	Name             string `json:"name"`
	CreatedAt        string `json:"created_at"`
	UnloadTTLSeconds int    `json:"unload_ttl_seconds"`
}

// Window 是一次卸载窗口。OpenedAt / Deadline / ClosedAt 均为数据库生成的
// UTC 时间串，格式为 YYYY-MM-DDTHH:MM:SS.SSSZ（毫秒精度，字典序即时间序）。
// TTLSeconds 是登记瞬间从设备策略快照下来的确认时限（秒），窗口此后与设备
// 当前策略脱钩：Deadline 始终等于 OpenedAt + TTLSeconds。
type Window struct {
	ID          int64   `json:"id"`
	DeviceID    string  `json:"device_id"`
	State       string  `json:"state"`
	OpenedAt    string  `json:"opened_at"`
	Deadline    string  `json:"deadline"`
	TTLSeconds  int     `json:"ttl_seconds"`
	ClosedAt    *string `json:"closed_at"`
	CloseReason *string `json:"close_reason"`
}

const schema = `
CREATE TABLE IF NOT EXISTS devices (
    id                 TEXT PRIMARY KEY,
    name               TEXT NOT NULL,
    created_at         TEXT NOT NULL,
    unload_ttl_seconds INTEGER NOT NULL DEFAULT 30
                               CHECK (unload_ttl_seconds BETWEEN 1 AND 600)
);

CREATE TABLE IF NOT EXISTS unload_windows (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id    TEXT NOT NULL REFERENCES devices(id),
    state        TEXT NOT NULL CHECK (state IN ('open', 'confirmed', 'quarantined')),
    opened_at    TEXT NOT NULL,
    deadline     TEXT NOT NULL,
    ttl_seconds  INTEGER NOT NULL CHECK (ttl_seconds BETWEEN 1 AND 600),
    closed_at    TEXT,
    close_reason TEXT
);

-- 同一设备至多一个 open 窗口，由数据库部分唯一索引强制保证；
-- 并发登记时第二个 INSERT 必然因唯一约束失败。
CREATE UNIQUE INDEX IF NOT EXISTS ux_unload_windows_open_device
    ON unload_windows (device_id) WHERE state = 'open';

-- worker 按 (state='open' AND now >= deadline) 扫描，加速到期窗口定位。
CREATE INDEX IF NOT EXISTS ix_unload_windows_open_deadline
    ON unload_windows (deadline) WHERE state = 'open';
`

// 旧版本数据库的追加式迁移：新列以默认 30 秒补齐，既有窗口的截止时刻本就按
// 30 秒生成，回填值与其语义一致。迁移逐列幂等执行。
const (
	migrateDevicesPolicyCol = `ALTER TABLE devices
	    ADD COLUMN unload_ttl_seconds INTEGER NOT NULL DEFAULT 30`
	migrateWindowsTTLCol = `ALTER TABLE unload_windows
	    ADD COLUMN ttl_seconds INTEGER NOT NULL DEFAULT 30`
)

const deviceColumns = "id, name, created_at, unload_ttl_seconds"

const windowColumns = "id, device_id, state, opened_at, deadline, ttl_seconds, closed_at, close_reason"

// registerWindowSQL 在单条 INSERT...SELECT（即 SQLite 的一个隐式事务）内完成
// 三件事：读取设备当前 unload_ttl_seconds 策略、用同一数据库 UTC 时刻生成
// opened_at 与 deadline（deadline = now + 策略秒数）、把采用的秒数快照到
// ttl_seconds 列。策略在窗口存续期间再被修改也触碰不到已登记窗口：窗口行
// 不与 devices 表做任何运行期联接。设备不存在时 SELECT 端无行，INSERT 命中
// 0 行，调用方映射为 ErrDeviceNotFound。
const registerWindowSQL = `
INSERT INTO unload_windows (device_id, state, opened_at, deadline, ttl_seconds)
SELECT d.id, 'open',
       strftime('%Y-%m-%dT%H:%M:%fZ','now'),
       strftime('%Y-%m-%dT%H:%M:%fZ','now', '+' || d.unload_ttl_seconds || ' seconds'),
       d.unload_ttl_seconds
FROM devices AS d
WHERE d.id = ?
RETURNING ` + windowColumns

// confirmWindowSQL 在单条 UPDATE 内完成裁决：数据库 UTC 当前时间严格早于
// 截止时刻时写入 confirmed，等于或晚于截止时刻时写入 quarantined。
// WHERE state='open' 是原子前置条件：与 worker 的到期扫描竞争时，两条
// UPDATE 被 SQLite 串行化，只有先执行的一条能命中该行。
const confirmWindowSQL = `
UPDATE unload_windows
SET state = CASE
                WHEN strftime('%Y-%m-%dT%H:%M:%fZ','now') < deadline THEN 'confirmed'
                ELSE 'quarantined'
            END,
    closed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
    close_reason = CASE
                WHEN strftime('%Y-%m-%dT%H:%M:%fZ','now') < deadline THEN 'door_open_confirmed'
                ELSE 'confirm_after_deadline'
            END
WHERE device_id = ? AND state = 'open'
RETURNING ` + windowColumns

// quarantineExpiredSQL 与确认使用相同的时间边界：数据库 UTC 当前时间等于
// 或晚于截止时刻的 open 窗口被原子地写入 quarantined。
const quarantineExpiredSQL = `
UPDATE unload_windows
SET state = 'quarantined',
    closed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
    close_reason = 'scan_timeout'
WHERE state = 'open'
  AND strftime('%Y-%m-%dT%H:%M:%fZ','now') >= deadline`

// Store 封装对 SQLite 数据库的访问。
type Store struct {
	db *sql.DB
}

// Open 打开（必要时创建）SQLite 数据库并执行 schema 迁移。
// busy_timeout 让两个进程的写冲突在驱动层自动重试；WAL 允许读写并发。
func Open(ctx context.Context, path string) (*Store, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create database directory: %w", err)
		}
	}
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if err := migrate(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// migrate 幂等地执行建表与旧库追加列迁移：ALTER TABLE 不支持 IF NOT EXISTS，
// 因此先通过 PRAGMA table_info 判断列是否已存在，避免重复执行报错。
func migrate(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("migrate schema: %w", err)
	}
	migrations := []struct {
		table  string
		column string
		stmt   string
	}{
		{"devices", "unload_ttl_seconds", migrateDevicesPolicyCol},
		{"unload_windows", "ttl_seconds", migrateWindowsTTLCol},
	}
	for _, m := range migrations {
		exists, err := columnExists(ctx, db, m.table, m.column)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		if _, err := db.ExecContext(ctx, m.stmt); err != nil {
			return fmt.Errorf("migrate add %s.%s: %w", m.table, m.column, err)
		}
	}
	return nil
}

func columnExists(ctx context.Context, db *sql.DB, table, column string) (bool, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return false, fmt.Errorf("inspect table %s: %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, ctype string
		var notNull, pk int
		var dflt any
		if err := rows.Scan(&cid, &name, &ctype, &notNull, &dflt, &pk); err != nil {
			return false, fmt.Errorf("inspect table %s: %w", table, err)
		}
		if name == column {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("inspect table %s: %w", table, err)
	}
	return false, nil
}

// Close 关闭底层数据库连接池。
func (s *Store) Close() error { return s.db.Close() }

// Ping 检查数据库连通性，供健康检查使用。
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// CreateDevice 登记一台设备；设备已存在时返回 ErrDeviceExists。
// 新设备不携带单独策略，使用 DefaultWindowTTLSeconds（列默认值）。
func (s *Store) CreateDevice(ctx context.Context, id, name string) (Device, error) {
	var d Device
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO devices (id, name, created_at)
		VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
		RETURNING `+deviceColumns, id, name).
		Scan(&d.ID, &d.Name, &d.CreatedAt, &d.UnloadTTLSeconds)
	switch {
	case err == nil:
		return d, nil
	case isUniqueViolation(err):
		return Device{}, ErrDeviceExists
	default:
		return Device{}, fmt.Errorf("create device: %w", err)
	}
}

// GetDevice 按 ID 查询设备；不存在时返回 ErrDeviceNotFound。
func (s *Store) GetDevice(ctx context.Context, id string) (Device, error) {
	var d Device
	err := s.db.QueryRowContext(ctx,
		`SELECT `+deviceColumns+` FROM devices WHERE id = ?`, id).
		Scan(&d.ID, &d.Name, &d.CreatedAt, &d.UnloadTTLSeconds)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Device{}, ErrDeviceNotFound
	case err != nil:
		return Device{}, fmt.Errorf("get device: %w", err)
	default:
		return d, nil
	}
}

// UpdateDevicePolicy 更新设备的开门确认时限策略（整数秒，[1,600] 闭区间）。
// 更新只写 devices 一行（单条原子 UPDATE，天然不存在半写入配置），既不会
// 触碰该设备任何既有窗口，也不依赖它们的状态——活动窗口的 deadline 与
// ttl_seconds 快照保持不变。设备不存在时返回 ErrDeviceNotFound；越界值在
// 进入 SQL 前由 ErrInvalidPolicy 拦截（CHECK 约束是第二道防线）。
func (s *Store) UpdateDevicePolicy(ctx context.Context, deviceID string, ttlSeconds int) (Device, error) {
	if ttlSeconds < MinWindowTTLSeconds || ttlSeconds > MaxWindowTTLSeconds {
		return Device{}, ErrInvalidPolicy
	}
	var d Device
	err := s.db.QueryRowContext(ctx, `
		UPDATE devices
		SET unload_ttl_seconds = ?
		WHERE id = ?
		RETURNING `+deviceColumns, ttlSeconds, deviceID).
		Scan(&d.ID, &d.Name, &d.CreatedAt, &d.UnloadTTLSeconds)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Device{}, ErrDeviceNotFound
	case err != nil:
		return Device{}, fmt.Errorf("update device policy: %w", err)
	default:
		return d, nil
	}
}

// RegisterWindow 为设备登记一个卸载窗口，返回包含数据库生成的 opened_at
// 与 deadline 的窗口。设备不存在时返回 ErrDeviceNotFound（策略 SELECT 端
// 无行，INSERT 命中 0 行）；设备已有 open 窗口时返回 ErrWindowAlreadyOpen
// （由部分唯一索引在并发下同样成立）。
func (s *Store) RegisterWindow(ctx context.Context, deviceID string) (Window, error) {
	w, err := scanWindow(s.db.QueryRowContext(ctx, registerWindowSQL, deviceID))
	switch {
	case err == nil:
		return w, nil
	case errors.Is(err, sql.ErrNoRows):
		// INSERT...SELECT 的源行只可能是设备缺失（FK 不再参与判定）。
		return Window{}, ErrDeviceNotFound
	case isUniqueViolation(err):
		return Window{}, ErrWindowAlreadyOpen
	default:
		return Window{}, fmt.Errorf("register window: %w", err)
	}
}

// ConfirmWindow 处理开门确认：数据库 UTC 当前时间严格早于截止时刻时写入
// confirmed，等于或晚于截止时刻时写入 quarantined，并返回终态窗口。
// 设备不存在时返回 ErrDeviceNotFound；没有可确认的 open 窗口（从未登记或
// 已被确认/隔离）时返回 ErrNoOpenWindow。
func (s *Store) ConfirmWindow(ctx context.Context, deviceID string) (Window, error) {
	w, err := scanWindow(s.db.QueryRowContext(ctx, confirmWindowSQL, deviceID))
	switch {
	case err == nil:
		return w, nil
	case errors.Is(err, sql.ErrNoRows):
		if _, derr := s.GetDevice(ctx, deviceID); derr != nil {
			if errors.Is(derr, ErrDeviceNotFound) {
				return Window{}, ErrDeviceNotFound
			}
			return Window{}, derr
		}
		return Window{}, ErrNoOpenWindow
	default:
		return Window{}, fmt.Errorf("confirm window: %w", err)
	}
}

// QuarantineExpired 由 worker 周期调用：把数据库 UTC 当前时间等于或晚于
// 截止时刻的 open 窗口原子地写入 quarantined，返回隔离的窗口数量。
// 与 ConfirmWindow 竞争同一窗口时，只有一条语句能命中 state='open' 的行。
func (s *Store) QuarantineExpired(ctx context.Context) (int64, error) {
	res, err := s.db.ExecContext(ctx, quarantineExpiredSQL)
	if err != nil {
		return 0, fmt.Errorf("quarantine expired windows: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("quarantine expired windows: %w", err)
	}
	return n, nil
}

// GetWindow 按窗口 ID 查询；不存在时返回 ErrWindowNotFound。
func (s *Store) GetWindow(ctx context.Context, id int64) (Window, error) {
	w, err := scanWindow(s.db.QueryRowContext(ctx,
		`SELECT `+windowColumns+` FROM unload_windows WHERE id = ?`, id))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Window{}, ErrWindowNotFound
	case err != nil:
		return Window{}, fmt.Errorf("get window: %w", err)
	default:
		return w, nil
	}
}

// LatestWindow 返回设备最近一个窗口；从未登记过时返回 ErrWindowNotFound。
func (s *Store) LatestWindow(ctx context.Context, deviceID string) (Window, error) {
	w, err := scanWindow(s.db.QueryRowContext(ctx,
		`SELECT `+windowColumns+` FROM unload_windows WHERE device_id = ? ORDER BY id DESC LIMIT 1`, deviceID))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Window{}, ErrWindowNotFound
	case err != nil:
		return Window{}, fmt.Errorf("latest window: %w", err)
	default:
		return w, nil
	}
}

func scanWindow(row *sql.Row) (Window, error) {
	var w Window
	var closedAt, closeReason sql.NullString
	if err := row.Scan(&w.ID, &w.DeviceID, &w.State, &w.OpenedAt, &w.Deadline, &w.TTLSeconds, &closedAt, &closeReason); err != nil {
		return Window{}, err
	}
	if closedAt.Valid {
		w.ClosedAt = &closedAt.String
	}
	if closeReason.Valid {
		w.CloseReason = &closeReason.String
	}
	return w, nil
}

// isUniqueViolation 判定 SQLite 唯一约束冲突（SQLITE_CONSTRAINT_UNIQUE=2067）。
func isUniqueViolation(err error) bool {
	var serr *sqlite.Error
	if errors.As(err, &serr) {
		return serr.Code() == 2067
	}
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
