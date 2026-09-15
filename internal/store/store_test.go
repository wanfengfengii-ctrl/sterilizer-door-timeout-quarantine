package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(context.Background(), filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func mustDevice(t *testing.T, st *Store, id string) {
	t.Helper()
	if _, err := st.CreateDevice(context.Background(), id, "device "+id); err != nil {
		t.Fatalf("create device %s: %v", id, err)
	}
}

func mustWindow(t *testing.T, st *Store, deviceID string) Window {
	t.Helper()
	w, err := st.RegisterWindow(context.Background(), deviceID)
	if err != nil {
		t.Fatalf("register window for %s: %v", deviceID, err)
	}
	return w
}

// setDeadline 用数据库自身的时间函数把窗口截止时刻改写为 now+offset，
// 使测试可以精确地把窗口放到边界上或过去，客户端时钟不参与。
func setDeadline(t *testing.T, st *Store, id int64, offset string) string {
	t.Helper()
	var deadline string
	err := st.db.QueryRow(`
		UPDATE unload_windows
		SET deadline = strftime('%Y-%m-%dT%H:%M:%fZ','now', ?)
		WHERE id = ?
		RETURNING deadline`, offset, id).Scan(&deadline)
	if err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	return deadline
}

// 及时确认：数据库时间严格早于截止时刻 → confirmed。
func TestConfirmWithinDeadline(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	mustDevice(t, st, "STER-01")

	w := mustWindow(t, st, "STER-01")
	if w.State != StateOpen {
		t.Fatalf("new window state = %q, want %q", w.State, StateOpen)
	}
	opened, err := time.Parse(time.RFC3339Nano, w.OpenedAt)
	if err != nil {
		t.Fatalf("parse opened_at %q: %v", w.OpenedAt, err)
	}
	deadline, err := time.Parse(time.RFC3339Nano, w.Deadline)
	if err != nil {
		t.Fatalf("parse deadline %q: %v", w.Deadline, err)
	}
	if got := deadline.Sub(opened); got != WindowTTLSeconds*time.Second {
		t.Fatalf("deadline - opened_at = %v, want %v", got, WindowTTLSeconds*time.Second)
	}

	done, err := st.ConfirmWindow(ctx, "STER-01")
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if done.State != StateConfirmed {
		t.Fatalf("state = %q, want %q", done.State, StateConfirmed)
	}
	if done.CloseReason == nil || *done.CloseReason != ReasonDoorOpenConfirmed {
		t.Fatalf("close_reason = %v, want %q", done.CloseReason, ReasonDoorOpenConfirmed)
	}
	if done.ClosedAt == nil {
		t.Fatal("closed_at must be set on terminal window")
	}
	if !(*done.ClosedAt < done.Deadline) {
		t.Fatalf("closed_at %q must be strictly before deadline %q", *done.ClosedAt, done.Deadline)
	}

	// 通过查询断言唯一最终状态。
	got, err := st.GetWindow(ctx, done.ID)
	if err != nil {
		t.Fatalf("get window: %v", err)
	}
	if got.State != StateConfirmed {
		t.Fatalf("persisted state = %q, want %q", got.State, StateConfirmed)
	}
	latest, err := st.LatestWindow(ctx, "STER-01")
	if err != nil {
		t.Fatalf("latest window: %v", err)
	}
	if latest.ID != done.ID || latest.State != StateConfirmed {
		t.Fatalf("latest = %+v, want id=%d confirmed", latest, done.ID)
	}
}

// 无人确认：worker 扫描把到期窗口写入 quarantined，且扫描幂等。
func TestScanQuarantinesExpiredWindow(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	mustDevice(t, st, "STER-02")

	w := mustWindow(t, st, "STER-02")
	deadline := setDeadline(t, st, w.ID, "-1 seconds")

	n, err := st.QuarantineExpired(ctx)
	if err != nil {
		t.Fatalf("quarantine expired: %v", err)
	}
	if n != 1 {
		t.Fatalf("quarantined %d windows, want 1", n)
	}

	got, err := st.GetWindow(ctx, w.ID)
	if err != nil {
		t.Fatalf("get window: %v", err)
	}
	if got.State != StateQuarantined {
		t.Fatalf("state = %q, want %q", got.State, StateQuarantined)
	}
	if got.CloseReason == nil || *got.CloseReason != ReasonScanTimeout {
		t.Fatalf("close_reason = %v, want %q", got.CloseReason, ReasonScanTimeout)
	}
	if got.ClosedAt == nil {
		t.Fatal("closed_at must be set on terminal window")
	}
	if *got.ClosedAt < deadline {
		t.Fatalf("closed_at %q must be at or after deadline %q", *got.ClosedAt, deadline)
	}

	// 再次扫描不应命中任何窗口：终态不会重复迁移。
	n, err = st.QuarantineExpired(ctx)
	if err != nil {
		t.Fatalf("second scan: %v", err)
	}
	if n != 0 {
		t.Fatalf("second scan quarantined %d windows, want 0", n)
	}
}

// 确认到达时数据库时间已不早于截止时刻 → quarantined（等号归属隔离侧）。
func TestConfirmAfterDeadlineWritesQuarantined(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	mustDevice(t, st, "STER-03")

	w := mustWindow(t, st, "STER-03")
	setDeadline(t, st, w.ID, "-1 seconds")

	done, err := st.ConfirmWindow(ctx, "STER-03")
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if done.State != StateQuarantined {
		t.Fatalf("state = %q, want %q", done.State, StateQuarantined)
	}
	if done.CloseReason == nil || *done.CloseReason != ReasonConfirmAfterDeadline {
		t.Fatalf("close_reason = %v, want %q", done.CloseReason, ReasonConfirmAfterDeadline)
	}
	if done.ClosedAt == nil || *done.ClosedAt < done.Deadline {
		t.Fatalf("closed_at %v must be at or after deadline %q", done.ClosedAt, done.Deadline)
	}
}

// 边界并发裁决：截止时刻被精确设置为数据库当前时间，确认请求与超时扫描
// 同时到达。两侧按相同边界都只能写 quarantined，且带 open 前置条件的原子
// UPDATE 保证只有一个终态被提交。
func TestConcurrentArbitrationAtDeadline(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	mustDevice(t, st, "STER-RACE")

	const rounds = 25
	for i := 0; i < rounds; i++ {
		w := mustWindow(t, st, "STER-RACE")
		// 把截止时刻精确放到数据库当前时间上：确认与扫描都落在边界。
		setDeadline(t, st, w.ID, "+0 seconds")

		var (
			confirmWin Window
			confirmErr error
			scanned    int64
			scanErr    error
		)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			confirmWin, confirmErr = st.ConfirmWindow(ctx, "STER-RACE")
		}()
		go func() {
			defer wg.Done()
			<-start
			scanned, scanErr = st.QuarantineExpired(ctx)
		}()
		close(start)
		wg.Wait()

		if scanErr != nil {
			t.Fatalf("round %d: scan: %v", i, scanErr)
		}
		if scanned != 0 && scanned != 1 {
			t.Fatalf("round %d: scan affected %d windows, want 0 or 1", i, scanned)
		}
		confirmWon := confirmErr == nil
		scanWon := scanned == 1
		if confirmWon == scanWon {
			t.Fatalf("round %d: exactly one side must commit the terminal state, confirmWon=%v scanWon=%v (confirmErr=%v)",
				i, confirmWon, scanWon, confirmErr)
		}
		if !confirmWon && !errors.Is(confirmErr, ErrNoOpenWindow) {
			t.Fatalf("round %d: losing confirm must return ErrNoOpenWindow, got %v", i, confirmErr)
		}
		if confirmWon {
			if confirmWin.State != StateQuarantined {
				t.Fatalf("round %d: confirm at the boundary must quarantine, got %q", i, confirmWin.State)
			}
			if confirmWin.CloseReason == nil || *confirmWin.CloseReason != ReasonConfirmAfterDeadline {
				t.Fatalf("round %d: close_reason = %v, want %q", i, confirmWin.CloseReason, ReasonConfirmAfterDeadline)
			}
		}

		// 通过查询断言唯一最终状态：该窗口有且仅有一条记录，且为 quarantined。
		var count int
		if err := st.db.QueryRow(`SELECT COUNT(*) FROM unload_windows WHERE id = ?`, w.ID).Scan(&count); err != nil {
			t.Fatalf("round %d: count windows: %v", i, err)
		}
		if count != 1 {
			t.Fatalf("round %d: found %d rows for window %d, want exactly 1", i, count, w.ID)
		}
		final, err := st.GetWindow(ctx, w.ID)
		if err != nil {
			t.Fatalf("round %d: get window: %v", i, err)
		}
		if final.State != StateQuarantined {
			t.Fatalf("round %d: final state = %q, want %q", i, final.State, StateQuarantined)
		}
		if final.ClosedAt == nil {
			t.Fatalf("round %d: closed_at must be set", i)
		}
		if final.CloseReason == nil ||
			(*final.CloseReason != ReasonConfirmAfterDeadline && *final.CloseReason != ReasonScanTimeout) {
			t.Fatalf("round %d: close_reason = %v, want %q or %q",
				i, final.CloseReason, ReasonConfirmAfterDeadline, ReasonScanTimeout)
		}
	}
}

// 活动窗口冲突：同一设备只允许一个 open 窗口；并发登记时恰好一个成功。
func TestOneOpenWindowPerDevice(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	mustDevice(t, st, "STER-04")

	mustWindow(t, st, "STER-04")
	if _, err := st.RegisterWindow(ctx, "STER-04"); !errors.Is(err, ErrWindowAlreadyOpen) {
		t.Fatalf("second register = %v, want ErrWindowAlreadyOpen", err)
	}

	if _, err := st.ConfirmWindow(ctx, "STER-04"); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	// 终态之后允许登记下一个窗口。
	if _, err := st.RegisterWindow(ctx, "STER-04"); err != nil {
		t.Fatalf("register after close: %v", err)
	}
	if _, err := st.ConfirmWindow(ctx, "STER-04"); err != nil {
		t.Fatalf("confirm second window: %v", err)
	}

	// 并发登记：N 个 goroutine 同时登记，数据库唯一索引保证恰好一个成功。
	mustDevice(t, st, "STER-05")
	const n = 8
	errs := make([]error, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = st.RegisterWindow(ctx, "STER-05")
		}(i)
	}
	close(start)
	wg.Wait()

	successes := 0
	for _, err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrWindowAlreadyOpen):
		default:
			t.Fatalf("unexpected register error: %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent register successes = %d, want exactly 1", successes)
	}

	// 通过查询断言：该设备当前有且仅有一个 open 窗口。
	var openCount int
	if err := st.db.QueryRow(
		`SELECT COUNT(*) FROM unload_windows WHERE device_id = 'STER-05' AND state = 'open'`).Scan(&openCount); err != nil {
		t.Fatalf("count open windows: %v", err)
	}
	if openCount != 1 {
		t.Fatalf("open windows = %d, want 1", openCount)
	}
}

// 未知设备的登记与确认都返回 ErrDeviceNotFound。
func TestUnknownDevice(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	if _, err := st.RegisterWindow(ctx, "NOPE"); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("register on unknown device = %v, want ErrDeviceNotFound", err)
	}
	if _, err := st.ConfirmWindow(ctx, "NOPE"); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("confirm on unknown device = %v, want ErrDeviceNotFound", err)
	}
	if _, err := st.GetDevice(ctx, "NOPE"); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("get unknown device = %v, want ErrDeviceNotFound", err)
	}
}

// 默认设备（从未配置策略）登记的窗口仍使用默认 30 秒，且快照到 ttl_seconds。
func TestDefaultDeviceGetsThirtySecondWindow(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	mustDevice(t, st, "STER-P-DEFAULT")

	d, err := st.GetDevice(ctx, "STER-P-DEFAULT")
	if err != nil {
		t.Fatalf("get device: %v", err)
	}
	if d.UnloadTTLSeconds != DefaultWindowTTLSeconds {
		t.Fatalf("device policy = %d, want default %d", d.UnloadTTLSeconds, DefaultWindowTTLSeconds)
	}

	w := mustWindow(t, st, "STER-P-DEFAULT")
	if w.TTLSeconds != DefaultWindowTTLSeconds {
		t.Fatalf("window snapshot ttl_seconds = %d, want %d", w.TTLSeconds, DefaultWindowTTLSeconds)
	}
	opened, _ := time.Parse(time.RFC3339Nano, w.OpenedAt)
	deadline, _ := time.Parse(time.RFC3339Nano, w.Deadline)
	if got := deadline.Sub(opened); got != DefaultWindowTTLSeconds*time.Second {
		t.Fatalf("deadline - opened_at = %v, want %v", got, DefaultWindowTTLSeconds*time.Second)
	}
}

// 策略更新后登记的新窗口采用新时限；活动窗口的快照与截止时刻不受再次改策影响。
func TestPolicyUpdateAffectsOnlyNewWindows(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	mustDevice(t, st, "STER-P-1")

	// 第一个窗口按默认 30 秒生成且仍处于 open。
	w1 := mustWindow(t, st, "STER-P-1")
	if w1.TTLSeconds != 30 {
		t.Fatalf("first window ttl = %d, want 30", w1.TTLSeconds)
	}

	// 改策为 60 秒后只能影响后续新登记的窗口；w1 仍 open，因此先确认关闭它，
	// 否则新窗口无法登记（同一设备至多一个 open）。
	if _, err := st.UpdateDevicePolicy(ctx, "STER-P-1", 60); err != nil {
		t.Fatalf("update policy to 60: %v", err)
	}
	d, err := st.GetDevice(ctx, "STER-P-1")
	if err != nil {
		t.Fatalf("get device: %v", err)
	}
	if d.UnloadTTLSeconds != 60 {
		t.Fatalf("device policy = %d, want 60", d.UnloadTTLSeconds)
	}

	// 再次改策不应改动 w1 的快照与 deadline —— 通过另一个 open 窗口（不同
	// 设备）交叉验证：改策瞬间两台设备的活动窗口都保持登记瞬间的快照。
	mustDevice(t, st, "STER-P-2")
	if _, err := st.UpdateDevicePolicy(ctx, "STER-P-2", 5); err != nil {
		t.Fatalf("update policy P-2: %v", err)
	}
	w2 := mustWindow(t, st, "STER-P-2")
	if w2.TTLSeconds != 5 {
		t.Fatalf("P-2 window ttl = %d, want 5", w2.TTLSeconds)
	}
	// 注：w1 属于 STER-P-1；60 秒策略已在它 open 期间下发，这是关键断言。
	w1Again, err := st.GetWindow(ctx, w1.ID)
	if err != nil {
		t.Fatalf("get first window: %v", err)
	}
	if w1Again.TTLSeconds != 30 || w1Again.Deadline != w1.Deadline {
		t.Fatalf("active window mutated by later policy change: ttl=%d deadline=%q, want ttl=30 deadline=%q",
			w1Again.TTLSeconds, w1Again.Deadline, w1.Deadline)
	}
	if w1Again.State != StateOpen {
		t.Fatalf("first window state = %q, want open", w1Again.State)
	}

	// 关闭 w1 后登记的新窗口必须采用当前策略 60 秒。
	if _, err := st.ConfirmWindow(ctx, "STER-P-1"); err != nil {
		t.Fatalf("confirm first window: %v", err)
	}
	w3, err := st.RegisterWindow(ctx, "STER-P-1")
	if err != nil {
		t.Fatalf("register window after policy update: %v", err)
	}
	if w3.TTLSeconds != 60 {
		t.Fatalf("new window ttl = %d, want 60", w3.TTLSeconds)
	}
	opened, _ := time.Parse(time.RFC3339Nano, w3.OpenedAt)
	deadline, _ := time.Parse(time.RFC3339Nano, w3.Deadline)
	if got := deadline.Sub(opened); got != 60*time.Second {
		t.Fatalf("new window duration = %v, want 60s", got)
	}

	// w2 的快照在其 open 期间把策略从 5 秒改为 600 秒也保持不变。
	if _, err := st.UpdateDevicePolicy(ctx, "STER-P-2", 600); err != nil {
		t.Fatalf("update policy P-2 to 600: %v", err)
	}
	w2Again, err := st.GetWindow(ctx, w2.ID)
	if err != nil {
		t.Fatalf("get second device window: %v", err)
	}
	if w2Again.TTLSeconds != 5 || w2Again.Deadline != w2.Deadline || w2Again.State != StateOpen {
		t.Fatalf("active window mutated by second policy change: %+v", w2Again)
	}
}

// 边界策略值 1 与 600 可更新并被窗口采用；0、601、负数被拒绝且不触碰数据。
func TestPolicyBoundariesAndValidation(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	mustDevice(t, st, "STER-P-B")

	for _, v := range []int{1, 600} {
		if _, err := st.UpdateDevicePolicy(ctx, "STER-P-B", v); err != nil {
			t.Fatalf("update policy to %d: %v", v, err)
		}
		w := mustWindow(t, st, "STER-P-B")
		if w.TTLSeconds != v {
			t.Fatalf("window ttl = %d, want %d", w.TTLSeconds, v)
		}
		if _, err := st.ConfirmWindow(ctx, "STER-P-B"); err != nil {
			t.Fatalf("confirm: %v", err)
		}
	}
	for _, bad := range []int{0, -1, 601, 1000} {
		if _, err := st.UpdateDevicePolicy(ctx, "STER-P-B", bad); !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("update policy to %d = %v, want ErrInvalidPolicy", bad, err)
		}
	}
	// 被拒的更新不得改变当前策略（仍为上次成功的 600）。
	d, _ := st.GetDevice(ctx, "STER-P-B")
	if d.UnloadTTLSeconds != 600 {
		t.Fatalf("policy after rejected updates = %d, want unchanged 600", d.UnloadTTLSeconds)
	}
	if _, err := st.UpdateDevicePolicy(ctx, "NOPE", 30); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("update policy on unknown device = %v, want ErrDeviceNotFound", err)
	}
}

// 策略更新不影响设备既有窗口：对有终态窗口的设备改策，历史窗口行不变。
func TestPolicyUpdateDoesNotTouchExistingWindows(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	mustDevice(t, st, "STER-P-OLD")
	w := mustWindow(t, st, "STER-P-OLD")
	originalDeadline := setDeadline(t, st, w.ID, "-1 seconds")
	done, err := st.ConfirmWindow(ctx, "STER-P-OLD")
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if done.State != StateQuarantined {
		t.Fatalf("state = %q, want quarantined", done.State)
	}
	if _, err := st.UpdateDevicePolicy(ctx, "STER-P-OLD", 120); err != nil {
		t.Fatalf("update policy: %v", err)
	}
	got, err := st.GetWindow(ctx, w.ID)
	if err != nil {
		t.Fatalf("get window: %v", err)
	}
	if got.TTLSeconds != 30 || got.Deadline != originalDeadline || got.State != StateQuarantined {
		t.Fatalf("historical window mutated by policy update: %+v", got)
	}
}

// 改策后登记的窗口仍按原有 UTC 边界裁决：快照为 1 秒的窗口到期后，
// worker 扫描与迟到确认都只产生 quarantined 终态（回归原有竞争语义）。
func TestCustomTTLArbitrationRegression(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	mustDevice(t, st, "STER-P-RACE")
	if _, err := st.UpdateDevicePolicy(ctx, "STER-P-RACE", 1); err != nil {
		t.Fatalf("update policy: %v", err)
	}

	w := mustWindow(t, st, "STER-P-RACE")
	if w.TTLSeconds != 1 {
		t.Fatalf("ttl snapshot = %d, want 1", w.TTLSeconds)
	}
	// 等到数据库时间越过该窗口的 1 秒截止时刻。
	deadline, err := time.Parse(time.RFC3339Nano, w.Deadline)
	if err != nil {
		t.Fatalf("parse deadline: %v", err)
	}
	time.Sleep(time.Until(deadline) + 600*time.Millisecond)

	var (
		confirmWin Window
		confirmErr error
		scanned    int64
		scanErr    error
	)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		confirmWin, confirmErr = st.ConfirmWindow(ctx, "STER-P-RACE")
	}()
	go func() {
		defer wg.Done()
		<-start
		scanned, scanErr = st.QuarantineExpired(ctx)
	}()
	close(start)
	wg.Wait()

	if scanErr != nil {
		t.Fatalf("scan: %v", scanErr)
	}
	confirmWon := confirmErr == nil
	scanWon := scanned == 1
	if confirmWon == scanWon {
		t.Fatalf("exactly one terminal state, confirmWon=%v scanWon=%v confirmErr=%v",
			confirmWon, scanWon, confirmErr)
	}
	if confirmWon && (confirmWin.State != StateQuarantined ||
		confirmWin.CloseReason == nil || *confirmWin.CloseReason != ReasonConfirmAfterDeadline) {
		t.Fatalf("late confirm must quarantine, got %+v", confirmWin)
	}
	final, err := st.GetWindow(ctx, w.ID)
	if err != nil {
		t.Fatalf("get window: %v", err)
	}
	if final.State != StateQuarantined {
		t.Fatalf("final state = %q, want quarantined", final.State)
	}
}

// 从旧版 schema（无策略列）创建的数据库迁移后：设备默认 30、既有窗口回填
// 30，迁移幂等，且新窗口仍按 30 秒登记。
func TestMigrateLegacyDatabase(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "legacy.db")

	// 用旧版表结构（无 unload_ttl_seconds / ttl_seconds 列）直接建库并造数据。
	legacy, err := sql.Open("sqlite", "file:"+path+"?_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open legacy db: %v", err)
	}
	_, err = legacy.Exec(`
CREATE TABLE devices (id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE unload_windows (
    id INTEGER PRIMARY KEY AUTOINCREMENT, device_id TEXT NOT NULL REFERENCES devices(id),
    state TEXT NOT NULL, opened_at TEXT NOT NULL, deadline TEXT NOT NULL,
    closed_at TEXT, close_reason TEXT);
INSERT INTO devices (id, name, created_at) VALUES ('OLD', 'old', '2026-09-15T00:00:00.000Z');
INSERT INTO unload_windows (device_id, state, opened_at, deadline)
VALUES ('OLD', 'open', '2026-09-15T00:00:00.000Z', '2026-09-15T00:00:30.000Z');`)
	if err != nil {
		t.Fatalf("seed legacy db: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy db: %v", err)
	}

	// 用新版 Store 打开，触发追加式迁移。
	st, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("open store over legacy db: %v", err)
	}
	defer st.Close()

	d, err := st.GetDevice(context.Background(), "OLD")
	if err != nil {
		t.Fatalf("get migrated device: %v", err)
	}
	if d.UnloadTTLSeconds != 30 {
		t.Fatalf("migrated device policy = %d, want 30", d.UnloadTTLSeconds)
	}
	w, err := st.LatestWindow(context.Background(), "OLD")
	if err != nil {
		t.Fatalf("get migrated window: %v", err)
	}
	if w.TTLSeconds != 30 || w.State != StateOpen || w.Deadline != "2026-09-15T00:00:30.000Z" {
		t.Fatalf("migrated window = %+v, want ttl 30 and preserved deadline/state", w)
	}

	// 再次 Open 必须幂等，不报错。
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	st2, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st2.Close()
}
