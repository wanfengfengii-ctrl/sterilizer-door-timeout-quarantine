package store

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
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
