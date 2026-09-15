package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"cssd-unload/internal/store"
)

func newTestHandler(t *testing.T) http.Handler {
	t.Helper()
	gin.SetMode(gin.TestMode)
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "api-test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return NewServer(st).Handler()
}

func do(t *testing.T, h http.Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		rdr = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, rdr)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var parsed map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("%s %s: response is not JSON: %v: %s", method, path, err, rec.Body.String())
	}
	return rec.Code, parsed
}

func errCode(t *testing.T, resp map[string]any) string {
	t.Helper()
	e, ok := resp["error"].(map[string]any)
	if !ok {
		t.Fatalf("response has no error object: %v", resp)
	}
	code, _ := e["code"].(string)
	return code
}

func windowOf(t *testing.T, resp map[string]any) map[string]any {
	t.Helper()
	w, ok := resp["window"].(map[string]any)
	if !ok {
		t.Fatalf("response has no window object: %v", resp)
	}
	return w
}

func TestHTTPConfirmFlow(t *testing.T) {
	h := newTestHandler(t)

	status, _ := do(t, h, http.MethodPost, "/devices",
		map[string]any{"device_id": "STER-HTTP-1", "name": "HTTP Sterilizer 1"})
	if status != http.StatusCreated {
		t.Fatalf("create device status = %d, want 201", status)
	}

	status, resp := do(t, h, http.MethodPost, "/devices/STER-HTTP-1/windows", map[string]any{})
	if status != http.StatusCreated {
		t.Fatalf("register window status = %d, want 201 (%v)", status, resp)
	}
	if got := windowOf(t, resp)["state"]; got != store.StateOpen {
		t.Fatalf("window state = %v, want open", got)
	}

	status, resp = do(t, h, http.MethodPost, "/devices/STER-HTTP-1/confirm", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("confirm status = %d, want 200 (%v)", status, resp)
	}
	if got := windowOf(t, resp)["state"]; got != store.StateConfirmed {
		t.Fatalf("window state = %v, want confirmed", got)
	}

	status, resp = do(t, h, http.MethodGet, "/devices/STER-HTTP-1/status", nil)
	if status != http.StatusOK {
		t.Fatalf("status query = %d, want 200", status)
	}
	if got := windowOf(t, resp)["state"]; got != store.StateConfirmed {
		t.Fatalf("queried state = %v, want confirmed", got)
	}
}

func TestHTTPRegisterConflictReturnsStructuredError(t *testing.T) {
	h := newTestHandler(t)
	do(t, h, http.MethodPost, "/devices", map[string]any{"device_id": "STER-HTTP-2", "name": "HTTP Sterilizer 2"})

	status, _ := do(t, h, http.MethodPost, "/devices/STER-HTTP-2/windows", nil)
	if status != http.StatusCreated {
		t.Fatalf("first register status = %d, want 201", status)
	}

	status, resp := do(t, h, http.MethodPost, "/devices/STER-HTTP-2/windows", nil)
	if status != http.StatusConflict {
		t.Fatalf("second register status = %d, want 409 (%v)", status, resp)
	}
	if got := errCode(t, resp); got != CodeWindowAlreadyOpen {
		t.Fatalf("error code = %q, want %q", got, CodeWindowAlreadyOpen)
	}
	e := resp["error"].(map[string]any)
	details, ok := e["details"].(map[string]any)
	if !ok || details["deadline"] == nil {
		t.Fatalf("conflict error must carry current window details: %v", e)
	}
}

func TestHTTPClientCannotSetDeadline(t *testing.T) {
	h := newTestHandler(t)
	do(t, h, http.MethodPost, "/devices", map[string]any{"device_id": "STER-HTTP-3", "name": "HTTP Sterilizer 3"})

	status, resp := do(t, h, http.MethodPost, "/devices/STER-HTTP-3/windows",
		map[string]any{"deadline": "2030-01-01T00:00:00.000Z"})
	if status != http.StatusBadRequest {
		t.Fatalf("register with client deadline status = %d, want 400 (%v)", status, resp)
	}
	if got := errCode(t, resp); got != CodeInvalidRequest {
		t.Fatalf("error code = %q, want %q", got, CodeInvalidRequest)
	}

	// 被拒绝的请求不得产生任何窗口。
	status, resp = do(t, h, http.MethodGet, "/devices/STER-HTTP-3/status", nil)
	if status != http.StatusOK {
		t.Fatalf("status query = %d, want 200", status)
	}
	if resp["window"] != nil {
		t.Fatalf("rejected request must not create a window: %v", resp["window"])
	}
}

func TestHTTPConfirmWithoutOpenWindow(t *testing.T) {
	h := newTestHandler(t)
	do(t, h, http.MethodPost, "/devices", map[string]any{"device_id": "STER-HTTP-4", "name": "HTTP Sterilizer 4"})

	status, resp := do(t, h, http.MethodPost, "/devices/STER-HTTP-4/confirm", nil)
	if status != http.StatusConflict {
		t.Fatalf("confirm without window status = %d, want 409 (%v)", status, resp)
	}
	if got := errCode(t, resp); got != CodeNoOpenWindow {
		t.Fatalf("error code = %q, want %q", got, CodeNoOpenWindow)
	}

	status, resp = do(t, h, http.MethodPost, "/devices/NOPE/confirm", nil)
	if status != http.StatusNotFound {
		t.Fatalf("confirm on unknown device status = %d, want 404 (%v)", status, resp)
	}
	if got := errCode(t, resp); got != CodeDeviceNotFound {
		t.Fatalf("error code = %q, want %q", got, CodeDeviceNotFound)
	}
}

func detailsOf(t *testing.T, resp map[string]any) map[string]any {
	t.Helper()
	e, ok := resp["error"].(map[string]any)
	if !ok {
		t.Fatalf("response has no error object: %v", resp)
	}
	d, _ := e["details"].(map[string]any)
	return d
}

// PUT policy：成功返回当前策略；设备不存在沿用原 404 DEVICE_NOT_FOUND。
func TestHTTPUpdateDevicePolicy(t *testing.T) {
	h := newTestHandler(t)
	do(t, h, http.MethodPost, "/devices", map[string]any{"device_id": "STER-P-1", "name": "Policy Sterilizer 1"})

	status, resp := do(t, h, http.MethodPut, "/devices/STER-P-1/policy",
		map[string]any{"unload_ttl_seconds": 45})
	if status != http.StatusOK {
		t.Fatalf("update policy status = %d, want 200 (%v)", status, resp)
	}
	dev, ok := resp["device"].(map[string]any)
	if !ok {
		t.Fatalf("response has no device object: %v", resp)
	}
	if got := dev["unload_ttl_seconds"]; got != float64(45) {
		t.Fatalf("device unload_ttl_seconds = %v, want 45", got)
	}

	// 边界值 1 和 600 可接受。
	for _, v := range []int{1, 600} {
		status, resp = do(t, h, http.MethodPut, "/devices/STER-P-1/policy",
			map[string]any{"unload_ttl_seconds": v})
		if status != http.StatusOK {
			t.Fatalf("update policy to %d status = %d, want 200 (%v)", v, status, resp)
		}
	}

	// 未知设备 → 原有未找到错误。
	status, resp = do(t, h, http.MethodPut, "/devices/NOPE/policy",
		map[string]any{"unload_ttl_seconds": 30})
	if status != http.StatusNotFound {
		t.Fatalf("unknown device status = %d, want 404 (%v)", status, resp)
	}
	if got := errCode(t, resp); got != CodeDeviceNotFound {
		t.Fatalf("error code = %q, want %q", got, CodeDeviceNotFound)
	}
}

// 非法策略值（缺失、非整数、越界、多余字段、畸形 JSON）一律 400
// INVALID_REQUEST，并携带结构化 details；且不写入任何策略。
func TestHTTPUpdateDevicePolicyRejectsInvalidValues(t *testing.T) {
	h := newTestHandler(t)
	do(t, h, http.MethodPost, "/devices", map[string]any{"device_id": "STER-P-2", "name": "Policy Sterilizer 2"})

	cases := []struct {
		name string
		body any
	}{
		{"missing field", map[string]any{}},
		{"zero", map[string]any{"unload_ttl_seconds": 0}},
		{"negative", map[string]any{"unload_ttl_seconds": -5}},
		{"above max", map[string]any{"unload_ttl_seconds": 601}},
		{"float", map[string]any{"unload_ttl_seconds": 30.5}},
		{"string number", map[string]any{"unload_ttl_seconds": "30"}},
		{"boolean", map[string]any{"unload_ttl_seconds": true}},
		{"null value", map[string]any{"unload_ttl_seconds": nil}},
		{"unknown field alongside", map[string]any{"unload_ttl_seconds": 30, "extra": 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, resp := do(t, h, http.MethodPut, "/devices/STER-P-2/policy", tc.body)
			if status != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (%v)", status, resp)
			}
			if got := errCode(t, resp); got != CodeInvalidRequest {
				t.Fatalf("error code = %q, want %q", got, CodeInvalidRequest)
			}
			d := detailsOf(t, resp)
			if d["field"] != "unload_ttl_seconds" || d["min"] != float64(1) || d["max"] != float64(600) {
				t.Fatalf("error details = %v, want field/min/max metadata", d)
			}
		})
	}

	// 非 JSON / 数组 / 多文档等畸形请求体同样 400。
	rawCases := []string{
		`not json`,
		`[30]`,
		`{"unload_ttl_seconds":30} {"unload_ttl_seconds":40}`,
		`{"unload_ttl_seconds": 1e2}`, // 指数写法不是整数字面量
	}
	for _, body := range rawCases {
		req := httptest.NewRequest(http.MethodPut, "/devices/STER-P-2/policy", bytes.NewReader([]byte(body)))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("body %q: status = %d, want 400: %s", body, rec.Code, rec.Body.String())
		}
		var parsed map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
			t.Fatalf("body %q: response not JSON: %v", body, err)
		}
		if got := errCode(t, parsed); got != CodeInvalidRequest {
			t.Fatalf("body %q: error code = %q, want %q", body, got, CodeInvalidRequest)
		}
	}

	// 全部拒绝后策略仍为默认 30。
	status, resp := do(t, h, http.MethodGet, "/devices/STER-P-2/status", nil)
	if status != http.StatusOK {
		t.Fatalf("status = %d (%v)", status, resp)
	}
	dev := resp["device"].(map[string]any)
	if got := dev["unload_ttl_seconds"]; got != float64(30) {
		t.Fatalf("policy after rejected updates = %v, want unchanged 30", got)
	}
}

// 端到端策略语义：默认 30 秒 → 改策 → 新窗口采用新时限并快照到窗口；
// 状态查询同时返回当前策略与最近窗口采用的时限；改策不触碰活动窗口。
func TestHTTPPolicyWindowSnapshotAndStatus(t *testing.T) {
	h := newTestHandler(t)
	do(t, h, http.MethodPost, "/devices", map[string]any{"device_id": "STER-P-3", "name": "Policy Sterilizer 3"})

	// 默认设备：窗口 30 秒、status 中策略与窗口快照都是 30。
	status, resp := do(t, h, http.MethodPost, "/devices/STER-P-3/windows", nil)
	if status != http.StatusCreated {
		t.Fatalf("register status = %d (%v)", status, resp)
	}
	if got := windowOf(t, resp)["ttl_seconds"]; got != float64(30) {
		t.Fatalf("default window ttl_seconds = %v, want 30", got)
	}
	firstID := windowOf(t, resp)["id"]

	// 活动窗口期间改策：接口成功，活动窗口快照不变。
	status, resp = do(t, h, http.MethodPut, "/devices/STER-P-3/policy",
		map[string]any{"unload_ttl_seconds": 90})
	if status != http.StatusOK {
		t.Fatalf("update policy while window open status = %d (%v)", status, resp)
	}
	status, resp = do(t, h, http.MethodGet, "/devices/STER-P-3/status", nil)
	if status != http.StatusOK {
		t.Fatalf("status = %d (%v)", status, resp)
	}
	dev := resp["device"].(map[string]any)
	if got := dev["unload_ttl_seconds"]; got != float64(90) {
		t.Fatalf("current policy = %v, want 90", got)
	}
	w := resp["window"].(map[string]any)
	if w["id"] != firstID || w["ttl_seconds"] != float64(30) || w["state"] != "open" {
		t.Fatalf("active window must keep ttl snapshot 30: %v", w)
	}

	// 关闭活动窗口后登记的新窗口采用 90 秒。
	status, _ = do(t, h, http.MethodPost, "/devices/STER-P-3/confirm", nil)
	if status != http.StatusOK {
		t.Fatalf("confirm status = %d", status)
	}
	status, resp = do(t, h, http.MethodPost, "/devices/STER-P-3/windows", nil)
	if status != http.StatusCreated {
		t.Fatalf("register second window status = %d (%v)", status, resp)
	}
	w2 := windowOf(t, resp)
	if w2["ttl_seconds"] != float64(90) {
		t.Fatalf("new window ttl_seconds = %v, want 90", w2["ttl_seconds"])
	}
	opened, _ := time.Parse(time.RFC3339Nano, w2["opened_at"].(string))
	deadline, _ := time.Parse(time.RFC3339Nano, w2["deadline"].(string))
	if deadline.Sub(opened) != 90*time.Second {
		t.Fatalf("deadline - opened_at = %v, want 90s", deadline.Sub(opened))
	}
}
