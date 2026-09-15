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
