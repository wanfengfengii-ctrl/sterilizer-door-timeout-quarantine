// verify 是一次性验收客户端：对运行中的 API + worker 执行端到端验收，
// 覆盖及时确认、无人确认超时隔离、活动窗口冲突、客户端禁止指定截止时刻、
// 终态后重复确认，以及按设备型号的卸载时限策略（默认 30 秒、改策后新窗口
// 采用新时限、活动窗口快照不受再次改策影响、非法参数结构化错误），
// 全部通过则以退出码 0 结束。
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type window struct {
	ID          int64   `json:"id"`
	DeviceID    string  `json:"device_id"`
	State       string  `json:"state"`
	OpenedAt    string  `json:"opened_at"`
	Deadline    string  `json:"deadline"`
	TTLSeconds  int     `json:"ttl_seconds"`
	ClosedAt    *string `json:"closed_at"`
	CloseReason *string `json:"close_reason"`
}

type device struct {
	DeviceID         string `json:"device_id"`
	Name             string `json:"name"`
	UnloadTTLSeconds int    `json:"unload_ttl_seconds"`
}

type checker struct {
	base   string
	client *http.Client
	failed int
}

func (c *checker) check(name string, ok bool, detail string) {
	if ok {
		fmt.Printf("PASS  %s\n", name)
		return
	}
	c.failed++
	fmt.Printf("FAIL  %s: %s\n", name, detail)
}

func (c *checker) doJSON(method, path string, body any) (int, []byte, error) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.base+path, rdr)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return resp.StatusCode, data, err
}

func decodeWindow(data []byte) (window, error) {
	var resp struct {
		Window *window `json:"window"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return window{}, err
	}
	if resp.Window == nil {
		return window{}, fmt.Errorf("response has no window field: %s", data)
	}
	return *resp.Window, nil
}

func decodeStatus(data []byte) (*window, error) {
	var resp struct {
		Window *window `json:"window"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return resp.Window, nil
}

// decodeStatusFull 返回状态查询中的设备（含当前策略）与最近窗口（含时限快照）。
func decodeStatusFull(data []byte) (device, *window, error) {
	var resp struct {
		Device device  `json:"device"`
		Window *window `json:"window"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return device{}, nil, err
	}
	return resp.Device, resp.Window, nil
}

// decodeDevice 解析 PUT policy 与 POST /devices 响应中的 device 对象。
func decodeDevice(data []byte) (device, error) {
	var resp struct {
		Device device `json:"device"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return device{}, err
	}
	if resp.Device.DeviceID == "" {
		return device{}, fmt.Errorf("response has no device field: %s", data)
	}
	return resp.Device, nil
}

func decodeErrDetails(data []byte) map[string]any {
	var resp struct {
		Error struct {
			Details map[string]any `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil
	}
	return resp.Error.Details
}

func decodeErrCode(data []byte) string {
	var resp struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return ""
	}
	return resp.Error.Code
}

func main() {
	base := strings.TrimRight(envOr("API_BASE_URL", "http://localhost:8080"), "/")
	c := &checker{base: base, client: &http.Client{Timeout: 5 * time.Second}}

	// 0. 等待 API 就绪（worker 无需直接探测，超时场景会间接验证它）。
	ready := false
	for i := 0; i < 60 && !ready; i++ {
		status, _, err := c.doJSON(http.MethodGet, "/health", nil)
		ready = err == nil && status == http.StatusOK
		if !ready {
			time.Sleep(500 * time.Millisecond)
		}
	}
	if !ready {
		fmt.Println("FAIL  api not healthy within 30s")
		fmt.Println("VERIFY: FAIL")
		os.Exit(1)
	}
	fmt.Println("PASS  api healthy")

	runID := fmt.Sprintf("%d", time.Now().UnixNano())
	devA := "VERIFY-A-" + runID
	devB := "VERIFY-B-" + runID
	devC := "VERIFY-C-" + runID

	// 场景 1：及时确认 → confirmed。
	status, body, err := c.doJSON(http.MethodPost, "/devices",
		map[string]any{"device_id": devA, "name": "Verify Sterilizer A"})
	c.check("create device A", err == nil && status == http.StatusCreated,
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devA+"/windows", map[string]any{})
	w1, werr := decodeWindow(body)
	c.check("register window A -> open", err == nil && status == http.StatusCreated && werr == nil && w1.State == "open",
		fmt.Sprintf("status=%d err=%v werr=%v body=%s", status, err, werr, body))

	opened, oerr := time.Parse(time.RFC3339Nano, w1.OpenedAt)
	deadline, derr := time.Parse(time.RFC3339Nano, w1.Deadline)
	c.check("deadline assigned by database as opened_at + 30s",
		oerr == nil && derr == nil && deadline.Sub(opened) == 30*time.Second,
		fmt.Sprintf("opened_at=%q deadline=%q", w1.OpenedAt, w1.Deadline))

	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devA+"/confirm", map[string]any{})
	cw, werr := decodeWindow(body)
	c.check("confirm within window -> confirmed",
		err == nil && status == http.StatusOK && werr == nil && cw.State == "confirmed",
		fmt.Sprintf("status=%d err=%v werr=%v body=%s", status, err, werr, body))
	c.check("close reason is door_open_confirmed",
		cw.CloseReason != nil && *cw.CloseReason == "door_open_confirmed",
		fmt.Sprintf("close_reason=%v", cw.CloseReason))
	c.check("closed_at set and strictly before deadline",
		cw.ClosedAt != nil && *cw.ClosedAt < cw.Deadline,
		fmt.Sprintf("closed_at=%v deadline=%q", cw.ClosedAt, cw.Deadline))

	status, body, err = c.doJSON(http.MethodGet, "/devices/"+devA+"/status", nil)
	sw, serr := decodeStatus(body)
	c.check("status query shows confirmed",
		err == nil && status == http.StatusOK && serr == nil && sw != nil && sw.State == "confirmed",
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	// 场景 2：同一设备重复登记 → 409 WINDOW_ALREADY_OPEN。
	c.doJSON(http.MethodPost, "/devices", map[string]any{"device_id": devB, "name": "Verify Sterilizer B"})
	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devB+"/windows", map[string]any{})
	wb, werr := decodeWindow(body)
	c.check("register window B -> open", err == nil && status == http.StatusCreated && werr == nil && wb.State == "open",
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))
	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devB+"/windows", map[string]any{})
	c.check("duplicate register -> 409 WINDOW_ALREADY_OPEN",
		err == nil && status == http.StatusConflict && decodeErrCode(body) == "WINDOW_ALREADY_OPEN",
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	// 场景 3：客户端不得指定截止时刻 → 400，且不产生任何窗口。
	c.doJSON(http.MethodPost, "/devices", map[string]any{"device_id": devC, "name": "Verify Sterilizer C"})
	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devC+"/windows",
		map[string]any{"deadline": "2030-01-01T00:00:00.000Z"})
	c.check("client-specified deadline rejected with 400 INVALID_REQUEST",
		err == nil && status == http.StatusBadRequest && decodeErrCode(body) == "INVALID_REQUEST",
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))
	status, body, err = c.doJSON(http.MethodGet, "/devices/"+devC+"/status", nil)
	nw, serr := decodeStatus(body)
	c.check("no window created for device C",
		err == nil && status == http.StatusOK && serr == nil && nw == nil,
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	// 场景 4：无人确认 → worker 在截止时刻后把窗口隔离为 quarantined。
	var finalB *window
	dead := time.Now().Add(45 * time.Second)
	for time.Now().Before(dead) {
		status, body, err = c.doJSON(http.MethodGet, "/devices/"+devB+"/status", nil)
		if err == nil && status == http.StatusOK {
			if w, serr := decodeStatus(body); serr == nil && w != nil && w.State == "quarantined" {
				finalB = w
				break
			}
		}
		time.Sleep(time.Second)
	}
	c.check("window B quarantined by worker after deadline", finalB != nil,
		"state did not become quarantined within 45s")
	if finalB != nil {
		c.check("close reason is scan_timeout",
			finalB.CloseReason != nil && *finalB.CloseReason == "scan_timeout",
			fmt.Sprintf("close_reason=%v", finalB.CloseReason))
		c.check("closed_at at or after deadline",
			finalB.ClosedAt != nil && *finalB.ClosedAt >= finalB.Deadline,
			fmt.Sprintf("closed_at=%v deadline=%q", finalB.ClosedAt, finalB.Deadline))
	}

	// 场景 5：终态之后再次确认 → 409 NO_OPEN_WINDOW。
	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devB+"/confirm", map[string]any{})
	c.check("confirm after quarantine -> 409 NO_OPEN_WINDOW",
		err == nil && status == http.StatusConflict && decodeErrCode(body) == "NO_OPEN_WINDOW",
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	// 场景 6：设备策略默认 30 秒（含无窗口设备的状态查询）。
	devD := "VERIFY-D-" + runID
	status, body, err = c.doJSON(http.MethodPost, "/devices",
		map[string]any{"device_id": devD, "name": "Verify Sterilizer D"})
	c.check("create device D", err == nil && status == http.StatusCreated,
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))
	status, body, err = c.doJSON(http.MethodGet, "/devices/"+devD+"/status", nil)
	devD0, nw0, serr := decodeStatusFull(body)
	c.check("default policy is 30s and no window yet",
		err == nil && status == http.StatusOK && serr == nil && devD0.UnloadTTLSeconds == 30 && nw0 == nil,
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	// 场景 7：非法策略值 → 结构化 400 INVALID_REQUEST；未知设备 → 原有 404。
	for _, bad := range []string{
		`{"unload_ttl_seconds":0}`,
		`{"unload_ttl_seconds":601}`,
		`{"unload_ttl_seconds":2.5}`,
		`{"unload_ttl_seconds":"3"}`,
		`{"unload_ttl_seconds":true}`,
		`{}`,
		`{"unload_ttl_seconds":3,"extra":1}`,
	} {
		s, b, e := c.doJSON(http.MethodPut, "/devices/"+devD+"/policy", json.RawMessage(bad))
		details := decodeErrDetails(b)
		c.check("reject policy "+bad+" -> 400 INVALID_REQUEST with details",
			e == nil && s == http.StatusBadRequest && decodeErrCode(b) == "INVALID_REQUEST" &&
				details["field"] == "unload_ttl_seconds" && details["min"] == float64(1) && details["max"] == float64(600),
			fmt.Sprintf("payload=%s status=%d err=%v body=%s", bad, s, e, b))
	}
	s, b, e := c.doJSON(http.MethodPut, "/devices/NOPE-"+runID+"/policy", map[string]any{"unload_ttl_seconds": 3})
	c.check("policy update on unknown device -> 404 DEVICE_NOT_FOUND",
		e == nil && s == http.StatusNotFound && decodeErrCode(b) == "DEVICE_NOT_FOUND",
		fmt.Sprintf("status=%d err=%v body=%s", s, e, b))

	// 场景 8：把 D 的策略改为 3 秒并登记窗口：新窗口必须采用 3 秒并快照。
	s, b, e = c.doJSON(http.MethodPut, "/devices/"+devD+"/policy", map[string]any{"unload_ttl_seconds": 3})
	devD3, derr := decodeDevice(b)
	c.check("update policy D to 3s -> 200",
		e == nil && s == http.StatusOK && derr == nil && devD3.UnloadTTLSeconds == 3,
		fmt.Sprintf("status=%d err=%v body=%s", s, e, b))
	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devD+"/windows", map[string]any{})
	wd, werr := decodeWindow(body)
	c.check("register window D -> open with 3s snapshot",
		err == nil && status == http.StatusCreated && werr == nil && wd.State == "open" && wd.TTLSeconds == 3,
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))
	od, _ := time.Parse(time.RFC3339Nano, wd.OpenedAt)
	dl, _ := time.Parse(time.RFC3339Nano, wd.Deadline)
	c.check("window D deadline = opened_at + 3s",
		dl.Sub(od) == 3*time.Second,
		fmt.Sprintf("opened_at=%q deadline=%q ttl=%d", wd.OpenedAt, wd.Deadline, wd.TTLSeconds))

	// 场景 9：活动窗口期间再次改策（3 -> 45）：当前策略变化，但活动窗口的
	// 快照秒数与截止时刻保持 3 秒不变。
	s, b, e = c.doJSON(http.MethodPut, "/devices/"+devD+"/policy", map[string]any{"unload_ttl_seconds": 45})
	c.check("update policy D to 45s while window open -> 200",
		e == nil && s == http.StatusOK && decodeErrCode(b) == "",
		fmt.Sprintf("status=%d err=%v body=%s", s, e, b))
	status, body, err = c.doJSON(http.MethodGet, "/devices/"+devD+"/status", nil)
	devNow, wOpen, serr := decodeStatusFull(body)
	c.check("status explains deadline source: policy 45s, active window still 3s",
		err == nil && status == http.StatusOK && serr == nil &&
			devNow.UnloadTTLSeconds == 45 && wOpen != nil &&
			wOpen.ID == wd.ID && wOpen.TTLSeconds == 3 && wOpen.Deadline == wd.Deadline && wOpen.State == "open",
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	// 3 秒窗口到期后由 worker 隔离；终态窗口仍保留 3 秒快照（截止时刻来源可解释）。
	var finalD *window
	deadD := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadD) {
		s, b, e = c.doJSON(http.MethodGet, "/devices/"+devD+"/status", nil)
		if e == nil && s == http.StatusOK {
			if _, w, _ := decodeStatusFull(b); w != nil && w.State == "quarantined" {
				finalD = w
				break
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	c.check("3s window D quarantined by worker; snapshot stays 3s with scan_timeout",
		finalD != nil && finalD.TTLSeconds == 3 && finalD.Deadline == wd.Deadline &&
			finalD.CloseReason != nil && *finalD.CloseReason == "scan_timeout",
		fmt.Sprintf("final=%+v", finalD))

	// 场景 10：改策后的再登记窗口采用当前 45 秒策略，及时确认仍为 confirmed，
	// 回归确认/超时裁决与 open 前置条件。
	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devD+"/windows", map[string]any{})
	wd2, werr := decodeWindow(body)
	c.check("new window D uses current 45s policy snapshot",
		err == nil && status == http.StatusCreated && werr == nil && wd2.TTLSeconds == 45,
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))
	o2, _ := time.Parse(time.RFC3339Nano, wd2.OpenedAt)
	d2, _ := time.Parse(time.RFC3339Nano, wd2.Deadline)
	c.check("window D2 deadline = opened_at + 45s", d2.Sub(o2) == 45*time.Second,
		fmt.Sprintf("opened_at=%q deadline=%q", wd2.OpenedAt, wd2.Deadline))
	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devD+"/confirm", map[string]any{})
	cw2, werr := decodeWindow(body)
	c.check("prompt confirm of 45s window -> confirmed",
		err == nil && status == http.StatusOK && werr == nil && cw2.State == "confirmed" && cw2.TTLSeconds == 45,
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))
	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devD+"/confirm", map[string]any{})
	c.check("repeat confirm after close -> 409 NO_OPEN_WINDOW (single terminal state)",
		err == nil && status == http.StatusConflict && decodeErrCode(body) == "NO_OPEN_WINDOW",
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	if c.failed > 0 {
		fmt.Printf("VERIFY: FAIL (%d check(s) failed)\n", c.failed)
		os.Exit(1)
	}
	fmt.Println("VERIFY: PASS")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
