// Package api 提供面向消毒供应中心设备集成的 HTTP 接口：
// 登记卸载窗口、接收开门确认、查询设备/窗口状态。
// 所有状态裁决都下沉到 store 层的 SQLite 原子语句，本层只做
// 参数校验、错误映射与 JSON 序列化。
package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"cssd-unload/internal/store"
)

// 结构化错误码，随错误响应的 error.code 返回。
const (
	CodeInvalidRequest    = "INVALID_REQUEST"
	CodeDeviceNotFound    = "DEVICE_NOT_FOUND"
	CodeDeviceExists      = "DEVICE_ALREADY_EXISTS"
	CodeWindowAlreadyOpen = "WINDOW_ALREADY_OPEN"
	CodeNoOpenWindow      = "NO_OPEN_WINDOW"
	CodeInternal          = "INTERNAL_ERROR"
)

// Server 持有路由与存储依赖。
type Server struct {
	st     *store.Store
	router *gin.Engine
}

// NewServer 构造 HTTP 服务并注册全部路由。
func NewServer(st *store.Store) *Server {
	r := gin.New()
	r.Use(gin.Recovery())

	s := &Server{st: st, router: r}
	r.GET("/health", s.health)
	r.POST("/devices", s.createDevice)
	r.GET("/devices/:deviceID/status", s.deviceStatus)
	r.PUT("/devices/:deviceID/policy", s.updateDevicePolicy)
	r.POST("/devices/:deviceID/windows", s.registerWindow)
	r.POST("/devices/:deviceID/confirm", s.confirmWindow)
	return s
}

// Handler 返回根 http.Handler，便于挂载到 http.Server 或 httptest。
func (s *Server) Handler() http.Handler { return s.router }

type errorBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func writeError(c *gin.Context, status int, code, message string, details map[string]any) {
	c.JSON(status, gin.H{"error": errorBody{Code: code, Message: message, Details: details}})
}

func (s *Server) health(c *gin.Context) {
	if err := s.st.Ping(c.Request.Context()); err != nil {
		writeError(c, http.StatusServiceUnavailable, CodeInternal, "database unreachable", nil)
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

type createDeviceRequest struct {
	DeviceID string `json:"device_id" binding:"required"`
	Name     string `json:"name" binding:"required"`
}

func (s *Server) createDevice(c *gin.Context) {
	var req createDeviceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, http.StatusBadRequest, CodeInvalidRequest,
			"request body must be a JSON object with non-empty device_id and name", nil)
		return
	}
	req.DeviceID = strings.TrimSpace(req.DeviceID)
	req.Name = strings.TrimSpace(req.Name)
	if req.DeviceID == "" || req.Name == "" || len(req.DeviceID) > 128 || len(req.Name) > 256 {
		writeError(c, http.StatusBadRequest, CodeInvalidRequest,
			"device_id and name must be non-empty (device_id <= 128 chars, name <= 256 chars)", nil)
		return
	}

	d, err := s.st.CreateDevice(c.Request.Context(), req.DeviceID, req.Name)
	switch {
	case errors.Is(err, store.ErrDeviceExists):
		writeError(c, http.StatusConflict, CodeDeviceExists,
			fmt.Sprintf("device %q already exists", req.DeviceID), nil)
	case err != nil:
		writeError(c, http.StatusInternalServerError, CodeInternal, "failed to create device", nil)
	default:
		c.JSON(http.StatusCreated, gin.H{"device": d})
	}
}

// policyField 是策略更新接口接受的唯一字段；使用 json.RawMessage 保留原始
// JSON token，以便严格区分整数数字与字符串/布尔/null 等非数字写法。
type updatePolicyRequest struct {
	UnloadTTLSeconds json.RawMessage `json:"unload_ttl_seconds"`
}

func policyErrorDetails() map[string]any {
	return map[string]any{
		"field": "unload_ttl_seconds",
		"min":   store.MinWindowTTLSeconds,
		"max":   store.MaxWindowTTLSeconds,
	}
}

// updateDevicePolicy 处理 PUT /devices/:deviceID/policy：集成方提交 1..600
// 的整数秒开门确认时限。校验失败一律返回 400 INVALID_REQUEST 的结构化错误；
// 设备不存在返回原有的 404 DEVICE_NOT_FOUND。更新是单条原子 UPDATE，不会
// 产生半写入配置，也不影响设备既有窗口。
func (s *Server) updateDevicePolicy(c *gin.Context) {
	deviceID := c.Param("deviceID")

	raw, err := io.ReadAll(io.LimitReader(c.Request.Body, 1<<20))
	if err != nil {
		writeError(c, http.StatusBadRequest, CodeInvalidRequest, "failed to read request body", policyErrorDetails())
		return
	}
	var req updatePolicyRequest
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(c, http.StatusBadRequest, CodeInvalidRequest,
			"request body must be a JSON object with exactly one integer field \"unload_ttl_seconds\"",
			policyErrorDetails())
		return
	}
	// 拒绝第二个 JSON 值（如尾随文档/垃圾字符）。
	var extra json.RawMessage
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		writeError(c, http.StatusBadRequest, CodeInvalidRequest,
			"request body must contain a single JSON object", policyErrorDetails())
		return
	}
	if len(req.UnloadTTLSeconds) == 0 {
		writeError(c, http.StatusBadRequest, CodeInvalidRequest,
			"missing required integer field \"unload_ttl_seconds\"", policyErrorDetails())
		return
	}
	// 仅接受整数数字字面量：先看原始 token 的首字符剔除字符串/布尔/null/对象，
	// 再由 Atoi 拒绝小数、指数、符号与溢出（30.0、1e2、"30"、true 均被拒绝）。
	token := strings.TrimSpace(string(req.UnloadTTLSeconds))
	if token == "" || (token[0] != '-' && (token[0] < '0' || token[0] > '9')) {
		writeError(c, http.StatusBadRequest, CodeInvalidRequest,
			fmt.Sprintf("unload_ttl_seconds must be an integer between %d and %d, got %s",
				store.MinWindowTTLSeconds, store.MaxWindowTTLSeconds, token),
			policyErrorDetails())
		return
	}
	ttl, err := strconv.Atoi(token)
	if err != nil {
		writeError(c, http.StatusBadRequest, CodeInvalidRequest,
			fmt.Sprintf("unload_ttl_seconds must be an integer between %d and %d, got %s",
				store.MinWindowTTLSeconds, store.MaxWindowTTLSeconds, token),
			policyErrorDetails())
		return
	}
	if ttl < store.MinWindowTTLSeconds || ttl > store.MaxWindowTTLSeconds {
		writeError(c, http.StatusBadRequest, CodeInvalidRequest,
			fmt.Sprintf("unload_ttl_seconds must be between %d and %d seconds, got %d",
				store.MinWindowTTLSeconds, store.MaxWindowTTLSeconds, ttl),
			policyErrorDetails())
		return
	}

	d, err := s.st.UpdateDevicePolicy(c.Request.Context(), deviceID, ttl)
	switch {
	case errors.Is(err, store.ErrDeviceNotFound):
		writeError(c, http.StatusNotFound, CodeDeviceNotFound,
			fmt.Sprintf("device %q not found", deviceID), nil)
	case errors.Is(err, store.ErrInvalidPolicy):
		// 存储层 CHECK 的防线，正常情况下 HTTP 层校验已拦截。
		writeError(c, http.StatusBadRequest, CodeInvalidRequest,
			fmt.Sprintf("unload_ttl_seconds must be between %d and %d seconds",
				store.MinWindowTTLSeconds, store.MaxWindowTTLSeconds),
			policyErrorDetails())
	case err != nil:
		writeError(c, http.StatusInternalServerError, CodeInternal, "failed to update device policy", nil)
	default:
		c.JSON(http.StatusOK, gin.H{"device": d})
	}
}

// rejectClientFields 解析请求体并拒绝任何客户端字段。登记与确认接口不接受
// 客户端参数：截止时刻、状态、时间戳一律由数据库生成，客户端指定即视为
// 非法请求。空请求体或空 JSON 对象 {} 均可接受。
func rejectClientFields(c *gin.Context) error {
	if c.Request.Body == nil {
		return nil
	}
	dec := json.NewDecoder(c.Request.Body)
	var payload map[string]any
	if err := dec.Decode(&payload); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("request body must be an empty JSON object: %v", err)
	}
	if len(payload) == 0 {
		return nil
	}
	return fmt.Errorf("unexpected field(s) %s: deadline, state and timestamps are always assigned by the database clock and cannot be set by clients",
		strings.Join(slices.Sorted(maps.Keys(payload)), ", "))
}

func (s *Server) registerWindow(c *gin.Context) {
	deviceID := c.Param("deviceID")
	if err := rejectClientFields(c); err != nil {
		writeError(c, http.StatusBadRequest, CodeInvalidRequest, err.Error(), nil)
		return
	}

	w, err := s.st.RegisterWindow(c.Request.Context(), deviceID)
	switch {
	case errors.Is(err, store.ErrDeviceNotFound):
		writeError(c, http.StatusNotFound, CodeDeviceNotFound,
			fmt.Sprintf("device %q not found", deviceID), nil)
	case errors.Is(err, store.ErrWindowAlreadyOpen):
		details := map[string]any{}
		if cur, lerr := s.st.LatestWindow(c.Request.Context(), deviceID); lerr == nil && cur.State == store.StateOpen {
			details["open_window_id"] = cur.ID
			details["deadline"] = cur.Deadline
		}
		writeError(c, http.StatusConflict, CodeWindowAlreadyOpen,
			fmt.Sprintf("device %q already has an open unload window", deviceID), details)
	case err != nil:
		writeError(c, http.StatusInternalServerError, CodeInternal, "failed to register unload window", nil)
	default:
		c.JSON(http.StatusCreated, gin.H{"window": w})
	}
}

func (s *Server) confirmWindow(c *gin.Context) {
	deviceID := c.Param("deviceID")
	if err := rejectClientFields(c); err != nil {
		writeError(c, http.StatusBadRequest, CodeInvalidRequest, err.Error(), nil)
		return
	}

	w, err := s.st.ConfirmWindow(c.Request.Context(), deviceID)
	switch {
	case errors.Is(err, store.ErrDeviceNotFound):
		writeError(c, http.StatusNotFound, CodeDeviceNotFound,
			fmt.Sprintf("device %q not found", deviceID), nil)
	case errors.Is(err, store.ErrNoOpenWindow):
		details := map[string]any{}
		if cur, lerr := s.st.LatestWindow(c.Request.Context(), deviceID); lerr == nil {
			details["latest_window_id"] = cur.ID
			details["latest_state"] = cur.State
		}
		writeError(c, http.StatusConflict, CodeNoOpenWindow,
			fmt.Sprintf("device %q has no open unload window; it may already be confirmed or quarantined", deviceID),
			details)
	case err != nil:
		writeError(c, http.StatusInternalServerError, CodeInternal, "failed to confirm unload window", nil)
	default:
		c.JSON(http.StatusOK, gin.H{"window": w})
	}
}

func (s *Server) deviceStatus(c *gin.Context) {
	deviceID := c.Param("deviceID")
	d, err := s.st.GetDevice(c.Request.Context(), deviceID)
	switch {
	case errors.Is(err, store.ErrDeviceNotFound):
		writeError(c, http.StatusNotFound, CodeDeviceNotFound,
			fmt.Sprintf("device %q not found", deviceID), nil)
		return
	case err != nil:
		writeError(c, http.StatusInternalServerError, CodeInternal, "failed to query device", nil)
		return
	}

	w, err := s.st.LatestWindow(c.Request.Context(), deviceID)
	switch {
	case errors.Is(err, store.ErrWindowNotFound):
		c.JSON(http.StatusOK, gin.H{"device": d, "window": nil})
	case err != nil:
		writeError(c, http.StatusInternalServerError, CodeInternal, "failed to query window", nil)
	default:
		c.JSON(http.StatusOK, gin.H{"device": d, "window": w})
	}
}
