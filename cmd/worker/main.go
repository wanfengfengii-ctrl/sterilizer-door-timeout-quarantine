// worker 是独立的扫描进程：周期性地把数据库 UTC 当前时间等于或晚于
// 截止时刻的 open 卸载窗口原子地写入 quarantined。它与 API 进程共享
// 同一个 SQLite 数据库文件，数据库是两进程的唯一裁决依据。
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cssd-unload/internal/store"
)

func main() {
	dbPath := envOr("DATABASE_PATH", "cssd.db")
	interval := envDuration("SCAN_INTERVAL", time.Second)

	st, err := store.Open(context.Background(), dbPath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("worker scanning every %s (database %s)", interval, dbPath)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		n, err := st.QuarantineExpired(ctx)
		if err != nil {
			log.Printf("scan failed: %v", err)
		} else if n > 0 {
			log.Printf("quarantined %d expired window(s)", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
		log.Printf("invalid %s %q, using %s", key, v, fallback)
	}
	return fallback
}
