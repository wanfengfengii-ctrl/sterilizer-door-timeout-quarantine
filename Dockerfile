# syntax=docker/dockerfile:1

# 构建阶段：编译 api / worker / verify 三个静态二进制（modernc.org/sqlite 为纯 Go 实现，无需 CGO）。
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/verify ./cmd/verify

# 运行阶段：单一镜像承载三个命令，由 docker-compose 的 command 区分角色。
FROM alpine:3.21
RUN adduser -D -u 10001 app && mkdir -p /data && chown app:app /data
COPY --from=build /out/api /app/api
COPY --from=build /out/worker /app/worker
COPY --from=build /out/verify /app/verify
USER app
EXPOSE 8080
CMD ["/app/api"]
