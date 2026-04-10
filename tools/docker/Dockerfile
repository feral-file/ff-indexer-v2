# ff-indexer image.
#
# Build args:
#   CGO_ENABLED=1 — media Temporal worker (libvips, resvg, chromium); see Docker-compose build args.
#   CGO_ENABLED=0 — smaller image; media worker stub (see cmd/ff-indexer/worker_media_stub.go).
#
# Stages:
#   1. builder — compile static or CGO-linked binary from repo root.
#   2. runtime — minimal Alpine + certs (+ vips/chromium when CGO=1).

# syntax=docker/dockerfile:1.4

FROM golang:1.25-alpine AS builder

ARG CGO_ENABLED=0
ARG ALPINE_VERSION=edge

WORKDIR /build

# Step: CGO build deps (vips for govips / libvips bindings; use edge for vips 8.18+)
RUN if [ "$CGO_ENABLED" = "1" ]; then \
    # First update all system packages to edge
    apk upgrade --no-cache \
        --repository=https://dl-cdn.alpinelinux.org/alpine/edge/main \
        --repository=https://dl-cdn.alpinelinux.org/alpine/edge/community ; \
    # Then install build dependencies
    apk add --no-cache \
        --repository=https://dl-cdn.alpinelinux.org/alpine/edge/main \
        --repository=https://dl-cdn.alpinelinux.org/alpine/edge/community \
        build-base \
        musl-dev \
        pkgconfig \
        vips-dev ; \
    fi

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# resvg static lib + headers: only needed when linking the CGO media worker (skipped for CGO_ENABLED=0).
RUN --mount=type=bind,from=feralfile/resvg-builder:v0.45.1,source=/usr/local/lib/libresvg.a,target=/mnt/libresvg.a,readonly \
    --mount=type=bind,from=feralfile/resvg-builder:v0.45.1,source=/usr/local/include/resvg.h,target=/mnt/resvg.h,readonly \
    if [ "$CGO_ENABLED" = "1" ]; then \
      mkdir -p /usr/local/lib /usr/local/include && \
      cp /mnt/libresvg.a /usr/local/lib/libresvg.a && \
      cp /mnt/resvg.h /usr/local/include/resvg.h ; \
    fi

# Step: compile ff-indexer (CGO or pure Go).
# CGO_LDFLAGS/CGO_CFLAGS apply only when linking CGO code; setting them globally can confuse tooling when CGO_ENABLED=0.
RUN if [ "$CGO_ENABLED" = "1" ]; then \
      export CGO_ENABLED=1 && \
      export CGO_LDFLAGS="-L/usr/local/lib" && \
      export CGO_CFLAGS="-I/usr/local/include" && \
      go build -ldflags="-w -s" -o /build/bin/ff-indexer ./cmd/ff-indexer ; \
    else \
      CGO_ENABLED=0 go build -ldflags="-w -s" -o /build/bin/ff-indexer ./cmd/ff-indexer ; \
    fi

FROM alpine:edge

ARG CGO_ENABLED=0
ARG ALPINE_VERSION=edge

# Step: runtime libraries (full media stack vs minimal)
RUN if [ "$CGO_ENABLED" = "1" ]; then \
      apk add --no-cache \
          --repository=https://dl-cdn.alpinelinux.org/alpine/edge/main \
          --repository=https://dl-cdn.alpinelinux.org/alpine/edge/community \
          ca-certificates \
          libgcc \
          vips \
          chromium \
          chromium-chromedriver \
          font-noto ; \
    else \
      apk add --no-cache ca-certificates libgcc ; \
    fi

# Chromium is on PATH when the CGO media stack is installed; avoid CHROME_* pointing at missing paths on stub images.

RUN addgroup -g 1000 appuser && \
    adduser -D -u 1000 -G appuser appuser

WORKDIR /app

# Step: ship binary and default layout
COPY --from=builder /build/bin/ff-indexer .

RUN mkdir -p tools/registry && chown -R appuser:appuser /app

USER appuser

EXPOSE 8081

CMD ["./ff-indexer", "-config", "config.yaml"]
