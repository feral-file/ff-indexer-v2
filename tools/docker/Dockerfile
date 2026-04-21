# ff-indexer image.
#
# Build args:
#   CGO_ENABLED=1 — media Temporal worker (libvips, resvg, chromium); see Docker-compose build args.
#   CGO_ENABLED=0 — smaller image; media worker stub (see cmd/ff-indexer/worker_media_stub.go).
#
# Stages:
#   1. builder-base — shared Go module/download setup.
#   2. builder-0 — lightweight, pure-Go compile with no external media toolchain.
#   3. builder-1 — full CGO/media compile with libvips + resvg.
#   4. runtime — minimal Alpine + certs (+ vips/chromium when CGO=1).

# syntax=docker/dockerfile:1.4

ARG CGO_ENABLED=0

FROM golang:1.25-alpine AS builder-base

ARG ALPINE_VERSION=edge

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

FROM builder-base AS builder-0

RUN CGO_ENABLED=0 go build -ldflags="-w -s" -o /build/bin/ff-indexer ./cmd/ff-indexer

FROM feralfile/resvg-builder:v0.45.1 AS resvg-builder

FROM builder-base AS builder-1

ARG ALPINE_VERSION=edge

# Step: CGO build deps (vips for govips / libvips bindings; use edge for vips 8.18+)
RUN apk upgrade --no-cache \
      --repository=https://dl-cdn.alpinelinux.org/alpine/edge/main \
      --repository=https://dl-cdn.alpinelinux.org/alpine/edge/community && \
    apk add --no-cache \
      --repository=https://dl-cdn.alpinelinux.org/alpine/edge/main \
      --repository=https://dl-cdn.alpinelinux.org/alpine/edge/community \
      build-base \
      musl-dev \
      pkgconfig \
      vips-dev

# resvg static lib + headers: only needed when linking the CGO media worker.
COPY --from=resvg-builder /usr/local/lib/libresvg.a /usr/local/lib/libresvg.a
COPY --from=resvg-builder /usr/local/include/resvg.h /usr/local/include/resvg.h

# Step: compile ff-indexer with the full CGO media stack.
RUN export CGO_ENABLED=1 && \
    export CGO_LDFLAGS="-L/usr/local/lib" && \
    export CGO_CFLAGS="-I/usr/local/include" && \
    go build -ldflags="-w -s" -o /build/bin/ff-indexer ./cmd/ff-indexer

FROM builder-${CGO_ENABLED} AS builder

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
