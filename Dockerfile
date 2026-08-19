FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown

RUN set -eu; \
    resolved_commit="${COMMIT}"; \
    if [ -z "${resolved_commit}" ] || [ "${resolved_commit}" = "none" ] || [ "${resolved_commit}" = "unknown" ]; then \
      source_hash="$(find . -type f -print | LC_ALL=C sort | xargs sha256sum | sha256sum | cut -c1-12)"; \
      resolved_commit="source-${source_hash}"; \
    fi; \
    resolved_version="${VERSION}"; \
    if [ -z "${resolved_version}" ] || [ "${resolved_version}" = "dev" ]; then \
      resolved_version="dev-${resolved_commit}"; \
    fi; \
    resolved_build_date="${BUILD_DATE}"; \
    if [ -z "${resolved_build_date}" ] || [ "${resolved_build_date}" = "unknown" ]; then \
      resolved_build_date="$(date -u +%Y-%m-%dT%H:%M:%SZ)"; \
    fi; \
    CGO_ENABLED=0 GOOS=linux go build \
      -ldflags="-s -w -X main.Version=${resolved_version} -X main.Commit=${resolved_commit} -X main.BuildDate=${resolved_build_date}" \
      -o ./CLIProxyAPI ./cmd/server/

FROM alpine:3.22.0

RUN apk add --no-cache tzdata graphviz

RUN mkdir -p /CLIProxyAPI/data

COPY --from=builder ./app/CLIProxyAPI /CLIProxyAPI/CLIProxyAPI
COPY --from=builder /usr/local/go /usr/local/go

COPY config.example.yaml /CLIProxyAPI/config.example.yaml
COPY docker-entrypoint.sh /CLIProxyAPI/docker-entrypoint.sh
COPY third_party/fastschema-qjs /CLIProxyAPI/third_party/fastschema-qjs

WORKDIR /CLIProxyAPI

EXPOSE 8317

ENV TZ=Asia/Shanghai
ENV PATH="/usr/local/go/bin:${PATH}"

RUN cp /usr/share/zoneinfo/${TZ} /etc/localtime && echo "${TZ}" > /etc/timezone

RUN chmod +x /CLIProxyAPI/docker-entrypoint.sh

ENTRYPOINT ["/CLIProxyAPI/docker-entrypoint.sh"]
CMD ["./CLIProxyAPI"]
