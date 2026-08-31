# Overture — generic (any registry / any container platform)
FROM golang:1.24-bookworm AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Build server binary
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=$(git rev-parse --short HEAD 2>/dev/null || echo dev)" -o /out/overture ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=builder /out/overture /app/overture
COPY database/migrations /app/database/migrations
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/app/overture", "server"]
