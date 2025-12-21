ARG GO_VERSION=1.22
FROM golang:${GO_VERSION}-bookworm as builder

WORKDIR /usr/src/app
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
RUN go build -v -o /run-app .


FROM debian:bookworm

COPY --from=builder /run-app /usr/local/bin/

# Default to binding to 0.0.0.0 so the service is reachable outside the container
ENV Q_SQS_HOST=0.0.0.0
ENV Q_DASHBOARD_HOST=0.0.0.0
ENV HOST=0.0.0.0
ENV Q_SERVER_USE_SINGLE_PORT=true

EXPOSE 8080
CMD ["run-app", "server"]
