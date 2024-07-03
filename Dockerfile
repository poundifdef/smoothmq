ARG GO_VERSION=1.22
FROM golang:${GO_VERSION}-bookworm as builder

WORKDIR /usr/src/app
COPY go.mod go.sum ./
RUN go mod download && go mod verify
# COPY . .
COPY cmd cmd
COPY dashboard dashboard
COPY docs docs
COPY models models
COPY protocols protocols
COPY queue queue
COPY tenants tenants
COPY *.go ./
RUN go build -v -o /run-app .



FROM debian:bookworm

# install nginx
RUN apt-get update && apt-get install -y nginx dos2unix && apt-get clean

COPY --from=builder /run-app /usr/local/bin/

COPY nginx.conf /etc/nginx/nginx.conf
COPY entrypoint.sh /entrypoint.sh

RUN dos2unix /entrypoint.sh



EXPOSE 3000
EXPOSE 3001

CMD ["/entrypoint.sh"]
