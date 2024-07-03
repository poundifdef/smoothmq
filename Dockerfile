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

# install nginx and apache2-utils (for htpasswd)
RUN apt-get update && apt-get install -y nginx dos2unix apache2-utils && apt-get clean

COPY --from=builder /run-app /usr/local/bin/

COPY nginx.conf /etc/nginx/nginx.conf
COPY entrypoint.sh /entrypoint.sh
RUN dos2unix /entrypoint.sh

ARG USER=user
ARG PASS=pass
ENV USER=$USER
ENV PASS=$PASS

# Generate .htpasswd file using environment variables
RUN echo '#!/bin/bash' > /generate-htpasswd.sh && \
    echo 'htpasswd -bc /etc/nginx/.htpasswd "$USER" "$PASS"' >> /generate-htpasswd.sh && \
    chmod +x /generate-htpasswd.sh

# Run the generate-htpasswd.sh script
RUN /generate-htpasswd.sh

EXPOSE 3000
EXPOSE 3001

CMD ["/entrypoint.sh"]
