ARG GO_VERSION=1.22
FROM golang:${GO_VERSION}-bookworm as builder

WORKDIR /usr/src/app
COPY sqs/go.mod sqs/go.sum ./
RUN go mod download && go mod verify
COPY sqs/cmd cmd
COPY sqs/dashboard dashboard
COPY sqs/models models
COPY sqs/protocols protocols
COPY sqs/queue queue
COPY sqs/tenants tenants
COPY sqs/*.go ./
RUN go build -v -o /run-app .


FROM debian:bookworm

# install nginx, apache2-utils (for htpasswd), pgrep, and bash
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
    nginx \
    dos2unix \
    apache2-utils \
    procps \
    bash \
    sudo \
    curl && \
    apt-get clean

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
RUN bash /generate-htpasswd.sh

EXPOSE 80

# For some reason we need to expose these ports our it won't work in docker.
EXPOSE 3000
EXPOSE 3001

ENV PORT=80

CMD ["/bin/sh", "/entrypoint.sh"]
