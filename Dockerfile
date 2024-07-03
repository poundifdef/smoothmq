ARG GO_VERSION=1.22
FROM golang:${GO_VERSION}-bookworm as builder

WORKDIR /usr/src/app
COPY sqs/go.mod sqs/go.sum ./
RUN go mod download && go mod verify



# Copy and compile individual packages
COPY sqs/models models
RUN go build -v ./models

COPY sqs/protocols protocols
RUN go build -v ./protocols/sqs

COPY sqs/queue queue
RUN go build -v ./queue/sqlite

COPY sqs/tenants tenants
RUN go build -v ./tenants/defaultmanager

COPY sqs/dashboard dashboard
RUN go build -v ./dashboard

COPY sqs/cmd cmd
RUN go build -v ./cmd/...

# Copy remaining files and build the main application
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
    curl \
    npm && \
    npm install -g dotenv-cli && \
    apt-get clean

COPY --from=builder /run-app /usr/local/bin/

COPY nginx.conf /etc/nginx/nginx.conf
COPY entrypoint.sh /entrypoint.sh
RUN dos2unix /entrypoint.sh

COPY .env /etc/.env

# Check if AWS_SECRET_ACCESS_KEY argument is provided and not default, then add to /etc/.env
ARG AWS_SECRET_ACCESS_KEY
RUN if [ -n "$AWS_SECRET_ACCESS_KEY" ] && [ "$AWS_SECRET_ACCESS_KEY" != "default" ]; then \
    echo "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" >> /etc/.env; \
    fi

# Copy the generate-htpasswd.sh script
COPY generate-htpasswd.sh /generate-htpasswd.sh
RUN chmod +x /generate-htpasswd.sh
RUN dos2unix /generate-htpasswd.sh
RUN /generate-htpasswd.sh

# Run the generate-htpasswd.sh script at container start

EXPOSE 80

# For some reason we need to expose these ports our it won't work in docker.
EXPOSE 3000
EXPOSE 3001

ENV PORT=80


CMD ["/bin/sh", "/entrypoint.sh"]
