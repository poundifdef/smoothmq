#!/bin/sh

# Create nginx user and group
adduser --system --no-create-home --shell /bin/false --group --disabled-login nginx

start_nginx() {
    while true; do
        if ! pgrep -x "nginx" > /dev/null
        then
            echo "Starting nginx"
            nginx
        fi
        sleep 1
    done
}

# Start nginx in the background
start_nginx &

echo "running sqs emulator"
run-app
