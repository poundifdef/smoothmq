#!/bin/sh

# Create nginx user and group
adduser --system --no-create-home --shell /bin/false --group --disabled-login nginx

# Start nginx in the background after a 10-second delay
(sleep 10 && echo "running nginx" && nginx) &

echo "running sqs emulator"
run-app
