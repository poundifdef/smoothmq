#!/bin/sh

# Create nginx user and group
# adduser --system --no-create-home --shell /bin/false --group --disabled-login nginx

# start nginx in the background
#echo "running nginx"
#nginx &

echo "running sqs emulator"
run-app
