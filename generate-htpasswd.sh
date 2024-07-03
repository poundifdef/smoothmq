#!/bin/bash

# Create logs directory if it doesn't exist
mkdir -p /logs

# Use dotenv to load environment variables and execute the commands
dotenv -e /etc/.env -- bash -c '
    echo "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" > /logs/htpasswd.log
    htpasswd -bc /etc/nginx/.htpasswd "user" "$AWS_SECRET_ACCESS_KEY"
'
