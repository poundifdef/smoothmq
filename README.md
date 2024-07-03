# SmoothMQ

SmoothMQ is a drop-in replacement for SQS with a much smoother developer experience.
It has a functional UI, observability, tracing, message scheduling, and rate-limiting.
SmoothMQ lets you run a private SQS instance on any cloud.

<!-- A drop-in replacement for SQS designed for great developer experience and efficiency. -->

## Login Credentials

The environmental variables `USER` and `PASS` are used to store http credentials.

The dashboard is located at `/` which forwards internally to port 3000 and the api is located at `/api` which forwards to port 3001.

## Quick Start

Clone the repo and then invoke:

```bash
docker compose up
```

When you are finished, ctrl-c and then `docker compose down`.

## Getting Started

SmoothMQ deploys as a single go binary and can be used by any existing SQS client.

## Running

This will run a UI on `:3000` and an SQS-compatible server on `:3001`.

```
$ go run .
```

## Connecting

This works with any SQS client in any language.

### Python

``` py
import boto3

# Simply change the endpoint_url
sqs = boto3.client("sqs", ..., endpoint_url="http://localhost:3001")
sqs.send_message(QueueUrl="...", MessageBody="hello world")
```

Celery works seamlessly:

``` py
app = Celery("tasks", broker_url="sqs://...@localhost:3001")
```

## UI

The UI lets you manage queues and search individual messages.

![Dashboard UI](docs/queue.gif)


## Other links

  * https://hub.docker.com/r/roribio16/alpine-sqs/