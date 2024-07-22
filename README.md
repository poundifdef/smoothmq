# SmoothMQ

SmoothMQ is a drop-in replacement for SQS with a much smoother developer experience.
It has a functional UI, observability, tracing, message scheduling, and rate-limiting.
SmoothMQ lets you run a private SQS instance on any cloud.

## Survey!
I'd love your feedback on the direction of this project! https://forms.gle/m5iMjcA5Xvp685Yw8


<!-- A drop-in replacement for SQS designed for great developer experience and efficiency. -->

## Getting Started

SmoothMQ deploys as a single go binary and can be used by any existing SQS client.

## Running

This will run a UI on `:3000` and an SQS-compatible server on `:3001`.

```
$ go run . server
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
