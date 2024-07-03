# jobs.kumquat.live

SmoothMQ is a drop-in replacement for SQS with a much smoother developer experience.
It has a functional UI, observability, tracing, message scheduling, and rate-limiting.
SmoothMQ lets you run a private SQS instance on any cloud.

<!-- A drop-in replacement for SQS designed for great developer experience and efficiency. -->

## Login Credentials

/ui will go to the ui of the service.
  * user: "user"
  * pass: (see .env file for password)

Note that the AWS_SECRET_ACCESS_KEY you should use in the api is located in the `.env` file.

It will be the same as the password used to access the /ui page.


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