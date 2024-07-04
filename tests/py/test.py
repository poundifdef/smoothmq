import boto3
import time
import os
import warnings
from dotenv import load_dotenv
from pathlib import Path
import requests

HERE = Path(__file__).parent
PROJECT_ROOT = HERE.parent.parent
ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(dotenv_path=ENV_FILE)

def create_or_get_queue(sqs, queue_name: str) -> tuple[str, bool]:
    response = sqs.list_queues(QueueNamePrefix=queue_name)
    if 'QueueUrls' in response and len(response['QueueUrls']) > 0:
        queue_url = response['QueueUrls'][0]
        print(f"Queue already exists: {queue_url}")
        return queue_url, False
    else:
        response = sqs.create_queue(QueueName=queue_name)
        queue_url = response['QueueUrl']
        print(f"Created queue: {queue_url}")
        return queue_url, True

def run_sqs_test(endpoint_url: str, aws_secret_acess_key: str) -> None:
    # Load environment variables from .env file

    params = {
        "region_name": "us-east-1",
        "aws_secret_access_key":'YOUR_ACCESS_KEY_ID',
        "aws_secret_access_key": aws_secret_acess_key,
        "endpoint_url": endpoint_url
    }

    print(f"Testing with parameters: {params}")


    # Configure the SQS client
    sqs = boto3.client("sqs", **params)

    # Create or get the queue
    queue_name = "my-test-que-for-testing"
    queue_url, queue_created = create_or_get_queue(sqs, queue_name)

    print(f"Queue URL: {queue_url}")

    try:
        # Perform operations
        sqs.send_message(QueueUrl=queue_url, MessageBody="hello world")
        print("Sent a message to the queue")

        # Receive and print the message
        response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
        if 'Messages' in response:
            message = response['Messages'][0]
            print(f"Received message: {message['Body']}")
            
            # Delete the message
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
            print("Deleted the message")
        else:
            print("No messages in the queue")

        # Wait a moment to ensure all operations are completed
        time.sleep(2)

    finally:
        print(f"Destroying queue: {queue_url}")
        sqs.delete_queue(QueueUrl=queue_url)
        print(f"Destroyed queue: {queue_url}")

def check_server_is_alive(endpoint_url: str) -> bool:
    try:
        response = requests.get(endpoint_url + "/healthz", timeout=2)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"\nError: {e}")
        print(f"\nCould not reach endpoint: {endpoint_url}")
    return False

def main() -> None:
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    endpoints = ["http://localhost", "https://jobs.kumquat.live"]
    for i, endpoint_url in enumerate(endpoints):
        is_alive = check_server_is_alive(endpoint_url)
        if not is_alive:
            warnings.warn(f"Endpoint is not alive: {endpoint_url}")
            if i == 0:
                # fatal error for localhost
                raise Exception("Localhost is not alive")
        print(f"\nTesting with endpoint: {endpoint_url}")
        run_sqs_test(endpoint_url=endpoint_url, aws_secret_acess_key=aws_secret_access_key)

if __name__ == "__main__":
    main()

