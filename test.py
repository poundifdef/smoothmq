import boto3
import time

# Configure the SQS client
sqs = boto3.client("sqs", 
                   region_name="us-east-1",
                   aws_access_key_id="YOUR_ACCESS_KEY_ID",
                   aws_secret_access_key="YOUR_SECRET_ACCESS_KEY2",
                   endpoint_url="http://localhost")

# Check if the queue already exists
queue_name = "my-test-que-for-testing"
response = sqs.list_queues(QueueNamePrefix=queue_name)
if 'QueueUrls' in response and len(response['QueueUrls']) > 0:
    queue_url = response['QueueUrls'][0]
    print(f"Queue already exists: {queue_url}")
    queue_created = False
else:
    # Create the queue if it doesn't exist
    response = sqs.create_queue(QueueName=queue_name)
    queue_url = response['QueueUrl']
    print(f"Created queue: {queue_url}")
    queue_created = True

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

