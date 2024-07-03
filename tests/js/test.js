const AWS = require('aws-sdk');
const dotenv = require('dotenv');
const { promisify } = require('util');

dotenv.config();

const sleep = promisify(setTimeout);

async function createOrGetQueue(sqs, queueName) {
    try {
        const response = await sqs.listQueues({ QueueNamePrefix: queueName }).promise();
        if (response.QueueUrls && response.QueueUrls.length > 0) {
            const queueUrl = response.QueueUrls[0];
            console.log(`Queue already exists: ${queueUrl}`);
            return [queueUrl, false];
        } else {
            const response = await sqs.createQueue({ QueueName: queueName }).promise();
            const queueUrl = response.QueueUrl;
            console.log(`Created queue: ${queueUrl}`);
            return [queueUrl, true];
        }
    } catch (error) {
        console.error('Error in createOrGetQueue:', error);
        throw error;
    }
}

async function runSqsTest(endpointUrl, awsSecretAccessKey) {
    const sqs = new AWS.SQS({
        region: 'us-east-1',
        accessKeyId: 'YOUR_ACCESS_KEY_ID',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
        endpoint: new AWS.Endpoint(endpointUrl)
    });

    const queueName = 'my-test-que-for-testing';
    let queueUrl, queueCreated;

    try {
        [queueUrl, queueCreated] = await createOrGetQueue(sqs, queueName);
        console.log(`Queue URL: ${queueUrl}`);

        await sqs.sendMessage({ QueueUrl: queueUrl, MessageBody: 'hello world' }).promise();
        console.log('Sent a message to the queue');

        const receiveResponse = await sqs.receiveMessage({ QueueUrl: queueUrl, MaxNumberOfMessages: 1 }).promise();
        if (receiveResponse.Messages && receiveResponse.Messages.length > 0) {
            const message = receiveResponse.Messages[0];
            console.log(`Received message: ${message.Body}`);

            await sqs.deleteMessage({ QueueUrl: queueUrl, ReceiptHandle: message.ReceiptHandle }).promise();
            console.log('Deleted the message');
        } else {
            console.log('No messages in the queue');
        }

        await sleep(2000);
    } catch (error) {
        console.error('Error in runSqsTest:', error);
    } finally {
        if (queueUrl) {
            console.log(`Destroying queue: ${queueUrl}`);
            await sqs.deleteQueue({ QueueUrl: queueUrl }).promise();
            console.log(`Destroyed queue: ${queueUrl}`);
        }
    }
}

async function main() {
    const awsSecretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;
    const endpoints = ['http://localhost', 'https://jobs.kumquat.live'];

    for (const endpointUrl of endpoints) {
        console.log(`\nTesting with endpoint: ${endpointUrl}`);
        await runSqsTest(endpointUrl, awsSecretAccessKey);
    }
}

main().catch(error => console.error('Error in main:', error));
