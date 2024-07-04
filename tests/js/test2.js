const { SQSClient, ListQueuesCommand, CreateQueueCommand, SendMessageCommand, ReceiveMessageCommand, DeleteMessageCommand, DeleteQueueCommand } = require("@aws-sdk/client-sqs");
const dotenv = require('dotenv');
const path = require('path');
const semver = require('semver');

// Check AWS SDK version
const awsSdkPackage = require('@aws-sdk/client-sqs/package.json');
const currentVersion = awsSdkPackage.version;
const requiredVersion = '3.310.0';

if (semver.lt(currentVersion, requiredVersion)) {
    console.error(`Your @aws-sdk/client-sqs version (${currentVersion}) is outdated. Please update to at least version ${requiredVersion}.`);
    console.error('Run the following command to update:');
    console.error('npm install @aws-sdk/client-sqs@latest');
    process.exit(1);
}

// .env file is located in the root of the project
const envFile = path.join(__dirname, '../../.env');

// dotenv.config();
dotenv.config({ path: envFile });

// assert that the AWS_SECRET_ACCESS_KEY is set
if (!process.env.AWS_SECRET_ACCESS_KEY) {
    console.error('AWS_SECRET_ACCESS_KEY is not set');
    process.exit(1);
} else {
    // print out the contents of the env file
    const parsed = dotenv.parse(require('fs').readFileSync(envFile, 'utf-8'));
    console.log("\nContents of .env file:");
    Object.entries(parsed).forEach(([key, value]) => {
        console.log(`${key}=${value}`);
    });
    console.log("");
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function createOrGetQueue(sqs, queueName) {
    try {
        const listQueuesResponse = await sqs.send(new ListQueuesCommand({ QueueNamePrefix: queueName }));
        if (listQueuesResponse.QueueUrls && listQueuesResponse.QueueUrls.length > 0) {
            const queueUrl = listQueuesResponse.QueueUrls[0];
            console.log(`Queue already exists: ${queueUrl}`);
            return [queueUrl, false];
        } else {
            const createQueueResponse = await sqs.send(new CreateQueueCommand({ QueueName: queueName }));
            const queueUrl = createQueueResponse.QueueUrl;
            console.log(`Created queue: ${queueUrl}`);
            return [queueUrl, true];
        }
    } catch (error) {
        console.error('Error in createOrGetQueue:', error);
        throw error;
    }
}

async function runSqsTest(endpointUrl, awsSecretAccessKey) {
    const clientConfig = {
        region: 'us-east-1',
        credentials: {
            accessKeyId: 'YOUR_ACCESS_KEY_ID',
            secretAccessKey: awsSecretAccessKey
        },
        endpoint: endpointUrl,
        tls: endpointUrl.startsWith('https'),
        forcePathStyle: true
    };
    console.log('Creating SQS client with config:', clientConfig);
    const sqs = new SQSClient(clientConfig);

    const queueName = 'my-test-que-for-testing';
    let queueUrl, queueCreated;

    try {
        [queueUrl, queueCreated] = await createOrGetQueue(sqs, queueName);
        console.log(`Queue URL: ${queueUrl}`);

        await sqs.send(new SendMessageCommand({ QueueUrl: queueUrl, MessageBody: 'hello world' }));
        console.log('Sent a message to the queue');

        const receiveResponse = await sqs.send(new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 1 }));
        if (receiveResponse.Messages && receiveResponse.Messages.length > 0) {
            const message = receiveResponse.Messages[0];
            console.log(`Received message: ${message.Body}`);

            await sqs.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: message.ReceiptHandle }));
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
            await sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
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
