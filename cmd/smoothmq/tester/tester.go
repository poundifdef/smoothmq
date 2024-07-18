package tester

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"

	"math/rand"
	"os"
	"sync"
	"time"

	smoothCfg "github.com/poundifdef/smoothmq/config"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func Run(c smoothCfg.TesterCommand) {

	// BaseEndpoint := "https://smoothmq-sqs.fly.dev"

	var sentMessages, receivedMessages int

	// Hardcoded AWS credentials
	awsAccessKeyID := "DEV_ACCESS_KEY_ID"
	awsSecretAccessKey := "DEV_SECRET_ACCESS_KEY"

	queueUrl := "https://sqs.us-east-1.amazonaws.com/123/test-queue"

	// Load the AWS configuration with hardcoded credentials
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(awsAccessKeyID, awsSecretAccessKey, "")),
	)
	cfg.BaseEndpoint = &c.SqsEndpoint
	if err != nil {
		log.Fatal().Msgf("unable to load SDK config, %v", err)
	}
	// cfg.RateLimiter = NoOpRateLimit{}

	sqsClient := sqs.NewFromConfig(cfg)

	var wg sync.WaitGroup

	var ch chan int

	for i := 0; i < c.Senders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < c.Messages; j += c.BatchSize {
				sendMessage(sqsClient, queueUrl, id, j, c.BatchSize)
				sentMessages += 1
			}
		}(i)
	}

	for i := 0; i < c.Receivers; i++ {
		go func(id int) {
			for {
				msgs := receiveMessage(sqsClient, queueUrl, id)
				receivedMessages += msgs
				// time.Sleep(1 * time.Second)
			}
		}(i)
	}

	go func() {
		pct := 0.0
		for {
			if sentMessages > 0 {
				pct = float64(receivedMessages) / float64(sentMessages)
			}
			log.Printf("sent: %d, received: %d, pct: %f", sentMessages, receivedMessages, pct)
			time.Sleep(1 * time.Second)
		}
	}()

	wg.Wait()

	if c.Senders > 0 {
		log.Printf("All messages sent")
		if c.Receivers == 0 {
			os.Exit(0)
		}
	}

	<-ch
}

func GenerateRandomString(n int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func sendMessage(client *sqs.Client, queueUrl string, goroutineID, requestID, batchSize int) {

	if batchSize > 1 {
		input := &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(queueUrl),
		}

		for i := range batchSize {
			messageBody := fmt.Sprintf("Message from goroutine %d, request %d, batchId %d %s", goroutineID, requestID, i, GenerateRandomString(2000))
			input.Entries = append(input.Entries, types.SendMessageBatchRequestEntry{
				Id:          aws.String(fmt.Sprintf("%d", i)),
				MessageBody: &messageBody,
				MessageAttributes: map[string]types.MessageAttributeValue{
					"a": types.MessageAttributeValue{
						DataType:    aws.String("String"),
						StringValue: aws.String("abc"),
					},
					"b": types.MessageAttributeValue{
						DataType:    aws.String("Binary"),
						BinaryValue: []byte("xyz"),
					},
				},
			})
		}

		_, err := client.SendMessageBatch(context.TODO(), input)

		if err != nil {
			log.Printf("Failed to send message from goroutine %d, request %d: %v", goroutineID, requestID, err)
		}

	} else {
		messageBody := fmt.Sprintf("Message from goroutine %d, request %d %s", goroutineID, requestID, GenerateRandomString(2000))
		input := &sqs.SendMessageInput{
			QueueUrl:    aws.String(queueUrl),
			MessageBody: aws.String(messageBody),
			MessageAttributes: map[string]types.MessageAttributeValue{
				"a": types.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String("abc"),
				},
				"b": types.MessageAttributeValue{
					DataType:    aws.String("Binary"),
					BinaryValue: []byte("xyz"),
				},
			},
		}
		_, err := client.SendMessage(context.TODO(), input)

		if err != nil {
			log.Printf("Failed to send message from goroutine %d, request %d: %v", goroutineID, requestID, err)
		}
	}

	// time.Sleep(100 * time.Millisecond)
}

func receiveMessage(client *sqs.Client, queueUrl string, goroutineID int) int {
	i := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueUrl),
		MaxNumberOfMessages: 1,
		MessageAttributeNames: []string{
			"All",
		},
	}
	msgs, err := client.ReceiveMessage(context.TODO(), i)
	if err != nil {
		log.Error().Err(err).Send()
	}

	for _, msg := range msgs.Messages {
		log.Trace().Interface("message", msg).Msg("Received message")
		delInput := &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(queueUrl),
			ReceiptHandle: msg.ReceiptHandle,
		}
		_, delerr := client.DeleteMessage(context.TODO(), delInput)
		if delerr != nil {
			log.Printf("Failed to delete message from goroutine %d, request %d: %v", goroutineID, msg.ReceiptHandle, delerr)
		}
	}

	// time.Sleep(1 * time.Second)
	return len(msgs.Messages)

}
