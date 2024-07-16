package tester

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func Run(numSenders, numReceivers, numMessagesPerGoroutine int, endpoint string) {

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
	cfg.BaseEndpoint = &endpoint
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	// cfg.RateLimiter = NoOpRateLimit{}

	sqsClient := sqs.NewFromConfig(cfg)

	var wg sync.WaitGroup

	var ch chan int

	for i := 0; i < numSenders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numMessagesPerGoroutine; j++ {
				sendMessage(sqsClient, queueUrl, id, j)
				sentMessages += 1
			}
		}(i)
	}

	for i := 0; i < numReceivers; i++ {
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
			log.Printf("sent: %d, received: %d, pct: %f\n", sentMessages, receivedMessages, pct)
			time.Sleep(1 * time.Second)
		}
	}()

	wg.Wait()

	if numSenders > 0 {
		log.Println("All messages sent")
		if numReceivers == 0 {
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

func sendMessage(client *sqs.Client, queueUrl string, goroutineID, requestID int) {

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
		log.Println(err)
	}

	for _, msg := range msgs.Messages {
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
