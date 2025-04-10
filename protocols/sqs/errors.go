package sqs

import "fmt"

// SQS error response format: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-json-api-responses.html#sqs-api-error-response-structure
// Common errors across all requests: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/CommonErrors.html
// Individual methods have errors: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_GetQueueUrl.html#API_GetQueueUrl_Errors

type SQSError struct {
	Code    int    `json:"-"`
	Type    string `json:"__type"`
	Message string `json:"message"`
}

func (e *SQSError) Error() string {
	return fmt.Sprintf("%s: %s", e.Type, e.Message)
}

func NewSQSError(code int, errType string, message string) *SQSError {
	return &SQSError{Code: code, Type: errType, Message: message}
}

var ErrIncompleteSignature = NewSQSError(400, "IncompleteSignature", "The request signature does not conform to AWS standards.")
var ErrInvalidClientTokenId = NewSQSError(403, "InvalidClientTokenId", "The security token included in the request is invalid")
var ErrQueueDoesNotExist = NewSQSError(400, "QueueDoesNotExist", "Queue does not exist")
var ErrValidationError = NewSQSError(400, "ValidationError", "Invalid request payload")
var ErrQueueNameExists = NewSQSError(400, "QueueNameExists", "Queue already exists")
