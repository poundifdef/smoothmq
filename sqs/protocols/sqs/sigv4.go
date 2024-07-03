package sqs

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type AuthHeader struct {
	Algorithm     string
	AccessKey     string
	Date          string
	SignedHeaders []string
	Signature     string
	Region        string
	Service       string
}

func ParseAuthorizationHeader(r *http.Request) (AuthHeader, error) {

	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return AuthHeader{}, errors.New("Authorization header is missing")
	}

	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 {
		return AuthHeader{}, errors.New("invalid Authorization header format")
	}

	rc := AuthHeader{}
	rc.Algorithm = parts[0]

	componentsString := parts[1]

	components := strings.Split(componentsString, ",")
	for _, componentString := range components {
		kv := strings.Split(strings.TrimSpace(componentString), "=")

		if strings.EqualFold(kv[0], "Credential") {
			tokens := strings.Split(kv[1], "/")
			rc.AccessKey = tokens[0]
			rc.Date = tokens[1]
			rc.Region = tokens[2]
			rc.Service = tokens[3]
		} else if strings.EqualFold(kv[0], "SignedHeaders") {
			rc.SignedHeaders = strings.Split(kv[1], ";")
		} else if strings.EqualFold(kv[0], "Signature") {
			rc.Signature = kv[1]
		}

	}

	return rc, nil
}

func getSignatureKey(key, dateStamp, region, service string) []byte {
	kSecret := []byte(fmt.Sprintf("AWS4%s", key))
	kDate := sign(kSecret, dateStamp)
	kRegion := sign(kDate, region)
	kService := sign(kRegion, service)
	kSigning := sign(kService, "aws4_request")
	return kSigning
}

func sign(key []byte, message string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(message))
	return h.Sum(nil)
}

func hashAndEncode(s string) string {
	h := sha256.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

func ValidateAWSRequest(awsCreds AuthHeader, secretKey string, r *http.Request) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}

	defer r.Body.Close()

	payloadHash := hashAndEncode(string(body))

	canonicalHeaders := strings.Builder{}
	for _, header := range awsCreds.SignedHeaders {
		canonicalHeaders.WriteString(header)
		canonicalHeaders.WriteString(":")
		canonicalHeaders.WriteString(r.Header.Get(header))
		canonicalHeaders.WriteString("\n")
	}

	signedHeaders := strings.Join(awsCreds.SignedHeaders, ";")

	canonicalRequest := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		strings.ToUpper(r.Method), r.URL, "", canonicalHeaders.String(), signedHeaders, payloadHash)

	canonicalRequestHash := hashAndEncode(canonicalRequest)
	credentialScope := fmt.Sprintf("%s/%s/sqs/aws4_request",
		awsCreds.Date, awsCreds.Region)

	stringToSign := fmt.Sprintf("%s\n%s\n%s\n%s",
		awsCreds.Algorithm, r.Header.Get("x-amz-date"), credentialScope, canonicalRequestHash)

	signingKey := getSignatureKey(secretKey, awsCreds.Date, awsCreds.Region, "sqs")

	signatureSHA := hmac.New(sha256.New, signingKey)
	signatureSHA.Write([]byte(stringToSign))
	signatureString := hex.EncodeToString(signatureSHA.Sum(nil))

	// authorizationHeader := fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s", awsCreds.Algorithm, awsCreds.AccessKey, credentialScope, signedHeaders, signatureString)

	if signatureString == awsCreds.Signature {
		return nil
	}

	return errors.New("invalid signature")
}
