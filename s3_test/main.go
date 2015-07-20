package main

import (
	"log"
	"os"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
)

func main() {
	var pub = os.Getenv("AWSPUB")
	var secret = os.Getenv("AWSSecret")
	auth := aws.Auth{AccessKey: pub, SecretKey: secret}
	//region := aws.APNortheast
	// region.Name = "us-west-2"
	// region.SQSEndpoint = "http://sqs.us-west-2.amazonaws.com"
	//region.S3Endpoint = "http://s3.amazonaws.com"
	//region.S3BucketEndpoint = "math-demo2"
	S3 := s3.New(auth, aws.APNortheast)
	bucket := S3.Bucket("math-results")
	// errPut := bucket.PutBucket(s3.Private)
	// if errPut != nil {
	// 	log.Println("Error Put", errPut)
	// }
	if bucket != nil {
		log.Println("It's not nil!", *bucket)
	}
	err := bucket.Put("test-file1", []byte("Did this go through?"), "text/plain", s3.PublicRead, s3.Options{})
	if err != nil {
		log.Println("ERROR:", err)
	}
	err2 := bucket.Put("test-file2", []byte("Dasdfasdf"), "text/plain", s3.PublicRead, s3.Options{})
	if err2 != nil {
		log.Println("ERROR:", err)
	}
	err3 := bucket.Put("test-file3", []byte("asdfasdfa"), "text/plain", s3.PublicRead, s3.Options{})
	if err3 != nil {
		log.Println("ERROR:", err)
	}
}
