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

	S3 := s3.New(auth, aws.APNortheast)
	bucket := S3.Bucket("math-demo2")

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
