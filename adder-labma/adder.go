package main

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
)

type mathProblem struct {
	Num1 int
	Num2 int
}

func main() {
	log.Println("-----")
	for _, v := range os.Args {
		log.Println(v)
	}
	log.Println("-----")
	if len(os.Args) > 1 {
		rawData := os.Args[1]

		var mp mathProblem
		err := json.Unmarshal([]byte(rawData), &mp)
		if err != nil {
			log.Println("Error:", err)
			return
		}
		t := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
		h := md5.New()
		io.WriteString(h, fmt.Sprintf("add-%v", t))
		filename := fmt.Sprintf("add-%x", h.Sum(nil))
		answer := fmt.Sprintf("%v + %v = %v", mp.Num1, mp.Num2, mp.Num1+mp.Num2)
		writeToBuck(filename, answer)
		return
	}
	log.Println("Error: os.Args was 1 length.")
}

func writeToBuck(f string, a string) {
	auth, authErr := aws.GetAuth("", "", "", time.Time{})
	if authErr != nil {
		log.Println("Error:", authErr)
		return
	}
	S3 := s3.New(auth, aws.APNortheast)
	bucket := S3.Bucket("math-answers")
	err := bucket.Put(f, []byte(a), "text/plain", s3.PublicRead, s3.Options{})
	if err != nil {
		log.Println("ERROR:", err)
	}
}
