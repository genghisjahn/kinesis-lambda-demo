package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/kinesis"
)

func main() {
	rand.Seed(time.Now().UnixNano()) // takes the current time in nanoseconds as the seed

	var pub = os.Getenv("AWSPUB")
	var secret = os.Getenv("AWSSecret")
	auth := aws.Auth{AccessKey: pub, SecretKey: secret}
	K := kinesis.New(auth, aws.USEast)

	for i := 0; i < 10; i++ {
		problemMap := make(map[string]int)
		problemMap["Num1"] = rand.Intn(100)
		problemMap["Num2"] = rand.Intn(100)
		jsonData, jsonErr := json.Marshal(problemMap)
		if jsonErr != nil {
			log.Println("Error:", jsonErr)
		}

		_, err2 := K.PutRecord("math-problems", "math-p", jsonData, "", "")
		log.Println(problemMap["Num1"], "+", problemMap["Num2"], "...sent!")
		if err2 != nil {
			log.Println("Error:", err2)
		}

		time.Sleep(10 * time.Millisecond)
	}
}
