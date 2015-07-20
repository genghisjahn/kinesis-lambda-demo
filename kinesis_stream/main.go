package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/kinesis"
)

func main() {
	var pub = os.Getenv("AWSPUB")
	var secret = os.Getenv("AWSSecret")
	auth := aws.Auth{AccessKey: pub, SecretKey: secret}
	K := kinesis.New(auth, aws.USEast)
	problemMap := make(map[string]int)
	problemMap["Num1"] = 10
	problemMap["Num2"] = 25
	jsonData, jsonErr := json.Marshal(problemMap)
	if jsonErr != nil {
		log.Println("Error:", jsonErr)
	}
	_, err2 := K.PutRecord("math-problems", "math-p", jsonData, "", "")
	if err2 != nil {
		log.Println("Error:", err2)
	}
}
