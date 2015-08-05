package main

import (
	"encoding/json"
	"io/ioutil"
	"log"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/kinesis"
)

type topicMessage struct {
	TopicID int    `json:"topic_id"`
	Message string `json:"message"`
}

func main() {
	pub, secret, _ := getSettings()
	auth := aws.Auth{AccessKey: pub, SecretKey: secret}
	K := kinesis.New(auth, aws.USEast)
	tm := topicMessage{}
	tm.TopicID = 1
	tm.Message = "THIS IS THIS MESSAGE PAYLOAD THAT WILL GO TO THE USER!!!!!"

	jsonData, jsonErr := json.Marshal(&tm)
	if jsonErr != nil {
		log.Println("Error:", jsonErr)
		return
	}

	_, err := K.PutRecord("topic-message", "topic-msg", jsonData, "", "")
	if err != nil {
		log.Println("Error:", err)
	}
	log.Println("Done!")

}

func getSettings() (string, string, error) {
	file, err := ioutil.ReadFile("./settings.json")
	if err != nil {
		return "", "", nil
	}
	settingsMap := make(map[string]string)
	json.Unmarshal(file, &settingsMap)
	return settingsMap["Access"], settingsMap["Secret"], nil
}
