package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
)

func main() {
	for k, v := range os.Args {
		log.Println(k, v)
	}
	log.Println("-----")
	if len(os.Args) > 1 {
		return
	}
	//TODO, add the call to downloadFromBucket
	log.Println("Error: os.Args was 1 length.")
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

func downloadFromBucket(b string, f string) {
	p, s, setErr := getSettings()
	if setErr != nil {
		log.Println("Error:", setErr)
		return
	}
	auth := aws.Auth{AccessKey: p, SecretKey: s}

	S3 := s3.New(auth, aws.APNortheast)
	bucket := S3.Bucket(b)
	log.Println("Starting Get...")
	data, err := bucket.Get(f)
	if err != nil {
		log.Println("ERROR:", err)
		return
	}
	log.Println("All Done!")
	log.Println("Length:", len(data))
}
