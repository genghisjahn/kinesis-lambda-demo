package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
)

type DBInfo struct {
	Host     string
	Database string
	Username string
	Password string
}

func getDBSettings() *DBInfo {
	file, err := ioutil.ReadFile("./settings.json")
	if err != nil {
		log.Println("Error:", err)
		return nil
	}
	db := DBInfo{}
	err2 := json.Unmarshal(file, &db)
	if err2 != nil {
		log.Println("Error:", err2)
		return nil
	}
	return &db
}

func main() {
	for k, v := range os.Args {
		log.Println(k, v)
	}
	log.Println("-----")
	if len(os.Args) > 1 {
		data, err := downloadFromBucket("csv-stream-demo", "data.csv")
		if err != nil {
			log.Println("Error:", err)
		}
		errDB := copyDataToDB(data)
		if errDB != nil {
			log.Println("Error:", errDB)
		}
		return
	}
	log.Println("Error: os.Args was 1 length.")
}

func copyDataToDB(data []byte) error {
	return nil
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

func downloadFromBucket(b string, f string) ([]byte, error) {
	p, s, setErr := getSettings()
	if setErr != nil {
		return nil, setErr
	}
	auth := aws.Auth{AccessKey: p, SecretKey: s}

	S3 := s3.New(auth, aws.USEast)
	bucket := S3.Bucket(b)
	log.Println("Starting Get...")
	data, err := bucket.Get(f)
	if err != nil {
		return nil, err
	}
	log.Println("All Done!")
	log.Println("Length:", len(data))
	return data, nil
}
