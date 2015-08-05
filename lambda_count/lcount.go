package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/kinesis"
)

type dbInfo struct {
	Host     string
	Database string
	Username string
	Password string
}

type topicMessage struct {
	TopicID int    `json:"topic_id"`
	Message string `json:"message"`
}

type topicPageMessage struct {
	TopicID int    `json:"topic_id"`
	Message string `json:"message"`
	PageNum int    `json:"page_num"`
}

func main() {
	pub, secret, _ := getSettings()
	auth := aws.Auth{AccessKey: pub, SecretKey: secret}
	K := kinesis.New(auth, aws.USEast)
	for k, v := range os.Args {
		log.Println(k, v)
	}
	log.Println("-----")
	if len(os.Args) > 1 {
		rawData := os.Args[1]

		var kpayload KinesisPayload
		err := json.Unmarshal([]byte(rawData), &kpayload)
		if err != nil {
			log.Println("Error Kinesisis Payload:", err)
		}
		log.Println("Record Count:", len(kpayload.Records))
		for _, v := range kpayload.Records {
			var tm topicMessage
			sDec, errDec := base64.StdEncoding.DecodeString(v.Kinesis.Data)
			if errDec != nil {
				log.Println("Error:", errDec)
			} else {
				log.Println(string(sDec))
				errJSON := json.Unmarshal(sDec, &tm)
				if errJSON != nil {
					log.Println("Error:", errJSON)
					return
				}
				pageCount := getDevicesByTopicIDPageCount(tm.TopicID)
				for i := 0; i < pageCount; i++ {
					tpm := topicPageMessage{}
					tpm.Message = tm.Message
					tpm.TopicID = tm.TopicID
					tpm.PageNum = i + 1
					jsonData, jsonErr := json.Marshal(&tpm)
					if jsonErr != nil {
						log.Println("Error:", jsonErr)
						break
					}
					_, err := K.PutRecord("topic-message-age", "topic-page", jsonData, "", "")
					if err != nil {
						log.Println("Error:", err)
					}
					log.Println("Sent...", tpm)
				}
				log.Println("All done!")
			}
		}
		return

	}
	log.Println("Error: os.Args was 1 length.")
}

func getDevicesByTopicIDPageCount(topicID int) int {
	var count int
	info := getDBSettings()
	db, errCon := sql.Open("postgres", fmt.Sprintf("host=%v user=%v password=%v dbname=%v sslmode=require", info.Host, info.Username, info.Password, info.Database))
	defer db.Close()
	if errCon != nil {
		log.Fatal(errCon)
	}
	rows, err := db.Query(`
		select
			((count(u.endpointarn)/10000)+1) as pagecoun
		from
			subscription s ,userdevices u
		where
			s.topicid= $1 and
			s.userID=u.userid

;`, topicID)
	if err != nil {
		panic(err)
	}
	rows.Next()
	errScan := rows.Scan(&count)
	if errScan != nil {
		panic(errScan)
	}
	return count
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

func getDBSettings() *dbInfo {
	file, err := ioutil.ReadFile("./settings.json")
	if err != nil {
		log.Println("Error:", err)
		return nil
	}
	db := dbInfo{}
	err2 := json.Unmarshal(file, &db)
	if err2 != nil {
		log.Println("Error:", err2)
		return nil
	}
	return &db
}

type KinesisPayload struct {
	Records []struct {
		AwsRegion         string `json:"awsRegion"`
		EventID           string `json:"eventID"`
		EventName         string `json:"eventName"`
		EventSource       string `json:"eventSource"`
		EventSourceARN    string `json:"eventSourceARN"`
		EventVersion      string `json:"eventVersion"`
		InvokeIdentityArn string `json:"invokeIdentityArn"`
		Kinesis           struct {
			Data                 string `json:"data"`
			KinesisSchemaVersion string `json:"kinesisSchemaVersion"`
			PartitionKey         string `json:"partitionKey"`
			SequenceNumber       string `json:"sequenceNumber"`
		} `json:"kinesis"`
	} `json:"Records"`
}
