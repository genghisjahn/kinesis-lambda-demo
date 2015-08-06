package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"sync"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/sqs"
	_ "github.com/lib/pq"
)

var SQS *sqs.SQS

func main() {
	n := runtime.NumCPU()
	log.Println("Num CPUS:", n)
	runtime.GOMAXPROCS(n)
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
		for _, v := range kpayload.Records {
			log.Println("Record:", v)
			var tpm topicPageMessage
			sDec, errDec := base64.StdEncoding.DecodeString(v.Kinesis.Data)
			if errDec != nil {
				log.Println("Error:", errDec)
			} else {
				log.Println(string(sDec))
				errJSON := json.Unmarshal(sDec, &tpm)
				if errJSON != nil {
					log.Println("Error:", errJSON)
					return
				}
				arns := getDevicesArnsByTopicIDPage(tpm.TopicID, tpm.PageNum, 10000)
				//This should return the arn & the lang for the user
				//we'd then pull the correct iten out of the message map
				msgSlice := make([]sqs.Message, 0, 10)
				msgAll := [][]sqs.Message{}
				for _, v := range arns {
					tempData := fmt.Sprintf("arn:%v|%v", v, tpm.Message)
					msg := sqs.Message{Body: base64.StdEncoding.EncodeToString([]byte(tempData))}
					msgSlice = append(msgSlice, msg)
					if len(msgSlice) == 10 {
						msgAll = append(msgAll, msgSlice)
						msgSlice = []sqs.Message{}
					}
				}
				var wg sync.WaitGroup
				for _, s := range msgAll {
					s := s //It's idomatic go I swear! http://golang.org/doc/effective_go.html#channels
					wg.Add(1)
					go func(sl10 []sqs.Message) {
						proxySNS(sl10)
						defer wg.Done()
					}(s)

				}
				wg.Done()
				log.Println("All done!")
			}
		}
		return

	}
	log.Println("Error: os.Args was 1 length.")
}

func getDevicesArnsByTopicIDPage(topicID, pagenum, pagesize int) []string {
	var arns []string
	info := getDBSettings()
	db, errCon := sql.Open("postgres", fmt.Sprintf("host=%v user=%v password=%v dbname=%v sslmode=require", info.Host, info.Username, info.Password, info.Database))
	defer db.Close()
	if errCon != nil {
		log.Fatal(errCon)
	}
	rows, err := db.Query(`
		select
			u.endpointarn
		from
			subscription s ,userdevices u
		where
			s.topicid= $1 and
			s.userID=u.userid
			order by u.userid
			limit $2 offset $3
;`, topicID, pagesize, (pagenum-1)*pagesize)
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var arn string
		errScan := rows.Scan(&arn)
		if errScan != nil {
			panic(errScan)
		}
		arns = append(arns, arn)
	}
	return arns
}

type topicPageMessage struct {
	TopicID int    `json:"topic_id"`
	Message string `json:"message"`
	PageNum int    `json:"page_num"`
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

type dbInfo struct {
	Host     string
	Database string
	Username string
	Password string
}

func proxySNS(msgs []sqs.Message) {
	pub, sec, _ := getSettings()
	sqs, err := getQueue("sns-prox", pub, sec)
	if err != nil {
		panic(err)
	}
	_, respErr := sqs.SendMessageBatch(msgs)
	if respErr != nil {
		log.Println("ERROR:", respErr)
	}
}

func getQueue(name, public, secret string) (*sqs.Queue, error) {
	auth := aws.Auth{AccessKey: public, SecretKey: secret}
	region := aws.Region{}
	region.Name = "us-east-1"
	region.SQSEndpoint = "http://sqs.us-east-1.amazonaws.com"
	SQS = sqs.New(auth, region)
	if SQS == nil {
		return nil, fmt.Errorf("Can't get sqs reference for %v %v", auth, region)
	}
	return SQS.GetQueue(name)
}
