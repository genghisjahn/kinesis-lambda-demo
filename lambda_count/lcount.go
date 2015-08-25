package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/ec2"
	"github.com/AdRoll/goamz/kinesis"
	"github.com/AdRoll/goamz/sns"
	_ "github.com/lib/pq"
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
	TopicID  int    `json:"topic_id"`
	Message  string `json:"message"`
	PageNum  int    `json:"page_num"`
	LastPage bool   `json:"last_page"`
}

func TestAdd() {
	pub, secret, sg, _ := getSettings()
	AddIPToGroup(pub, secret, sg)
}

func main() {
	TestAdd()
	return
	pub, secret, sg, _ := getSettings()
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
		for _, v := range kpayload.Records {
			log.Println("Record:", v)
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
				AddIPToGroup(pub, secret, sg)
				pageCount := getDevicesByTopicIDPageCount(tm.TopicID)
				RemoveIPFromGroup(sg)
				publishMessage("Count: 1st page sent")
				for i := 0; i < pageCount; i++ {
					tpm := topicPageMessage{}
					tpm.Message = tm.Message
					tpm.TopicID = tm.TopicID
					tpm.PageNum = i + 1
					if i+1 == pageCount {
						tpm.LastPage = true
						log.Println("LastPage is", i+1)
					}
					jsonData, jsonErr := json.Marshal(&tpm)
					if jsonErr != nil {
						log.Println("Error:", jsonErr)
						break
					}
					_, err := K.PutRecord("topic-message-page", "topic-page", jsonData, "", "")
					if tpm.LastPage {
						publishMessage(fmt.Sprintf("Count: Last Page Sent: %v", pageCount))
					}
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
func AddIPToGroup(p string, s string, secGroup string) error {
	ec2 := ec2.EC2{}
	auth := aws.Auth{AccessKey: p, SecretKey: s}
	region := aws.Region{}
	region.Name = "us-east-1"
	ec2.Auth = auth
	ec2.Region = region
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		panic(err)
	}
	for i, addr := range addrs {
		log.Printf("%d %v\n", i, addr)
	}
	return nil
}
func RemoveIPFromGroup(secGroup string) error {
	return nil
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
	dbcount := GetPageCountFromDB()
	if dbcount > 0 {
		return dbcount
	}
	return count
}

func getSettings() (string, string, string, error) {
	file, err := ioutil.ReadFile("./settings.json")
	if err != nil {
		return "", "", "", nil
	}
	settingsMap := make(map[string]string)
	json.Unmarshal(file, &settingsMap)
	return settingsMap["Access"], settingsMap["Secret"], settingsMap["SecGroup"], nil
}

func GetPageCountFromDB() int {
	info := getDBSettings()
	db, errCon := sql.Open("postgres", fmt.Sprintf("host=%v user=%v password=%v dbname=%v sslmode=require", info.Host, info.Username, info.Password, info.Database))
	defer db.Close()
	if errCon != nil {
		log.Fatal(errCon)
	}
	rows, err := db.Query(`
		select
			s.value
		from
			lambdasettings s
		where
			s.name= 'pagecount'
	;`)
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var strval string
		errScan := rows.Scan(&strval)
		if errScan != nil {
			panic(errScan)
		}
		bc, cErr := strconv.Atoi(strval)
		if cErr != nil {
			panic(cErr)
		}
		return bc
	}
	return 0
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

func publishMessage(msg string) error {
	topicarn, topicErr := getTopicArn()
	if topicErr != nil {
		log.Println(topicErr)
		return topicErr
	}
	p, s, _, _ := getSettings()
	auth := aws.Auth{AccessKey: p, SecretKey: s}
	region := aws.Region{}
	region.Name = "us-east-1"
	region.SNSEndpoint = "http://sns.us-east-1.amazonaws.com"
	awssns, _ := sns.New(auth, region)
	if awssns == nil {
		return fmt.Errorf("Can't get sns reference for %v %v", auth, region)
	}
	opt := sns.PublishOptions{}
	opt.TopicArn = topicarn
	opt.Message = msg
	opt.Subject = msg
	_, pubErr := awssns.Publish(&opt)
	if pubErr != nil {
		return pubErr
	}
	return nil
}

func getTopicArn() (string, error) {
	file, err := ioutil.ReadFile("./settings.json")
	if err != nil {
		return "", err
	}
	settingsMap := make(map[string]string)
	json.Unmarshal(file, &settingsMap)
	return settingsMap["Topicarn"], nil
}
