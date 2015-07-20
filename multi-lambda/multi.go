package main

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
)

type mathProblem struct {
	Num1 int
	Num2 int
}

func main() {
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
			var mp mathProblem
			sDec, errDec := base64.StdEncoding.DecodeString(v.Kinesis.Data)
			if errDec != nil {
				log.Println("Error:", errDec)
			} else {
				log.Println(string(sDec))
				errJson := json.Unmarshal(sDec, &mp)
				if errJson != nil {
					log.Println("Error:", errJson)
					return
				}
				t := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
				h := md5.New()
				io.WriteString(h, fmt.Sprintf("multi-%v", t))
				filename := fmt.Sprintf("multi-%x", h.Sum(nil))
				answer := fmt.Sprintf("%v * %v = %v", mp.Num1, mp.Num2, mp.Num1*mp.Num2)
				writeToBucket(filename, answer)
				log.Println("All done!")
			}
		}
		return

	}
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

func writeToBucket(f string, a string) {
	p, s, setErr := getSettings()
	if setErr != nil {
		log.Println("Error:", setErr)
		return
	}
	auth := aws.Auth{AccessKey: p, SecretKey: s}

	S3 := s3.New(auth, aws.APNortheast)
	bucket := S3.Bucket("math-results")
	err := bucket.Put(f, []byte(a), "text/plain", s3.PublicRead, s3.Options{})
	if err != nil {
		log.Println("ERROR:", err)
	}
}
