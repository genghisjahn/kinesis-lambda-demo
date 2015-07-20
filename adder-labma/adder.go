package main

import (
	"crypto/md5"
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

	if len(os.Args) > 1 {
		rawData := os.Args[1]

		var mp mathProblem
		err := json.Unmarshal([]byte(rawData), &mp)
		if err != nil {
			log.Println("Error:", err)
			return
		}
		t := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
		h := md5.New()
		io.WriteString(h, fmt.Sprintf("add-%v", t))
		filename := fmt.Sprintf("add-%x", h.Sum(nil))
		answer := fmt.Sprintf("%v + %v = %v", mp.Num1, mp.Num2, mp.Num1+mp.Num2)
		writeToBuck(filename, answer)
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

func writeToBuck(f string, a string) {
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
