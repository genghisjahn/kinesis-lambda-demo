package main

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
	"github.com/lib/pq"
)

type dbInfo struct {
	Host     string
	Database string
	Username string
	Password string
}

type userInfo struct {
	UserID int
	Email  string
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

type createdata func([]int) error

func createUserData() {
	ids := getUserIDs()
	return
	cdata := []createdata{createLang, createDevices, createGroups, createSubscriptions}
	for _, v := range cdata {
		if dataErr := v(ids); dataErr != nil {
			panic(fmt.Sprintf("%v - %v", v, dataErr))
		}
	}
}

func createLang(ids []int) error {
	return fmt.Errorf("Not implemented")
}

func createDevices(ids []int) error {
	return fmt.Errorf("Not implemented")

}

func createGroups(ids []int) error {
	return fmt.Errorf("Not implemented")

}

func createSubscriptions(ids []int) error {
	return fmt.Errorf("Not implemented")

}

func main() {
	//createUserData()
	//return
	for k, v := range os.Args {
		log.Println(k, v)
	}
	log.Println("-----")
	if len(os.Args) > 0 {
		data, err := downloadFromBucket("csv-stream-demo", "data.csv")
		if err != nil {
			log.Println("Error:", err)
			return
		}

		log.Println("DB Start")
		errDB := copyDataToDB(data)
		if errDB != nil {
			log.Println("Error:", errDB)
		}
		log.Println("DB END")
		return
	}
	log.Println("Error: os.Args was 1 length.")
}

func getUserIDs() []int {
	info := getDBSettings()
	db, errCon := sql.Open("postgres", fmt.Sprintf("host=%v user=%v password=%v dbname=%v sslmode=require", info.Host, info.Username, info.Password, info.Database))
	defer db.Close()
	if errCon != nil {
		log.Fatal(errCon)
	}
	log.Println("Connected...")
	return nil
}

func copyDataToDB(data []byte) error {
	info := getDBSettings()
	db, errCon := sql.Open("postgres", fmt.Sprintf("host=%v user=%v password=%v dbname=%v sslmode=require", info.Host, info.Username, info.Password, info.Database))
	defer db.Close()
	if errCon != nil {
		log.Fatal(errCon)
	}
	txn, errT := db.Begin()
	if errT != nil {
		log.Println(errT)
		return errT
	}
	stmt, errPrep := txn.Prepare(pq.CopyIn("userdata", "userid", "jobid"))
	if errPrep != nil {
		log.Fatal(errPrep)
	}
	r := bytes.NewReader(data)
	reader := csv.NewReader(r)
	reader.Comma = ','
	lineCount := 0
	log.Println("Start For...")
	var wg sync.WaitGroup
	for {

		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Error:", err)
			return err
		}

		email := record[0]
		_ = email
		jobid := "abc123"
		userID, _ := strconv.Atoi(record[1])
		_ = userID
		wg.Add(1)
		go func(id int, e string) {
			defer wg.Done()
			_, errA := stmt.Exec(id, e)
			if errA != nil {
				log.Fatal(errA)
			}
		}(userID, jobid)
		lineCount++
		if lineCount == 1000000 {
			break
		}
	}
	wg.Wait()
	log.Println("End For")
	log.Println("Start Exec")
	_, errEX := stmt.Exec()
	if errEX != nil {
		log.Fatal(errEX)
	}
	log.Println("End Exec")

	errClose := stmt.Close()
	if errClose != nil {
		log.Fatal(errClose)
	}
	log.Println("Start Commit")
	errCommit := txn.Commit()
	if errCommit != nil {
		log.Fatal(errCommit)
	}
	log.Println("End Commit")
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
	log.Println("Completed Get!", len(data))
	return data, nil
}
