package main

import (
	"bytes"
	cr "crypto/rand"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	mr "math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

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

type createdata func(int) error

func createUserData() {
	ids := getUserIDs()
	log.Println("userID count:", len(ids))
	if dataErr := createSubscriptions(ids); dataErr != nil {
		panic(fmt.Sprintf("%v", dataErr))
	}
}

func createLang(ids []int) error {
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
	stmt, errPrep := txn.Prepare(pq.CopyIn("userinfo", "userid", "langcode"))
	if errPrep != nil {
		log.Fatal(errPrep)
	}

	log.Println("Start For...")
	var wg sync.WaitGroup
	for _, id := range ids {

		userID := id
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, errA := stmt.Exec(userID, "en")
			if errA != nil {
				log.Fatal(errA)
			}
		}(userID)

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

func createDevices(ids []int) error {
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
	stmt, errPrep := txn.Prepare(pq.CopyIn("userdevices", "userid", "token", "endpointarn"))
	if errPrep != nil {
		log.Fatal(errPrep)
	}

	log.Println("Start For...")
	var wg sync.WaitGroup
	for _, id := range ids {

		userID := id
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			mr.Seed(time.Now().UnixNano())
			numDevs := mr.Intn(3) + 1
			for i := 0; i < numDevs; i++ {
				b := make([]byte, 32)
				c := make([]byte, 16)
				cr.Read(b)
				cr.Read(c)
				token := fmt.Sprintf("%X", b[0:32])
				arn := fmt.Sprintf("arn:%X", c[0:8])
				_, errA := stmt.Exec(userID, token, arn)
				if errA != nil {
					log.Fatal(errA)
				}
			}
		}(userID)

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

// func createGroups(id int) error {
// 	return fmt.Errorf("Not implemented")
//
// }

func createSubscriptions(ids []int) error {
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
	stmt, errPrep := txn.Prepare(pq.CopyIn("subscription", "topicid", "userid"))
	if errPrep != nil {
		log.Fatal(errPrep)
	}

	log.Println("Start For...")
	var wg sync.WaitGroup
	for _, id := range ids {

		userID := id
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			mr.Seed(time.Now().UnixNano())
			numSubs := mr.Intn(5) + 1
			for i := 0; i < numSubs; i++ {
				topicID := i + 1
				_, errA := stmt.Exec(topicID, userID)
				if errA != nil {
					log.Fatal(errA)
				}
			}
		}(userID)

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

func main() {
	//pagecount := getDevicesByTopicIDPageCount(1)
	var arns []string
	log.Println("Start...")
	arns = getDevicesArnsByTopicIDPage(1, 18, 10000)
	log.Println(arns[3453])
	log.Println("End...")
	log.Println("Start...")
	arns = getDevicesArnsByTopicIDPage(1, 12, 10000)
	log.Println(arns[3453])
	log.Println("End...")
	return
	n := runtime.NumCPU()
	log.Println("Num CPUS:", n)
	runtime.GOMAXPROCS(n)
	pagecount := 21
	log.Println("Page Count:", pagecount)
	var wg sync.WaitGroup
	for i := 0; i < pagecount; i++ {
		wg.Add(1)
		num := i
		go func() {
			defer wg.Done()
			arns := getDevicesArnsByTopicIDPage(1, num+1, 100000)
			if len(arns) > 0 {
				log.Println(num+1, arns[0])
			} else {
				log.Println(num+1, "was zero length")
			}
		}()
	}
	log.Println("Waiting")
	wg.Wait()
	log.Println("Done")
}

func downloadBucketStuff() {
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
			((count(u.endpointarn)/100000)+1) as pagecoun
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

func getUserIDsByTopicID(topicID int) []int {
	var ids []int
	info := getDBSettings()
	db, errCon := sql.Open("postgres", fmt.Sprintf("host=%v user=%v password=%v dbname=%v sslmode=require", info.Host, info.Username, info.Password, info.Database))
	defer db.Close()
	if errCon != nil {
		log.Fatal(errCon)
	}
	log.Println("Starting Query...")
	rows, err := db.Query("select userid from subscription where topicid= $1", topicID)
	if err != nil {
		panic(err)
	}
	log.Println("Query Complete!")
	log.Println("Starting loop")
	for rows.Next() {
		var userid int
		errScan := rows.Scan(&userid)
		if errScan != nil {
			panic(errScan)
		}
		ids = append(ids, userid)
	}
	log.Println("Loop Complete!")
	log.Println("Count:", len(ids))
	return ids
}

func getUserIDs() []int {
	var ids []int
	info := getDBSettings()
	db, errCon := sql.Open("postgres", fmt.Sprintf("host=%v user=%v password=%v dbname=%v sslmode=require", info.Host, info.Username, info.Password, info.Database))
	defer db.Close()
	if errCon != nil {
		log.Fatal(errCon)
	}
	log.Println("Starting Query...")
	rows, err := db.Query("SELECT userid FROM jobuser where jobid = 'def456'")
	if err != nil {
		panic(err)
	}
	log.Println("Query Complete!")
	log.Println("Starting loop")
	for rows.Next() {
		var userid int
		errScan := rows.Scan(&userid)
		if errScan != nil {
			panic(errScan)
		}
		ids = append(ids, userid)
	}
	log.Println("Loop Complete!")
	return ids
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
	stmt, errPrep := txn.Prepare(pq.CopyIn("jobuser", "userid", "jobid", "messageid"))
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
		jobid := "ghi789"
		userID, _ := strconv.Atoi(record[1])
		_ = userID
		wg.Add(1)
		go func(id int, e string) {
			defer wg.Done()
			_, errA := stmt.Exec(id, e, "100")
			if errA != nil {
				log.Fatal(errA)
			}
		}(lineCount+1, jobid)
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
