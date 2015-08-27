package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"

	_ "github.com/lib/pq"
)

type config struct {
	Access   string
	Secret   string
	Host     string
	Database string
	Username string
	Password string
	Topicarn string
	PageSize string
}

func main() {
	c, err := getConfig()
	if err != nil {
		log.Println(err)
		return
	}
	processGroup(c, "Scifi")
}

func processGroup(c *config, groupID string) error {
	db, errCon := sql.Open("postgres", fmt.Sprintf("host=%v user=%v password=%v dbname=%v sslmode=require", c.Host, c.Username, c.Password, c.Database))
	defer db.Close()
	if errCon != nil {
		log.Fatal(errCon)
	}
	tx, err := db.Begin()
	declareCursor := `declare devices SCROLL CURSOR FOR

  select u.langcode,ud.endpointarn arn

  from
    subscription s,userinfo u,userdevices ud
  where
    s.topicid = $1
    and s.userid = u.id
    and u.id = ud.userid;`
	fstmt := fmt.Sprintf("FETCH FORWARD %v from devices;", c.PageSize)
	f := fstmt
	_, err1 := tx.Exec(declareCursor, 1)
	rows, err2 := tx.Query(f)

	if err1 != nil {
		log.Println(err)
	}
	if err2 != nil {
		log.Println(err2)
	}
	arns := []string{}
	for rows.Next() {
		var arn string
		var lang string
		errScan := rows.Scan(&lang, &arn)
		if errScan != nil {
			panic(errScan)
		}
		arns = append(arns, arn)
	}
	psize, _ := strconv.Atoi(c.PageSize)
	if len(arns) == psize {
		//do this again until len(arns) is less
	}
	if err != nil {
		panic(err)
	}
	log.Println("Arn Count:", len(arns))
	tx.Commit()

	//okay, if the return count equals
	//the cursor page size count, do it again
	return nil

}

func getConfig() (*config, error) {
	var c config
	file, err := ioutil.ReadFile("./settings.json")
	if err != nil {
		return nil, err
	}
	sm := make(map[string]string)
	json.Unmarshal(file, &sm)
	c.Access = sm["Access"]
	c.Secret = sm["Secret"]
	c.Database = sm["Database"]
	c.Username = sm["Username"]
	c.Password = sm["Password"]
	c.Host = sm["Host"]
	c.Topicarn = sm["TopicARN"]
	c.PageSize = sm["Pagesize"]
	return &c, nil
}
