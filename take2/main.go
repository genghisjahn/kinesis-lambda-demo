package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
)

type config struct {
	Access   string
	Secret   string
	Host     string
	Database string
	Username string
	Password string
	Topicarn string
}

func main() {
	fmt.Println("hello")
	c, err := getConfig()
	if err != nil {
		log.Println(err)
		return
	}
	log.Println(*c)
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
	c.Password = sm["Passowrd"]
	c.Host = sm["Host"]
	c.Topicarn = sm["TopicARN"]
	return &c, nil
}
