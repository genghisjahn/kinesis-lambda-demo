package main

import (
	"encoding/base64"
	"encoding/json"
	"log"
	"os"
)

type mathProblem struct {
	Num1 int
	Num2 int
}

func main() {
	//eyJOdW0xIjo1LCJOdW0yIjo1fQ==
	if len(os.Args) > 1 {
		rawData := os.Args[1]
		sDec, errDec := base64.StdEncoding.DecodeString(rawData)
		if errDec != nil {
			log.Println("Error:", errDec)
		}
		var mp mathProblem
		err := json.Unmarshal([]byte(sDec), &mp)
		if err != nil {
			log.Println("Error:", err)
			return
		}
		log.Printf("%v + %v = %v", mp.Num1, mp.Num2, mp.Num1+mp.Num2)
		return
	}
	log.Println("Error: os.Args was 1 length.")
}
