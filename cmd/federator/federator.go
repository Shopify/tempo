package main

import (
	app "github.com/grafana/tempo/cmd/federator/federator"
	"log"
)

func main() {
	if err := app.Run(); err != nil {
		log.Fatalln("error", err.Error())
	}
	log.Println("msg", "tempo-federator finished running")
}
