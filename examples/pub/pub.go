package main

import (
	"encoding/json"
	es "github.com/paullucas/eventstoreutil"
	"log"
	"os"
	"time"
)

var (
	eventsPub = make(chan []byte)
	closeChan = make(chan os.Signal, 1)
)

type eventData struct {
	Msg string `json:"msg"`
}

func pubLoop() {
	for {
		evt, err := json.Marshal(eventData{"text"})
		if err != nil {
			log.Panicf("Error marshalling event: %v", err)
		}

		select {
		case eventsPub <- evt:
		case <-closeChan:
			break
		}

		<-time.After(time.Duration(1000000) * time.Microsecond)
	}
}

func main() {
	gesConf := es.GESConfig{}
	es.ParseFlags(&gesConf)
	log.Printf("Config: %v", gesConf)

	go es.Pub(gesConf, "eventData", eventsPub, closeChan)
	pubLoop()
}
