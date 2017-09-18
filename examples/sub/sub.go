package main

import (
	"encoding/json"
	es "github.com/paullucas/eventstoreutil"
	"log"
	"os"
)

var (
	eventsSub = make(chan []byte)
	closeChan = make(chan os.Signal, 1)
)

type eventData struct {
	Msg string `json:"msg"`
}

func subLoop() {
loop:
	for {
		select {
		case event := <-eventsSub:
			evtData := eventData{}
			err := json.Unmarshal(event, &evtData)
			if err != nil {
				log.Printf("Error unmarshalling event: %v", err)
				break
			}
			log.Printf("Data: %v", evtData)

		case <-closeChan:
			break loop
		}
	}
}

func main() {
	gesConf := es.GESConfig{}
	es.ParseFlags(&gesConf)
	log.Printf("Config: %v", gesConf)

	go es.Sub(gesConf, eventsSub, closeChan)
	subLoop()
}
