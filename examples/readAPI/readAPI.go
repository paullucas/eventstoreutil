package main

import (
	"encoding/json"
	es "github.com/paullucas/eventstoreutil"
	"log"
	"net/http"
	"os"
	"time"
)

var (
	eventsSub = make(chan []byte)
	closeChan = make(chan os.Signal, 1)
	state     = make([]eventData, 0)
)

type eventData struct {
	Msg string `json:"msg"`
}

func rootHandler(res http.ResponseWriter, req *http.Request) {
	if err := json.NewEncoder(res).Encode(state); err != nil {
		panic(err)
	}

	log.Printf("%s\t%s\t%s", req.Method, req.RequestURI, time.Since(time.Now()))
}

func api() {
	http.HandleFunc("/", rootHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
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
			state = append(state, evtData)

		case <-closeChan:
			break loop
		}
	}
}

func main() {
	gesConf := es.GESConfig{}
	es.ParseFlags(&gesConf)
	log.Printf("Config: %v", gesConf)

	go api()
	go es.Sub(gesConf, eventsSub, closeChan)
	subLoop()
}
