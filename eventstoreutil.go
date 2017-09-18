// Package eventstoreutil contains utility functions for working with EventStore.
package eventstoreutil

import (
	"flag"
	ges "github.com/jdextraze/go-gesclient"
	gesClient "github.com/jdextraze/go-gesclient/client"
	"github.com/satori/go.uuid"
	"log"
	"net/url"
	"os"
	"strings"
	"time"
)

// GESConfig is for details related to EventStore
type GESConfig struct {
	Addr    string
	Stream  string
	User    string
	Pass    string
	Verbose bool
}

// ParseFlags is for initializing a GESConfig via CLI arguments
func ParseFlags(gesConf *GESConfig) {
	flag.StringVar(&gesConf.Addr, "gesEndpoint", "tcp://127.0.0.1:1113", "EventStore Address")
	flag.StringVar(&gesConf.Stream, "gesStream", "Default", "EventStore Stream ID")
	flag.StringVar(&gesConf.User, "gesUser", "admin", "EventStore Username")
	flag.StringVar(&gesConf.Pass, "gesPass", "changeit", "EventStore Password")
	flag.BoolVar(&gesConf.Verbose, "v", false, "Verbose Logging")
	flag.Parse()
}

func subscriptionDroppedHandler(s *gesClient.EventStoreSubscription, r gesClient.SubscriptionDropReason, err error) error {
	log.Printf("subscription dropped: %s, %v", r, err)
	return nil
}

// Sub subscribes to the all stream
func Sub(
	gesConf GESConfig,
	eventsSub chan<- []byte,
	closeChan <-chan os.Signal,
) {
	uri, err := url.Parse(gesConf.Addr)
	if err != nil {
		log.Fatalf("Sub: Error parsing address: %v", err)
	}

	conn, err := ges.Create(gesClient.DefaultConnectionSettings, uri, "AllSubscriber")
	if err != nil {
		log.Fatalf("Sub: Error creating connection: %v", err)
	}

	if err := conn.ConnectAsync().Wait(); err != nil {
		log.Fatalf("Sub: Error connecting: %v", err)
	}

	user := gesClient.NewUserCredentials(gesConf.User, gesConf.Pass)

	sub := &gesClient.EventStoreSubscription{}

	task, err := conn.SubscribeToAllAsync(
		true,
		func(s *gesClient.EventStoreSubscription, e *gesClient.ResolvedEvent) error {
			eventType := e.OriginalEvent().EventType()
			if !strings.HasPrefix(eventType, "$") {
				select {
				case eventsSub <- e.OriginalEvent().Data():
					if gesConf.Verbose {
						log.Printf("Event appeared! Type: %v", eventType)
					}
				}

			}
			return nil
		},
		subscriptionDroppedHandler,
		user,
	)
	if err != nil {
		log.Fatalf("Error occured while subscribing to stream: %v", err)
	} else if err := task.Result(sub); err != nil {
		log.Fatalf("Error occured while waiting for result of subscribing to stream: %v", err)
	} else {
		log.Printf("SubscribeToAll result: %v", sub)

		<-closeChan

		_ = sub.Close()
		time.Sleep(10 * time.Millisecond)
	}

	_ = conn.Close()
	time.Sleep(10 * time.Millisecond)
}

// Pub publishes events to a stream
func Pub(
	gesConf GESConfig,
	eventType string,
	eventsPub <-chan []byte,
	closeChan <-chan os.Signal,
) {
	uri, err := url.Parse(gesConf.Addr)
	if err != nil {
		log.Fatalf("Pub: Error parsing address: %v", err)
	}

	conn, err := ges.Create(gesClient.DefaultConnectionSettings, uri, "Publisher")
	if err != nil {
		log.Fatalf("Pub: Error creating connection: %v", err)
	}

	if err := conn.ConnectAsync().Wait(); err != nil {
		log.Fatalf("Pub: Error connecting: %v", err)
	}

	for {
		select {
		case event := <-eventsPub:
			if err != nil {
				log.Printf("Error occured while parsing event: %v", err)
			} else {
				evt := gesClient.NewEventData(uuid.NewV4(), eventType, true, event, nil)
				result := &gesClient.WriteResult{}

				task, err := conn.AppendToStreamAsync(gesConf.Stream, gesClient.ExpectedVersion_Any, []*gesClient.EventData{evt}, nil)
				if err != nil {
					log.Printf("Error occured while appending to stream: %v", err)
				} else if err := task.Result(result); err != nil {
					log.Printf("Error occured while waiting for result of appending to stream: %v", err)
				} else {
					log.Printf("AppendToStream result: %v", result)
				}
			}

		case <-closeChan:
			_ = conn.Close()
			time.Sleep(10 * time.Millisecond)
			return
		}
	}
}
