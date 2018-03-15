package main

import (
	"flag"
	"log"
	"net/url"
	"os"
	"os/signal"
	"time"

	"github.com/gorilla/websocket"
	"errors"
)

var addr = flag.String("addr", "localhost:8000", "http service address")


// taken from the github.com/gorilla/websocket client example
func main() {
	flag.Parse()
	log.SetFlags(0)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	streamName := "STREAMNAMENOTSET"
	partitionKey := "PARTITIONKEYNOTSET"

	if len(os.Args) > 1 {
		streamName = os.Args[1]
		partitionKey = os.Args[2]
	} else {
		panic(errors.New("Not enough arguements"))
	}

	u := url.URL{Scheme: "ws", Host: *addr, Path: "/ws/" + streamName +"/" + partitionKey}
	log.Printf("connecting to %s", u.String())

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()

	done := make(chan struct{})
	var counter uint64 = 0

	go func() {
		defer close(done)
		for {
			_, message, err := c.ReadMessage()
			counter++
			if err != nil {
				log.Println("read:", err)
				return
			}
			log.Printf("(%v) recv: %s", counter, message)
		}
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			continue
		case <-interrupt:
			log.Println("interrupt")

			// Cleanly close the connection by sending a close message and then
			// waiting (with timeout) for the server to close the connection.
			err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				log.Println("write close:", err)
				return
			}
			select {
			case <-done:
			case <-time.After(time.Second):
			}
			return
		}
	}
}
