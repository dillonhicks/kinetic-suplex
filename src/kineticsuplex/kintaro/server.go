package kintaro

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"kineticsuplex/kintaro/constants"
	"errors"
	"strings"
	"kineticsuplex/goro"
	"sync/atomic"
)

const (
	StreamName   = "StreamName"
	Keys = "keys"
)


type CollectorComs struct {
	dataChan chan []byte
	ctrlChan chan int
	ackChan chan int
}

func newCollectorComs() CollectorComs {
	return CollectorComs {
		dataChan: make(chan []byte),
		ctrlChan: make(chan int),
		ackChan: make(chan int),
	}
}


type KintaroCacheServer struct {
	upgrader          websocket.Upgrader
	connectionManager CacheManager
	state int64
	connectionCount int64
}


func NewKintaroCacheServer() KintaroCacheServer {
	return KintaroCacheServer{
		upgrader:          websocket.Upgrader{},
		connectionManager: NewCacheManager(),
		state: constants.ServerStateInitializing,
	}
}


func (k *KintaroCacheServer) Run(addr string, port uint) {
	r := mux.NewRouter()

	r.Path("/ws/{"+StreamName+"}").
		Queries(Keys, "{"+Keys+"}").
		HandlerFunc( k.kinesisWebsocketStreamingBufferCacheHandler)


	k.connectionManager.Start()
	k.state = constants.ServerStateRunning
	log.Fatal(http.ListenAndServe(fmt.Sprintf("%s:%v", addr, port), r))
}

func (k *KintaroCacheServer) Shutdown() bool {
	fmt.Println(fmt.Sprintf("[KINTARO] Stopping ...."))
	for !atomic.CompareAndSwapInt64(&k.state, constants.ServerStateRunning, constants.ServerStateStopping){
		time.Sleep(1 * time.Second)
		fmt.Println(fmt.Sprintf("[KINTARO] Stopping ...."))
	}
	return true
}


func (k *KintaroCacheServer) Wait() {
	deadline := time.Now().Add(5 * time.Second)
	for k.connectionCount > 0 && !time.Now().After(deadline){
		fmt.Println(fmt.Sprintf("[KINTARO] there are still %v active connections....", k.connectionCount))
		time.Sleep(1 * time.Second)
	}
	fmt.Println(fmt.Sprintf("[KINTARO] [DONE ]"))
}

func (k *KintaroCacheServer) incConnectionCount() {
	atomic.AddInt64(&k.connectionCount, 1)
}

func (k *KintaroCacheServer) decConnectionCount() {
	atomic.AddInt64(&k.connectionCount, -1)
}

func (k *KintaroCacheServer) runCollector(streamName string, partitionKey string, collector CollectorComs) {
	// fmt.Println(fmt.Sprintf("[ws:%v:new] new connection", partitionKey))
	// fmt.Println(fmt.Sprintf("[ws:%v:new] request connect from gatekeeper", partitionKey))

	future, err := k.connectionManager.Connect(streamName, partitionKey)

	if err != nil {
		collector.dataChan <- []byte("error")
		collector.ackChan <- 1
		return
	}

	collector.dataChan <- []byte("waiting result")
	result := future.WaitForResult()
	if result.IsError() {
		collector.dataChan <- []byte(fmt.Sprintf("Error: %v", result.Error()))
		collector.ackChan <- 1
		return
	}

	connection := result.Connection()

	collector.dataChan <- []byte(fmt.Sprintf("waiting data from %v", connection))
	// fmt.Println(fmt.Sprintf("[ws:%v:%v] connection established", partitionKey, connection.Id))

	done := false
	for !done {
		select {
		case msg := <-connection.DataChan:
			if msg == goro.TaskletDoneSentinel {
				connection.Timestamp = time.Now()
				err = errors.New("the tasklet for " + partitionKey + " reported an earlier than expected done status, this is a likely error")
				done = true
			} else if msg != "" {
				connection.Timestamp = time.Now()
				collector.dataChan <- []byte(msg)
			}
		case <- collector.ctrlChan:
			done = true
		default:
			time.Sleep(constants.WebSocketWaitTimeBetweenPolls)
		}

		if k.state == constants.ServerStateStopping {
			done = true
			fmt.Println(fmt.Sprintf("[worker] Stopping %v", *connection))
		}
	}

	k.connectionManager.Disconnect(partitionKey, connection.Id)
	if err != nil {
		collector.dataChan <- []byte(err.Error())
		collector.ackChan <- 1
		return
	}

	fmt.Println(fmt.Sprintf("[ws:%v:%v] done", partitionKey, connection.Id))
	collector.ackChan <- 1
	future.WaitForResult()
}

/////////////////
// Handlers
/////////////////


// Create a websocket that will subscribe to the kinatro cache. Updates will be pushed on a regular interval as they
// added to the cache.
func (k *KintaroCacheServer) kinesisWebsocketStreamingBufferCacheHandler(w http.ResponseWriter, r *http.Request) {
	k.incConnectionCount()
	defer k.decConnectionCount()
	if k.state == constants.ServerStateStopping {
		http.Error(w, "The server is stopping", 500)
		return
	}

	v := mux.Vars(r)
	streamName := v[StreamName]
	keys := strings.Split(v[Keys], ",")

	ws, err := k.upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println(fmt.Sprintf("[ws:%v:new] error: %v", streamName, err))
		return
	}
	defer ws.Close()

	ws.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("Connecting..., %v %v", keys, v)))
	taskCount := 0
	doneCount := 0
	coms := newCollectorComs()

	for _, key := range keys {
		taskCount ++
		go k.runCollector(streamName, key, coms)
	}

	err = nil

	for err == nil {
		select {
		case <-coms.ackChan:
			doneCount++
			fmt.Println(fmt.Sprintf("[ws:%v] received ack before ctrlmessage sent", streamName))
		case msg := <-coms.dataChan:
			err = ws.WriteMessage(websocket.TextMessage, msg)
		default:
			time.Sleep(constants.WebSocketWaitTimeBetweenPolls)
		}
		if doneCount == taskCount {
			err = errors.New("all collector tasks exited")
		}

	}

	ws.WriteMessage(websocket.CloseNormalClosure, []byte("Goodbye!"))

	for i := doneCount; i < taskCount; i++ {
		coms.ctrlChan <- 1
	}

	for doneCount < taskCount {
		select {
		case <-coms.ackChan:
			doneCount++
		default:
			time.Sleep(constants.WebSocketWaitTimeBetweenPolls)
		}
	}

	fmt.Println(fmt.Sprintf("[WEBSOCK] [CLOSE]"))
	time.Sleep(500 * time.Millisecond)
}

