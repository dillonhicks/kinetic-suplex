package kintaro

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"kineticsuplex/kintaro/constants"
)

const (
	StreamName   = "StreamName"
	PartitionKey = "PartitionKey"
)


type KintaroCacheServer struct {
	upgrader          websocket.Upgrader
	connectionManager CacheManager
}


func NewKintaroCacheServer() KintaroCacheServer {
	return KintaroCacheServer{
		upgrader:          websocket.Upgrader{},
		connectionManager: NewCacheManager(),
	}
}


func (k *KintaroCacheServer) Run(addr string, port uint) {
	r := mux.NewRouter()

	r.HandleFunc("/ws/{"+StreamName+"}/{"+PartitionKey+"}", k.kinesisWebsocketStreamingBufferCacheHandler)
	k.connectionManager.Start()
	log.Fatal(http.ListenAndServe(fmt.Sprintf("%s:%v", addr, port), r))
}


/////////////////
// Handlers
/////////////////


// Create a websocket that will subscribe to the kinatro cache. Updates will be pushed on a regular interval as they
// added to the cache.
func (k *KintaroCacheServer) kinesisWebsocketStreamingBufferCacheHandler(w http.ResponseWriter, r *http.Request) {
	v := mux.Vars(r)
	partitionKey := v[PartitionKey]
	streamName := v[StreamName]
	
	fmt.Println(fmt.Sprintf("[ws:%v:new] new connection", partitionKey))

	ws, err := k.upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println(fmt.Sprintf("[ws:%v:new] error: %v", partitionKey, err))
		return
	}

	defer ws.Close()

	ws.WriteMessage(websocket.TextMessage, []byte("Start websocket"))
	ws.WriteMessage(websocket.TextMessage, []byte("Waiting connect"))
	fmt.Println(fmt.Sprintf("[ws:%v:new] request connect from gatekeeper", partitionKey))
	future, err := k.connectionManager.Connect(streamName, partitionKey)

	if err != nil {
		ws.WriteMessage(websocket.TextMessage, []byte("error"))
		if err != nil {
			fmt.Println(fmt.Sprintf("[ws:%v:new] error: %v", partitionKey, err))
		}
		return
	}

	ws.WriteMessage(websocket.TextMessage, []byte("waiting result"))
	result := future.WaitForResult()
	if result.IsError() {
		err = ws.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("Error: %v", result.Error())))
		if err != nil {
			fmt.Println(fmt.Sprintf("[ws:%v:new] error: %v", partitionKey, err))
		}
		return
	}

	connection := result.Connection()

	ws.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("waiting data from %v", connection)))
	fmt.Println(fmt.Sprintf("[ws:%v:%v] connection established", partitionKey, connection.Id))

	done := false
	for !done {
		select {
		case msg := <-connection.DataChan:
			if msg != "" {
				connection.Timestamp = time.Now()

				err = ws.WriteMessage(websocket.TextMessage, []byte(msg))
				if err != nil {
					connection.State = constants.ConnectionStateClosed
					fmt.Println(fmt.Sprintf("[ws:%v:%v] error: %v", partitionKey, connection.Id, err))
					done = true
				}
			}
		default:
			time.Sleep(constants.WebSocketWaitTimeBetweenPolls)
		}
	}

	k.connectionManager.Disconnect(partitionKey, connection.Id)
	if err != nil {
		ws.WriteMessage(websocket.TextMessage, []byte("error on disconnect"))
		if err != nil {
			fmt.Println(fmt.Sprintf("[ws:%v:new] error: %v", partitionKey, err))
		}
		return
	}

	fmt.Println(fmt.Sprintf("[ws:%v:%v] done", partitionKey, connection.Id))
	future.WaitForResult()
}

