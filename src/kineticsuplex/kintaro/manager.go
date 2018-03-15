package kintaro

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"kineticsuplex/goro"
	"kineticsuplex/kintaro/constants"
)


type CacheManager struct {
	lock        sync.RWMutex
	ctrlChan    chan *Request
	bufferCache map[string]*StreamBufferCache
	state       int
}



func NewCacheManager() CacheManager {
	return CacheManager{
		lock:        sync.RWMutex{},
		ctrlChan:    make(chan *Request),
		bufferCache: make(map[string]*StreamBufferCache),
		state:       constants.CacheManagerStateInitializing,
	}
}


func (c *CacheManager) Start() {
	if c.state != constants.CacheManagerStateInitializing{
		return
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	if c.state != constants.CacheManagerStateInitializing {
		return
	}

	c.state = constants.CacheManagerStateRunning
	go c.eventLoop()
}


func (c *CacheManager) Connect(streamName string, partitionKey string) (*Future, error) {
	request := NewConnectRequest(streamName, partitionKey)
	fmt.Println("[gk] sending request")
	c.ctrlChan <- &request
	fmt.Println("[gk] returning future ")
	return request.Future(), nil
}


func (c *CacheManager) Disconnect(partitionKey string, connectionId uint64) (*Future, error) {
	request := NewDisconnectRequest(partitionKey, connectionId)
	fmt.Println("[gk] sending request")
	c.ctrlChan <- &request
	fmt.Println("[gk] returning future ")
	return request.Future(), nil
}



func (c *CacheManager) eventLoop() {
	fmt.Println("[gk] starting")
	defer close(c.ctrlChan)

	lastStalenessCheck := time.Now()

	for c.state == constants.CacheManagerStateRunning {

		fmt.Println("[gk] waiting")

		request := <-c.ctrlChan

		switch msg := request.Payload().(type) {
		case ConnectRequest:
			fmt.Println("[gk] connect")
			result := c.newConnection(msg.StreamName, msg.PartitionKey)
			request.RespondWith(result)
		case DisconnectRequest:
			fmt.Println("[gk] disconnect")
			result := c.deleteConnection(msg.PartitionKey, msg.ConnectionId)
			request.RespondWith(result)
		default:
			errMsg := fmt.Sprintf("[gk] unknown message type: %T", msg)
			request.RespondWith(Error(errors.New(errMsg)))
		}

		now := time.Now()
		delta := time.Duration(now.UnixNano() - lastStalenessCheck.UnixNano())
		if delta >= constants.BufferCacheStalenessCheckInterval {

			c.reapStateCaches()
			lastStalenessCheck = now
		}

		time.Sleep(1 * time.Millisecond)
	}
}

func (c *CacheManager) reapStateCaches() {
	fmt.Println("[gk] reaping stale caches")

	for partitionKey, kache := range c.bufferCache {
		switch  kache.Status() {
		case Active:
			continue
		case NoConnections:
			if kache.TimeSinceLastDisconnect() >= constants.BufferCacheInactivityGracePeriod {
				kache.Stop()
				fmt.Println(fmt.Sprintf("[gk] reaping cache `%v`", partitionKey))
				delete(c.bufferCache, partitionKey)
			}
		default:
			panic("unknown cache state")
		}
	}
}



func (c *CacheManager) deleteConnection(partitionKey string, connectionId uint64) Result {
	if bufferCache, ok := c.bufferCache[partitionKey]; ok {
		bufferCache.DeleteConnection(connectionId)
		return Ok(nil)
	}

	return Error(errors.New(fmt.Sprintf("No such application id %v", connectionId)))
}



func (c *CacheManager) newConnection(streamName string, partitionKey string) Result {
	_, exists := c.bufferCache[partitionKey]
	if !exists {
		if len(c.bufferCache) == constants.BufferCachesMaxInstances {
			return Error(errors.New("[gk] Too many app buffer streams"))
		}

		fmt.Println(fmt.Sprintf("[%v] Created", partitionKey))
		c.bufferCache[partitionKey] = NewStreamBufferCache(partitionKey)
		goro.StartStreamCollectorTask(streamName)

	} else {
		fmt.Println(fmt.Sprintf("[%v] Exists ", partitionKey))
	}

	result := c.bufferCache[partitionKey].NewConnection()

	if result.IsError() {
		fmt.Println(fmt.Sprintf("[%v] Error creating connection: %v", partitionKey, result.Error()))
		return result;
	}

	if !c.bufferCache[partitionKey].IsPolling() {
		c.bufferCache[partitionKey].StartPolling()
	}

	fmt.Println(fmt.Sprintf("[%v] Connetions: %v", partitionKey, len(c.bufferCache[partitionKey].Connections)))
	return result
}

