package kintaro

import (
	"fmt"
	"sync"
	"time"

	"kineticsuplex/goro"
	"kineticsuplex/kintaro/constants"
	"bytes"
	"errors"
)


const (
	Active        = iota
	NoConnections
)


type StreamBufferCache struct {
	Lock            sync.Mutex
	partitionKey    string
	buffer          [][]byte
	Connections     []*Connection
	lastDcTimestamp time.Time
	deleteChan      chan uint64
	state           int
	sequenceNumber  uint64
	IdCounter       uint64
}


func NewStreamBufferCache(partitionKey string) *StreamBufferCache {
	logCache := StreamBufferCache{
		Lock:            sync.Mutex{},
		partitionKey:    partitionKey,
		buffer:          [][]byte{},
		Connections:     []*Connection{},
		lastDcTimestamp: time.Now(),
		deleteChan:      make(chan uint64),
		state:           constants.StreamBufferCacheStateInitializing,
		sequenceNumber:  0,
		IdCounter:       0,
	}

	return &logCache
}


func (s *StreamBufferCache) sendToAll(msg []byte) {
	syncMessage := []byte("")

	for _, conn := range s.Connections {
		if conn.State == constants.ConnectionStateNew {
			if len(syncMessage) == 0{
				syncMessage = bytes.Join(s.buffer, []byte("\n"))
			}

			conn.DataChan <- string(syncMessage)

			conn.State = constants.ConnectionStateNominal
		} else if conn.State == constants.ConnectionStateNominal {
			conn.DataChan <- string(msg)
		}
	}
}


// Append the new records to the cache
//
func (s *StreamBufferCache) record(msg [][]byte) {
	if len(msg) == 0 {
		fmt.Println(fmt.Sprintf("Not recording empty message"))
		return
	}

	s.buffer = append(s.buffer, msg...)
}


func (s *StreamBufferCache) runEventLoop() {
	fmt.Println(fmt.Sprintf("[cache:%v] Start event loop", s.partitionKey))

	istreamChannel := goro.GetOrCreateChannel(s.partitionKey)
	fmt.Println(fmt.Sprintf("[cache:%v] obtained receiver ", s.partitionKey))

	stats := newStats()
	nextSend := time.Now().Add(constants.BufferCacheMaxWaitBetweenSends)
	collector := make([][]byte, 0, constants.BufferCacheMaxEntriesPerSend)

	shouldSendBatch := func(now *time.Time) bool {
		return len(collector) != 0 && (len(collector) == constants.BufferCacheMaxEntriesPerSend || now.After(nextSend))
	}

	ticker := time.NewTicker(1 * time.Nanosecond)

	for s.state == constants.StreamBufferCacheStateRunning {
		now := <-ticker.C

		select {
		case msg := <-istreamChannel:
			s.sequenceNumber++
			msg = fmt.Sprintf("[cache:%v] [%.2f rps] %v %v %v",
				s.partitionKey, stats.CurrentRps(), s.sequenceNumber, time.Now(), msg)

			if now.After(nextSend) {
				fmt.Println(fmt.Sprintf("[cache:%v] [%.2f rps] [%v conns] tick: %v",
					s.partitionKey, stats.CurrentRps(), len(s.Connections), s.sequenceNumber))
			}

			collector = append(collector, []byte(msg))

		default:
			time.Sleep(0)  // yield
		}

		if shouldSendBatch(&now) {
			s.record(collector)
			s.sendToAll(bytes.Join(collector, []byte("\n")))
			s.reapConnections()
			s.trimBuffer()
			collector = collector[:0]
			nextSend = now.Add(constants.BufferCacheMaxWaitBetweenSends)
		}

		stats.Update(s.sequenceNumber)
	}

	fmt.Println(fmt.Sprintf("[cache:%v] done", s.partitionKey))
	s.state = constants.StreamBufferCacheStateTerminated
}


func (s *StreamBufferCache) trimBuffer() {
	if len(s.buffer) >= constants.BufferCacheSize {
		s.buffer = s.buffer[0:constants.BufferCacheSize]
	}
}

// Remove connections if they are are in the delete channel (queue).
//
func (s *StreamBufferCache) reapConnections() {

	missing := -1
	removeIdx := 0
	for removeIdx > missing {
		removeIdx = missing

		select {
		case id := <-s.deleteChan:
			for idx, conn := range s.Connections {
				if conn.Id == id {
					removeIdx = idx
					break
				}
			}

			if removeIdx == missing {
				fmt.Println(fmt.Sprintf("[cache:%v] Could not find connection id to remove %v", s.partitionKey, id))
			}
		default:
			// pass
		}

		if removeIdx > missing {
			s.Lock.Lock()
			i := removeIdx
			conn := s.Connections[i]
			fmt.Println(fmt.Sprintf("[cache:%v] Reaping connection %v", s.partitionKey, conn.Id))

			s.Connections[i] = nil
			s.Connections = append(s.Connections[:i], s.Connections[i+1:]...)
			s.Lock.Unlock()

			s.lastDcTimestamp = time.Now()
		}
	}
}


func (s *StreamBufferCache) Status() int {
	if len(s.Connections) == 0 {
		return NoConnections
	}

	return Active
}


func (s *StreamBufferCache) TimeSinceLastDisconnect() time.Duration {
	return time.Duration(time.Now().UnixNano() - s.lastDcTimestamp.UnixNano())
}


func (s *StreamBufferCache) Stop() {
	if s.state == constants.StreamBufferCacheStateTerminated {
		fmt.Println(fmt.Sprintf("[cache:%v] already stopped", s.partitionKey))
		return
	}

	fmt.Println(fmt.Sprintf("[cache:%v] stopping event loop", s.partitionKey))
	s.state = constants.StreamBufferCacheStateStopping
	for s.state != constants.StreamBufferCacheStateTerminated {
		time.Sleep(1 * time.Microsecond)
	}
}


func (s *StreamBufferCache) IsPolling() bool {
	return s.state == constants.StreamBufferCacheStateRunning
}


// Begin the cache's mainloop that pools the source input channel and
// manages the buffer of current entries as a slice backed "ring buffer".
//
func (s *StreamBufferCache) StartPolling() {
	if s.state != constants.StreamBufferCacheStateInitializing {
		fmt.Println(fmt.Sprintf("Already started 1"))
		return
	}

	s.Lock.Lock()
	if s.state != constants.StreamBufferCacheStateInitializing {
		fmt.Println(fmt.Sprintf("Already started 2"))
		s.Lock.Unlock()
		return
	}

	s.state = constants.StreamBufferCacheStateRunning
	s.Lock.Unlock()
	go s.runEventLoop()
}


func (s *StreamBufferCache) NewConnection() Result {

	if len(s.Connections) == constants.BufferCacheMaxConnectionCount {
		return Error(errors.New("Too many Connections "))
	}

	s.Lock.Lock()

	conn := Connection{
		Id:        s.IdCounter,
		DataChan:  make(chan string),
		Timestamp: time.Now(),
		State:     constants.ConnectionStateNew,
	}

	s.IdCounter++
	s.Connections = append(s.Connections, &conn)
	s.Lock.Unlock()

	return Ok(&conn)
}

// Add a connection id to the delete queue
func (s *StreamBufferCache) DeleteConnection(connectionId uint64) {
	s.deleteChan <- connectionId
}
