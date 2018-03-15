package kintaro

import (
	"fmt"
)


type MessageType int


type Result struct {
	connection *Connection
	err        error
}

func (r *Result) Connection() *Connection {
	return r.connection
}

func (r *Result) IsError() bool {
	return r.err != nil
}

func (r *Result) Error() error {
	return r.err
}


type ConnectRequest struct {
	StreamName   string
	PartitionKey string
}

type DisconnectRequest struct {
	PartitionKey string
	ConnectionId uint64
}


type Future struct {
	hasResponded  bool
	resultChannel chan Result
}



type Request struct {
	future  Future
	payload interface{}
}


func NewConnectRequest(streamName string, partitionKey string) Request {
	return Request{
		newFuture(),
		ConnectRequest{streamName, partitionKey}}
}

func NewDisconnectRequest(partitionKey string, connectionId uint64) Request {
	return Request{
		newFuture(),
		DisconnectRequest{partitionKey, connectionId}}
}


func (r *Request) RespondWith(result Result) {
	r.future.Set(result)
}

func (r *Request) Future() *Future {
	return &r.future
}

func newFuture() Future {
	return Future{
		false,
		make(chan Result, 1),
	}
}


func (r *Request) Payload() interface{} {
	return r.payload
}

func (f *Future) Set(result Result) {
	if f.hasResponded {
		panic("[Future] Error already set")
	}

	f.hasResponded = true
	f.resultChannel <- result
	fmt.Println("[Future] Set success")
}

func (f *Future) WaitForResult() Result {
	return <-f.resultChannel
}

func Ok(connection *Connection) Result {
	return Result{connection, nil}
}

func Error(err error) Result {
	return Result{nil, err}
}
