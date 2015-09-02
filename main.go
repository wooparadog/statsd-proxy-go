package main

import (
	"fmt"
	"net"
)

type payload struct {
	data   []byte
	length int
}

var msg_chan chan *payload
var hash_ring *Map

func processMsg(n int) {
	for {
		select {
		case payload := <-msg_chan:
			fmt.Println("worker:", n, payload.data[:payload.length])
			fmt.Println("Hashed to", hash_ring.Get(payload.data))
		}
	}
}

func IOLoop(conn *net.UDPConn, outbuffer chan *payload) {
	buffer := make([]byte, 2048)
	for {
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println("Error in loop:", err)
			continue
		}
		outbuffer <- &payload{data: buffer, length: n}
	}
}

func main() {
	msg_chan = make(chan *payload)
	event_chan := make(chan int)
	hash_ring = New(nil)
	hash_ring.Populate(
		"127.0.0.1:8001",
		"127.0.0.1:8002",
		"127.0.0.1:8003",
		"127.0.0.1:8004",
		"127.0.0.1:8005",
		"127.0.0.1:8006",
		"127.0.0.1:8007",
		"127.0.0.1:8008",
	)
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{Port: 8000, IP: net.IP{127, 0, 0, 1}})
	if err != nil {
		fmt.Print(err)
	}

	go IOLoop(conn, msg_chan)
	go processMsg(1)
	go processMsg(2)
	go processMsg(3)
	<-event_chan
	fmt.Println("Quiting")
}
