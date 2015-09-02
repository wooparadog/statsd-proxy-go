package main

import (
	"fmt"
	sender "github.com/wooparadog/statsd-proxy/sender"
	consistenthash "github.com/wooparadog/statsd-proxy/utils"
	"log"
	"net"
)

type payload struct {
	data   []byte
	length int
}

var msg_chan chan *payload
var hash_ring *consistenthash.Map
var sender_hash map[string]sender.Sender

func processMsg(n int) {
	for {
		select {
		case payload := <-msg_chan:
			full_data := payload.data[:payload.length]
			fmt.Println("worker:", n, full_data)
			host := hash_ring.Get(payload.data)
			fmt.Println("Hashed to", host)
			sender_hash[host].Send(full_data)
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

func initSenders(hosts ...string) error {
	sender_hash = make(map[string]sender.Sender)
	for _, host := range hosts {
		udp_addr, err := net.ResolveUDPAddr("udp4", host)
		if err != nil {
			log.Fatal(err)
		}
		tcp_addr, err := net.ResolveTCPAddr("tcp4", host)
		if err != nil {
			log.Fatal(err)
		}
		sender_hash[host] = sender.NewUdpSender(udp_addr, tcp_addr)
	}
	return nil
}

func main() {
	msg_chan = make(chan *payload)
	event_chan := make(chan int)
	hash_ring = consistenthash.New(nil)
	hosts := []string{
		"127.0.0.1:8001",
		"127.0.0.1:8002",
		"127.0.0.1:8003",
		"127.0.0.1:8004",
		"127.0.0.1:8005",
		"127.0.0.1:8006",
		"127.0.0.1:8007",
		"127.0.0.1:8008",
	}
	hash_ring.Populate(hosts...)
	initSenders(hosts...)

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{Port: 8000, IP: net.IP{127, 0, 0, 1}})
	if err != nil {
		fmt.Print(err)
	}

	go IOLoop(conn, msg_chan)
	for i := 0; i < 10; i++ {
		go processMsg(1)
		go processMsg(2)
		go processMsg(3)
	}
	<-event_chan
	fmt.Println("Quiting")
}
