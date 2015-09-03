package main

import (
	sender "github.com/wooparadog/statsd-proxy/sender"
	server "github.com/wooparadog/statsd-proxy/server"
	consistenthash "github.com/wooparadog/statsd-proxy/utils"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type payload struct {
	data   []byte
	length int
}

var hash_ring *consistenthash.Map

const MAX_METRICS_LEN = 2048

func processMsg(id int, srv server.Server, senders *sender.Senders) {
	metrics := make([]byte, MAX_METRICS_LEN)
	var s sender.Sender
	var cursor int
	var overflow bool

	msg_chan := srv.GetOutChan()
	exit_chan := srv.GetExitChan()

ioloop:
	for {
		select {
		case <-exit_chan:
			log.Println("Closing worker: ", id)
			break ioloop
		case payload := <-msg_chan:
			if payload == nil {
				log.Println(payload)
				continue
			}
			cursor = 0
			for _, b := range payload {
				if cursor >= MAX_METRICS_LEN {
					log.Println("Overflow metrics")
					overflow = true
				}
				if b == 10 {
					if !overflow {
						s.Send(metrics[:cursor])
					}
					cursor = 0
					overflow = false
					continue
				}
				if overflow {
					continue
				}
				if b == 58 {
					host := hash_ring.Get(metrics[:cursor])
					s = senders.Get(host)
				}
				metrics[cursor] = b
				cursor += 1
			}
			if cursor > 0 {
				s.Send(metrics[:cursor])
			}
		}
	}
}

func ProcessMsgs(s server.Server, ss *sender.Senders) {
	for i := 0; i < 10; i++ {
		go processMsg(i, s, ss)
	}
}

func main() {
	hash_ring = consistenthash.New(nil)
	hosts := []string{
		"127.0.0.1:8001",
		//"127.0.0.1:8002",
		//"127.0.0.1:8003",
		//"127.0.0.1:8004",
		//"127.0.0.1:8005",
		//"127.0.0.1:8006",
		//"127.0.0.1:8007",
		//"127.0.0.1:8008",
	}
	hash_ring.Populate(hosts...)

	senders := sender.NewSenders("udp", hosts...)
	server := server.NewServer("udp", &net.UDPAddr{Port: 8000, IP: net.IP{127, 0, 0, 1}})

	ProcessMsgs(server, senders)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-signalChan
	log.Println("Got signal, gracefully shutting donw(wait 3sec)")
	server.Close()
	time.Sleep(3 * time.Second)
	senders.Close()
	log.Println("Quiting")
}
