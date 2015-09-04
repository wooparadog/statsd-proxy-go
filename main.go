package main

import (
	sender "github.com/wooparadog/statsd-proxy-go/sender"
	server "github.com/wooparadog/statsd-proxy-go/server"
	consistenthash "github.com/wooparadog/statsd-proxy-go/utils"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

type payload struct {
	data   []byte
	length int
}

var hash_ring *consistenthash.Map

const MAX_METRICS_LEN = 2048

func processMsg(payload []byte, srv server.Server, senders *sender.Senders) {
	var s sender.Sender
	if payload == nil {
		log.Println(payload)
		return
	}
	cursor := 0
	offset := 0
	overflow := false

	for n, b := range payload {
		if offset-cursor >= MAX_METRICS_LEN {
			log.Println("Overflow metrics")
			overflow = true
		}
		if b == 10 {
			if !overflow {
				s.Send(payload[offset:cursor])
			}
			offset = n + 1
			cursor = offset
			overflow = false
			continue
		}
		if overflow {
			continue
		}
		if b == 58 {
			host := hash_ring.Get(payload[offset:cursor])
			s = senders.Get(host)
		}
		cursor += 1
	}
	if cursor > offset {
		s.Send(payload[offset:cursor])
	}
}
func dispatcher(s server.Server, ss *sender.Senders) {
	msg_chan := s.GetOutChan()
	exit_chan := s.GetExitChan()

ioloop:
	for {
		select {
		case <-exit_chan:
			break ioloop
		case payload := <-msg_chan:
			go processMsg(payload, s, ss)
		}
	}
}

func main() {
	runtime.GOMAXPROCS(24)
	hash_ring = consistenthash.New(nil)
	hosts := []string{
		"127.0.0.1:8005",
		//"127.0.0.1:8006",
		//"10.0.12.101:8127",
		//"10.0.12.101:8128",
		//"10.0.12.101:8129",
		//"10.0.12.101:8130",
		//"10.0.12.101:8131",
		//"10.0.12.101:8132",
		//"10.0.12.101:8133",
		//"10.0.12.101:8134",
		//"10.0.12.101:8135",
		//"10.0.12.101:8136",
		//"10.0.12.101:8137",
		//"10.0.12.101:8138",
		//"10.0.12.101:8139",
		//"10.0.12.101:8140",
		//"10.0.12.101:8141",
		//"10.0.12.101:8142",
		//"10.0.12.101:8143",
		//"10.0.12.101:8144",
		//"10.0.12.101:8145",
		//"10.0.12.101:8146",
		//"10.0.12.101:8147",
		//"10.0.12.101:8148",
		//"10.0.12.101:8149",
		//"10.0.12.101:8150",
		//"10.0.12.101:8151",
		//"10.0.12.101:8152",
		//"10.0.12.101:8153",
		//"10.0.12.101:8154",
		//"10.0.12.101:8155",
		//"10.0.12.101:8156",
		//"10.0.12.101:8157",
		//"10.0.12.101:8158",
		//"10.0.12.101:8159",
		//"10.0.12.101:8160",
		//"10.0.12.101:8161",
		//"10.0.12.101:8162",
	}
	hash_ring.Populate(hosts...)

	senders := sender.NewSenders("udp", hosts...)
	server := server.NewServer("udp", &net.UDPAddr{Port: 8125, IP: net.IP{0, 0, 0, 0}})

	dispatcher(server, senders)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-signalChan
	log.Println("Got signal, gracefully shutting donw(wait 3sec)")
	server.Close()
	time.Sleep(3 * time.Second)
	senders.Close()
	log.Println("Quiting")
}
