package main

import (
	"bytes"
	sender "github.com/wooparadog/statsd-proxy-go/sender"
	server "github.com/wooparadog/statsd-proxy-go/server"
	consistenthash "github.com/wooparadog/statsd-proxy-go/utils"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

type payload struct {
	data   []byte
	length int
}

var hash_ring *consistenthash.Map

const MAX_METRICS_LEN = 2048

func processMsg(payload []byte, srv server.Server, senders *sender.Senders) {
	var s sender.Sender
	var host string
	if payload == nil {
		log.Println(payload)
		return
	}

	members := hash_ring.Members()
	hash := make(map[string]*bytes.Buffer, len(members))

	for _, v := range members {
		hash[v] = new(bytes.Buffer)
	}

	cursor := 0
	offset := 0
	sep := []byte("\n")

	for n, b := range payload {
		if b == '\n' || b == 0 {
			if host == "" {
				log.Println("!!!!")
			}

			size := cursor - offset
			// check built packet size and send if metric doesn't fit
			if hash[host].Len()+size > 1000 {
				s = senders.Get(host)
				s.Send(hash[host].Bytes())
				hash[host].Reset()
			}
			// add to packet
			hash[host].Write(payload[offset:cursor])
			hash[host].Write(sep)
			offset = n + 1
			cursor = offset
			continue
		}
		if b == 58 {
			host = hash_ring.Get(payload[offset:cursor])
		}
		cursor += 1
	}
	if cursor > offset {
		hash[host].Write(payload[offset:cursor])
		hash[host].Write(sep)
	}

	// Empty out any remaining data
	for server, buff := range hash {
		if buff.Len() > 0 {
			s = senders.Get(server)
			s.Send(buff.Bytes())
		}
	}
}

func dispatcher(s server.Server, ss *sender.Senders) {
	msg_chan := s.GetOutChan()
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

ioloop:
	for {
		select {
		case <-signalChan:
			log.Println("Got signal, shutting donw")
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
		//"127.0.0.1:8005",
		//"127.0.0.1:8006",
		"10.0.12.101:8127",
		"10.0.12.101:8128",
		"10.0.12.101:8129",
		"10.0.12.101:8130",
		"10.0.12.101:8131",
		"10.0.12.101:8132",
		"10.0.12.101:8133",
		"10.0.12.101:8134",
		"10.0.12.101:8135",
		"10.0.12.101:8136",
		"10.0.12.101:8137",
		"10.0.12.101:8138",
		"10.0.12.101:8139",
		"10.0.12.101:8140",
		"10.0.12.101:8141",
		"10.0.12.101:8142",
		"10.0.12.101:8143",
		"10.0.12.101:8144",
		"10.0.12.101:8145",
		"10.0.12.101:8146",
		"10.0.12.101:8147",
		"10.0.12.101:8148",
		"10.0.12.101:8149",
		"10.0.12.101:8150",
		"10.0.12.101:8151",
		"10.0.12.101:8152",
		"10.0.12.101:8153",
		"10.0.12.101:8154",
		"10.0.12.101:8155",
		"10.0.12.101:8156",
		"10.0.12.101:8157",
		"10.0.12.101:8158",
		"10.0.12.101:8159",
		"10.0.12.101:8160",
		"10.0.12.101:8161",
		"10.0.12.101:8162",
	}
	hash_ring.Populate(hosts...)

	senders := sender.NewSenders("udp", hosts...)
	server := server.NewServer("udp", &net.UDPAddr{Port: 8125, IP: net.IP{0, 0, 0, 0}})
	dispatcher(server, senders)
	log.Println("Quiting")
}
