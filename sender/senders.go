package sender

import (
	"log"
	"net"
)

type Senders struct {
	senders map[string]Sender
}

func (senders *Senders) Get(name string) Sender {
	val, ok := senders.senders[name]
	if !ok {
		log.Fatalln("Fail to get sender: ", name)
	}
	return val
}

func NewSenders(kind string, hosts ...string) *Senders {
	senders := &Senders{
		senders: make(map[string]Sender),
	}
	var senderClass func(backend_addr *net.UDPAddr, admin_addr *net.TCPAddr) Sender

	if kind == "udp" {
		senderClass = NewUdpSender
	} else {
		log.Fatalln("Illegal kind, should be `udp` or ...", kind)
	}

	for _, host := range hosts {
		udp_addr, err := net.ResolveUDPAddr("udp4", host)
		if err != nil {
			log.Fatal(err)
		}
		tcp_addr, err := net.ResolveTCPAddr("tcp4", host)
		if err != nil {
			log.Fatal(err)
		}
		senders.senders[host] = senderClass(udp_addr, tcp_addr)
	}
	return senders
}

func (senders *Senders) Close() {
	for host, sender := range senders.senders {
		sender.Close()
		log.Println("Closing Handler: ", host)
	}
}
