package server

import (
	"log"
	"net"
)

type Server interface {
	IOLoop()
	Close()
	GetOutChan() chan []byte
	GetExitChan() chan int
}

func NewServer(kind string, addr net.Addr) Server {
	var s Server
	if kind == "udp" {
		s = NewUdpServer(addr.(*net.UDPAddr))
	} else {
		log.Fatal("Wrong server type, should be `udp` or ...: ", kind)
	}
	return s
}
