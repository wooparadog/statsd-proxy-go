package server

import (
	"log"
	"net"
)

type Server interface {
	IOLoop()
	Close()
	GetOutChan() chan []byte
}

func NewServer(kind string, addr *net.UDPAddr) Server {
	var s Server
	if kind == "udp" {
		s = NewUdpServer(addr)
	} else {
		log.Fatal("Wrong server type, should be `udp` or ...: ", kind)
	}
	return s
}
