package server

import (
	"log"
	"net"
)

type UdpServer struct {
	Addr     *net.UDPAddr
	out_chan chan []byte
	conn     *net.UDPConn
}

func (s *UdpServer) GetOutChan() chan []byte {
	return s.out_chan
}

func (s *UdpServer) IOLoop() {
	buffer := make([]byte, 2048)
	for {
		n, _, err := s.conn.ReadFromUDP(buffer)
		if err != nil {
			log.Println("Error in loop:", err)
			continue
		}
		s.out_chan <- buffer[:n]
	}
}

func (s *UdpServer) Close() {
	s.conn.Close()
}

func NewUdpServer(addr *net.UDPAddr) *UdpServer {
	conn, err := net.ListenUDP("udp4", addr)
	if err != nil {
		log.Fatalln(err)
	}
	server := &UdpServer{
		conn:     conn,
		Addr:     addr,
		out_chan: make(chan []byte),
	}
	go server.IOLoop()
	return server
}
