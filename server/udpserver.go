package server

import (
	"log"
	"net"
)

type UdpServer struct {
	Addr      *net.UDPAddr
	out_chan  chan []byte
	conn      *net.UDPConn
	exit_chan chan int
}

func (s *UdpServer) GetExitChan() chan int {
	return s.exit_chan
}

func (s *UdpServer) GetOutChan() chan []byte {
	return s.out_chan
}

func (s *UdpServer) IOLoop() {
	buffer := make([]byte, 2048)
ioloop:
	for {
		select {
		case <-s.exit_chan:
			log.Println("Exiting IOLoop")
			break ioloop
		default:
			n, _, err := s.conn.ReadFromUDP(buffer)
			if err != nil {
				log.Println("Error in loop:", err)
				continue
			}
			s.out_chan <- buffer[:n]
		}
	}
}

func (s *UdpServer) Close() {
	log.Println("Closing server")
	close(s.exit_chan)
	s.conn.Close()
}

func NewUdpServer(addr *net.UDPAddr) *UdpServer {
	conn, err := net.ListenUDP("udp4", addr)
	if err != nil {
		log.Fatalln(err)
	}
	server := &UdpServer{
		conn:      conn,
		Addr:      addr,
		out_chan:  make(chan []byte),
		exit_chan: make(chan int),
	}
	go server.IOLoop()
	return server
}
