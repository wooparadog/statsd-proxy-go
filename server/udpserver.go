package server

import (
	"log"
	"net"
	"time"
)

const BUFFERSIZE int = 1 * 1024 * 1024 // 1MiB
type UdpServer struct {
	Addr     *net.UDPAddr
	out_chan chan []byte
	conn     *net.UDPConn
}

func (s *UdpServer) GetOutChan() chan []byte {
	return s.out_chan
}

func (s *UdpServer) IOLoop() {
	var buff *[BUFFERSIZE]byte
	var offset int
	var timeout bool
	defer s.conn.Close()

	for {
		if buff == nil {
			buff = new([BUFFERSIZE]byte)
			offset = 0
			timeout = false
		}

		i, err := s.conn.Read(buff[offset:])
		if err == nil {
			buff[offset+i] = '\n'
			offset = offset + i + 1
		} else if err.(net.Error).Timeout() {
			timeout = true
			err = s.conn.SetDeadline(time.Now().Add(time.Second))
			if err != nil {
				log.Panicln(err)
			}
		} else {
			log.Printf("Read Error: %s\n", err)
			continue
		}

		if offset > BUFFERSIZE-4096 || timeout {
			// Approching make buff size
			// we use a 4KiB margin
			s.out_chan <- buff[:offset]
			buff = nil
		}
		buffer := make([]byte, 2048)
		n, err := s.conn.Read(buffer[0:])
		if err != nil {
			log.Println("Error in loop:", err)
			continue
		}
		s.out_chan <- buffer[:n]
	}
}

func (s *UdpServer) Close() {
	log.Println("Closing server")
	s.conn.Close()
}

func NewUdpServer(addr *net.UDPAddr) *UdpServer {
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalln(err)
	}
	conn.SetReadBuffer(16777216)
	server := &UdpServer{
		conn:     conn,
		Addr:     addr,
		out_chan: make(chan []byte, 256),
	}
	go server.IOLoop()
	return server
}
