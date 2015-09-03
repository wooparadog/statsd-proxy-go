package sender

import (
	"log"
	"net"
)

type Sender interface {
	Send([]byte) error
	Close() error
}

type UdpSender struct {
	backend_addr *net.UDPAddr
	admin_addr   *net.TCPAddr
	conn         *net.UDPConn
}

func (s *UdpSender) Close() error {
	return s.conn.Close()
}

func NewUdpSender(backend_addr *net.UDPAddr, admin_addr *net.TCPAddr) Sender {
	udp_conn, err := net.DialUDP("udp4", nil, backend_addr)
	if err != nil {
		log.Fatalln("Shit happened")
	}
	return &UdpSender{
		backend_addr: backend_addr,
		admin_addr:   admin_addr,
		conn:         udp_conn,
	}
}

func (s *UdpSender) Send(payload []byte) error {
	log.Println(s.backend_addr)
	_, err := s.conn.Write(payload)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}
