package sender

import (
	"log"
	"net"
)

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
	udp_conn.SetWriteBuffer(10485760)
	return &UdpSender{
		backend_addr: backend_addr,
		admin_addr:   admin_addr,
		conn:         udp_conn,
	}
}

func (s *UdpSender) Send(payload []byte) error {
	_, err := s.conn.Write(payload)
	if err != nil {
		log.Println("Error sending msg", err)
		return err
	}
	return nil
}
