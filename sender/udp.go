package sender

import (
	"log"
	"net"
)

type UdpSender struct {
	backend_addr *net.UDPAddr
	admin_addr   *net.TCPAddr
}

func (s *UdpSender) Close() error {
	return nil
}

func NewUdpSender(backend_addr *net.UDPAddr, admin_addr *net.TCPAddr) Sender {
	return &UdpSender{
		backend_addr: backend_addr,
		admin_addr:   admin_addr,
	}
}

func (s *UdpSender) Send(payload []byte) error {
	udp_conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		log.Fatalln("Shit happened", err)
	}
	_, err = udp_conn.WriteToUDP(payload, s.backend_addr)
	udp_conn.Close()
	if err != nil {
		log.Println("Error sending msg", err)
		return err
	}
	return nil
}
