package inputevents

import (
	"encoding/binary"
	"fmt"
	"net"
)

// WireEvent represents the hardware event layout passed over the wire.
type WireEvent struct {
	Sec   int64
	Usec  int64
	Type  uint16
	Code  uint16
	Value int32
}
type ServerMode int

const (
	ModePassthrough ServerMode = iota
	ModeBlocking
	ModeInjection
	ModeVirtListen
)

// Client modes available for connection routing.
// const (
//
//	ModePassthrough = "LISTEN"
//	ModeBlocking    = "FILTER"
//	ModeInjection   = "INJECT"
//	ModeVirtListen  = "LISTEN_VIRT"
//
// )
type ManagerConnection struct {
	conn net.Conn
}

func Connect(mode ServerMode) *ManagerConnection {
	conn, err := net.Dial("unix", "/tmp/kbd_manager.sock")
	if err != nil {
		panic(err)
	}

	fmt.Fprintf(conn, "%d\n", mode)
	return &ManagerConnection{conn: conn}
}
func (self *ManagerConnection) Close() error {
	return self.conn.Close()
}
func (self *ManagerConnection) Send(event WireEvent) error {
	return binary.Write(self.conn, binary.LittleEndian, event)
}
