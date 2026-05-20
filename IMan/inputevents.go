package IMan

import (
	"encoding/binary"
	"io"
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

func Connect(mode ServerMode) (*ManagerConnection, error) {
	conn, err := net.Dial("unix", "/tmp/kbd_manager.sock")
	if err != nil {
		return &ManagerConnection{}, err
	}

	// Send the mode directly as a single byte
	_, err = conn.Write([]byte{byte(mode)})
	if err != nil {
		conn.Close()
		return &ManagerConnection{}, err
	}
	return &ManagerConnection{conn: conn}, nil
}
func (self *ManagerConnection) Close() error {
	return self.conn.Close()
}
func (self *ManagerConnection) Send(event WireEvent) error {
	return binary.Write(self.conn, binary.LittleEndian, event)
}
func (self *ManagerConnection) ReadFull(buf []byte) (int, error) {
	return io.ReadFull(self.conn, buf)
}
func (self *ManagerConnection) BlockInput(block uint8) (int, error) {
	return self.conn.Write([]byte{block})
}
func (self *ManagerConnection) Read(ev *WireEvent) error {
	return binary.Read(self.conn, binary.LittleEndian, &ev)
}
