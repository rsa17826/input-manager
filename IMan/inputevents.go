package IMan

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

type ServerMode uint8

const (
	ModeListen     ServerMode = 1 << 0
	ModeBlocking   ServerMode = 1 << 1
	ModeInjection  ServerMode = 1 << 2
	ModeVirtListen ServerMode = 1 << 3
)

type ManagerConnection struct {
	asyncConn net.Conn // Handles Listen, Injection, VirtListen
	blockConn net.Conn // Handles Blocking strictly isolated
}

func Connect(mode ServerMode) (*ManagerConnection, error) {
	mgr := &ManagerConnection{}

	// 1. If Blocking is requested, isolate it to its own socket connection
	if (mode & ModeBlocking) != 0 {
		conn, err := net.Dial("unix", "/tmp/kbd_manager.sock")
		if err != nil {
			return nil, fmt.Errorf("failed to connect block channel: %w", err)
		}
		// Register strictly as Blocking
		_, err = conn.Write([]byte{byte(ModeBlocking)})
		if err != nil {
			conn.Close()
			return nil, err
		}
		mgr.blockConn = conn
	}

	// 2. If any other modes are requested, handle them over a second connection
	asyncMask := mode & ^ModeBlocking
	if asyncMask != 0 {
		conn, err := net.Dial("unix", "/tmp/kbd_manager.sock")
		if err != nil {
			if mgr.blockConn != nil {
				mgr.blockConn.Close()
			}
			return nil, fmt.Errorf("failed to connect async channel: %w", err)
		}
		// Register the remaining combined bits
		_, err = conn.Write([]byte{byte(asyncMask)})
		if err != nil {
			conn.Close()
			if mgr.blockConn != nil {
				mgr.blockConn.Close()
			}
			return nil, err
		}
		mgr.asyncConn = conn
	}

	return mgr, nil
}

func (self *ManagerConnection) Close() error {
	var err1, err2 error
	if self.asyncConn != nil {
		err1 = self.asyncConn.Close()
	}
	if self.blockConn != nil {
		err2 = self.blockConn.Close()
	}
	if err1 != nil {
		return err1
	}
	return err2
}

// Send pushes injection events up to the server via the async connection
func (self *ManagerConnection) Send(event WireEvent) error {
	if self.asyncConn == nil {
		return fmt.Errorf("injection mode not initialized on this connection")
	}
	return binary.Write(self.asyncConn, binary.LittleEndian, event)
}

// Read intercepts incoming event streams (Listen / VirtListen) from the async connection
func (self *ManagerConnection) Read(ev *WireEvent) error {
	if self.asyncConn == nil {
		return fmt.Errorf("listen mode not initialized on this connection")
	}
	return binary.Write(self.asyncConn, binary.LittleEndian, ev)
}

// BlockInput replies back to a filter challenge strictly via the isolated blocking connection
func (self *ManagerConnection) BlockInput(block uint8) (int, error) {
	if self.blockConn == nil {
		return 0, fmt.Errorf("blocking mode not initialized on this connection")
	}
	return self.blockConn.Write([]byte{block})
}

// ReadBlockChallenge reads the incoming filter request from the server
func (self *ManagerConnection) ReadBlockChallenge(ev *WireEvent) error {
	if self.blockConn == nil {
		return fmt.Errorf("blocking mode not initialized on this connection")
	}
	return binary.Read(self.blockConn, binary.LittleEndian, ev)
}
