package IMan

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"sync"
)

// evKeyType is the Linux EV_KEY event type constant (0x01).
// Duplicated here to avoid importing the input package into the IMan package.
const evKeyType uint16 = 0x01

type WireEvent struct {
	Sec   int64
	Usec  int64
	Type  uint16
	Code  uint16
	Value int32
}

type ServerMode int

const (
	ModeListen ServerMode = iota
	ModeFilter
	ModeInjection
	ModeVirtListen
)

// RoutedEvent tags where the event came from so your loop can process it correctly
type RoutedEvent struct {
	Event WireEvent
	From  ServerMode
}

type ManagerConnection struct {
	listenConn     net.Conn
	filterConn     net.Conn
	injectConn     net.Conn
	virtListenConn net.Conn

	// A single unified pipeline stream for all inbound events
	eventChan chan RoutedEvent
	closeChan chan struct{}

	// KeyMap — optional local mirror of pressed key state.
	// Populated either by ReadNext (autoRead=false) or a background goroutine (autoRead=true).
	keyMapEnabled bool
	keyMapAuto    bool
	realKeyMap    map[uint16]int32
	virtKeyMap    map[uint16]int32
	realKeyMapMu  sync.RWMutex
	virtKeyMapMu  sync.RWMutex
}

func Connect(name string, modes ...ServerMode) (*ManagerConnection, error) {
	mgr := &ManagerConnection{
		eventChan: make(chan RoutedEvent, 512),
		closeChan: make(chan struct{}),
	}

	// Clamp name to 255 bytes so it fits in the 1-byte length prefix
	nameBytes := []byte(name)
	if len(nameBytes) > 255 {
		nameBytes = nameBytes[:255]
	}

	// Encode PID as 4 little-endian bytes
	pid := os.Getpid()
	pidBytes := []byte{byte(pid), byte(pid >> 8), byte(pid >> 16), byte(pid >> 24)}

	for _, mode := range modes {
		conn, err := net.Dial("unix", "/tmp/kbd_manager.sock")
		if err != nil {
			mgr.Close()
			return nil, fmt.Errorf("failed to connect mode %d: %w", mode, err)
		}

		// Send: mode byte | name-length byte | name bytes | pid 4 bytes
		handshake := append([]byte{byte(mode), byte(len(nameBytes))}, nameBytes...)
		handshake = append(handshake, pidBytes...)
		_, err = conn.Write(handshake)
		if err != nil {
			conn.Close()
			mgr.Close()
			return nil, err
		}

		switch mode {
		case ModeListen:
			mgr.listenConn = conn
		case ModeFilter:
			mgr.filterConn = conn
		case ModeInjection:
			mgr.injectConn = conn
		case ModeVirtListen:
			mgr.virtListenConn = conn
		}
	}

	// Spin up the dynamic multi-channel listener loop
	go mgr.startUnifiedMultiplexer()

	return mgr, nil
}

// startUnifiedMultiplexer dynamically builds a select statement at runtime
func (self *ManagerConnection) startUnifiedMultiplexer() {
	var cases []reflect.SelectCase
	var modes []ServerMode

	// Helper to attach an active network stream connection to a channel case
	addConnCase := func(conn net.Conn, mode ServerMode) {
		if conn == nil {
			return
		}
		ch := make(chan WireEvent)
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)})
		modes = append(modes, mode)

		// Dedicated light reader per socket pushing into reflect.Select cases
		go func() {
			var ev WireEvent
			for {
				err := binary.Read(conn, binary.LittleEndian, &ev)
				if err != nil {
					close(ch)
					return
				}
				select {
				case ch <- ev:
				case <-self.closeChan:
					return
				}
			}
		}()
	}

	addConnCase(self.listenConn, ModeListen)
	addConnCase(self.virtListenConn, ModeVirtListen)
	addConnCase(self.filterConn, ModeFilter)

	// Include a close casing mechanism so loops exit gracefully on Close()
	closeIdx := len(cases)
	cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(self.closeChan)})

	if len(cases) == 1 { // Only the close case exists
		return
	}

	for {
		chosen, value, ok := reflect.Select(cases)
		if chosen == closeIdx || !ok {
			return
		}

		ev := value.Interface().(WireEvent)
		select {
		case self.eventChan <- RoutedEvent{Event: ev, From: modes[chosen]}:
		case <-self.closeChan:
			return
		}
	}
}

func (self *ManagerConnection) Close() error {
	close(self.closeChan)
	if self.listenConn != nil {
		self.listenConn.Close()
	}
	if self.filterConn != nil {
		self.filterConn.Write([]byte{0xFF})
		self.filterConn.Close()
	}
	if self.injectConn != nil {
		self.injectConn.Close()
	}
	if self.virtListenConn != nil {
		self.virtListenConn.Close()
	}
	return nil
}

// ReadNext blocks until ANY of the active sockets (Listen, Virt, or Filter) receives an event.
// If EnableKeyMap(true) was called, ReadNext returns an error immediately — the background
// auto-read goroutine owns the event stream in that mode.
// If EnableKeyMap(false) was called, the keymap is updated as a side-effect before returning.
func (self *ManagerConnection) ReadNext() (RoutedEvent, error) {
	self.realKeyMapMu.RLock()
	autoRead := self.keyMapAuto && self.keyMapEnabled
	self.realKeyMapMu.RUnlock()

	if autoRead {
		return RoutedEvent{}, fmt.Errorf("ReadNext is unavailable when EnableKeyMap(autoRead=true): the auto-read loop owns the event stream")
	}

	select {
	case re, ok := <-self.eventChan:
		if !ok {
			return RoutedEvent{}, io.EOF
		}
		self.realKeyMapMu.RLock()
		enabled := self.keyMapEnabled
		self.realKeyMapMu.RUnlock()
		if enabled {
			self.applyToKeyMap(re.Event, re.From)
		}
		return re, nil
	case <-self.closeChan:
		return RoutedEvent{}, io.EOF
	}
}

// Send outputs injection commands up the line
func (self *ManagerConnection) Send(event WireEvent) error {
	if self.injectConn == nil {
		return fmt.Errorf("injection mode not initialized")
	}
	return binary.Write(self.injectConn, binary.LittleEndian, event)
}

// BlockInput replies back to intercept challenges via the blocking socket context
func (self *ManagerConnection) BlockInput(block uint8) (int, error) {
	if self.filterConn == nil {
		return 0, fmt.Errorf("blocking mode not initialized")
	}
	return self.filterConn.Write([]byte{block})
}
