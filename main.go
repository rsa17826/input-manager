package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	input "github.com/rsa17826/go-input-lib"
)

type Client struct {
	conn      net.Conn
	mode      string
	send      chan WireEvent
	blockResp chan bool
}

type WireEvent struct {
	Sec   int64
	Usec  int64
	Type  uint16
	Code  uint16
	Value int32
}

const (
	ModePassthrough = "LISTEN"
	ModeBlocking    = "FILTER"
)

var (
	clients    []*Client
	clientsMu  sync.Mutex
	socketPath = "/tmp/kbd_manager.sock"
)

var eventBus = make(chan WireEvent, 1024)

func startSocketServer() {
	os.Remove(socketPath)

	l, err := net.Listen("unix", socketPath)
	if err != nil {
		panic(err)
	}

	for {
		conn, _ := l.Accept()
		go handleNewConnection(conn)
	}
}

func closeConnection() {
	os.Remove(socketPath)
}

func handleNewConnection(conn net.Conn) {
	mode, _ := bufio.NewReader(conn).ReadString('\n')
	mode = strings.TrimSpace(mode)

	c := &Client{
		conn: conn,
		mode: mode,
		send: make(chan WireEvent, 256),
	}

	clientsMu.Lock()
	clients = append(clients, c)
	clientsMu.Unlock()

	fmt.Printf("New client: %s\n", mode)

	go clientWriter(c)
}

func clientWriter(c *Client) {
	for ev := range c.send {
		err := binary.Write(c.conn, binary.LittleEndian, ev)
		if err != nil {
			c.conn.Close()
			return
		}
	}
}

func broadcast(ev WireEvent) {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	for _, c := range clients {
		select {
		case c.send <- ev:
		default:
			// drop slow client
		}
	}
}

func keyboardReader(rootKbd *input.RealKeyboard) {
	for {
		ev, err := rootKbd.ReadNextInput()
		if err != nil {
			continue
		}

		if ev.Type != input.EV_KEY {
			continue
		}

		eventBus <- WireEvent{
			Sec:   ev.Time.Sec,
			Usec:  ev.Time.Usec,
			Type:  ev.Type,
			Code:  ev.Code,
			Value: ev.Value,
		}
	}
}

func main() {
	// open keyboard
	kbdPath, err := input.FindDevice("id:usb-0c45_USB_Wired_Keyboard-event-kbd")
	if err != nil {
		panic(err)
	}

	rootKbd, err := input.OpenKeyboard(kbdPath)
	if err != nil {
		panic(err)
	}
	defer rootKbd.Close()

	vKb, err := input.CreateVirtualKeyboard("root kbd")
	if err != nil {
		panic(err)
	}

	// clear stuck keys (simple + safe)
	pressed, _ := rootKbd.GetPressedKeys()
	for _, k := range pressed {
		vKb.SendEvent(input.EV_KEY, k, 0)
	}
	vKb.Sync()

	err = rootKbd.Grab()
	if err != nil {
		panic(err)
	}

	fmt.Println("started")

	// socket server
	go startSocketServer()
	defer closeConnection()

	// input reader
	go keyboardReader(rootKbd)

	// event processor
	for ev := range eventBus {
		if shouldBlock(ev) {
			continue
		}

		vKb.SendEvent(ev.Type, ev.Code, ev.Value)
		vKb.Sync()
	}
}
func shouldBlock(ev WireEvent) bool {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	block := false

	for _, c := range clients {
		if c.mode != ModeBlocking {
			continue
		}

		select {
		case c.send <- ev:
		default:
		}

		c.conn.SetReadDeadline(time.Now().Add(2 * time.Millisecond))

		resp := make([]byte, 1)
		_, err := c.conn.Read(resp)

		if err == nil && resp[0] == '1' {
			block = true
		}
	}

	return block
}
