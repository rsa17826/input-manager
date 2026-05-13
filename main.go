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
	conn net.Conn
	mode string
	send chan WireEvent
	dead bool
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

	if mode != ModePassthrough && mode != ModeBlocking {
		fmt.Printf("Unknown mode %q, closing connection\n", mode)
		conn.Close()
		return
	}

	c := &Client{
		conn: conn,
		mode: mode,
		// send channel is only used by LISTEN clients
		send: make(chan WireEvent, 256),
	}

	clientsMu.Lock()
	clients = append(clients, c)
	clientsMu.Unlock()

	fmt.Printf("New client: %s\n", mode)

	if mode == ModePassthrough {
		// LISTEN clients: async writer goroutine drains the channel.
		// When the goroutine exits (write error = client gone), mark dead.
		go func() {
			listenWriter(c)
			clientsMu.Lock()
			c.dead = true
			clientsMu.Unlock()
			fmt.Printf("LISTEN client disconnected\n")
		}()
	}
	// FILTER clients have no writer goroutine; the event loop writes to them
	// directly and synchronously so it can read the block/allow response.
}

// listenWriter drains the send channel and writes events to a LISTEN client.
func listenWriter(c *Client) {
	for ev := range c.send {
		err := binary.Write(c.conn, binary.LittleEndian, ev)
		if err != nil {
			c.conn.Close()
			return
		}
	}
}

// broadcast sends ev to all live LISTEN clients, dropping slow ones.
func broadcast(ev WireEvent) {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	live := clients[:0]
	for _, c := range clients {
		if c.dead {
			continue
		}
		live = append(live, c)
		if c.mode != ModePassthrough {
			continue
		}
		select {
		case c.send <- ev:
		default:
			// drop slow client; it will be cleaned up on next write error
		}
	}
	clients = live
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

	for {
		pressed, err := rootKbd.GetPressedKeys()
		if err != nil {
			panic(err)
		}

		if len(pressed) == 0 {
			break
		}
		fmt.Printf("release all keys before starting: %v\n", pressed)
		for {
			ev, err := rootKbd.ReadNextInput()
			if err != nil {
				panic(err)
			}
			if ev.Value == 0 {
				break
			}
		}
	}

	err = rootKbd.Grab()
	if err != nil {
		panic(err)
	}

	fmt.Println("started")

	go startSocketServer()
	defer closeConnection()

	go keyboardReader(rootKbd)

	for ev := range eventBus {
		blocked := filterClients(ev)

		// LISTEN clients always receive events, even blocked ones, so they
		// see the full key stream regardless of what filters do.
		broadcast(ev)

		if !blocked {
			vKb.SendEvent(ev.Type, ev.Code, ev.Value)
			vKb.Sync()
		}
	}
}

// filterClients sends ev directly and synchronously to every live FILTER
// client and waits up to 5 ms for each to reply.  Returns true if any
// client wants the event blocked.
//
// Why direct write instead of c.send / listenWriter?
// Because we need to guarantee the bytes are on the wire *before* we call
// Read for the response.  A buffered channel + separate goroutine gives no
// such guarantee within the deadline window.
func filterClients(ev WireEvent) bool {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	block := false
	live := clients[:0]

	for _, c := range clients {
		if c.dead {
			continue
		}
		live = append(live, c)

		if c.mode != ModeBlocking {
			continue
		}

		// Write the event directly (sync, not via channel).
		c.conn.SetWriteDeadline(time.Now().Add(5 * time.Millisecond))
		err := binary.Write(c.conn, binary.LittleEndian, ev)
		if err != nil {
			fmt.Printf("FILTER client write error: %v - removing\n", err)
			c.conn.Close()
			c.dead = true
			continue
		}

		// Read the 1-byte response: '1' = block, anything else = pass.
		c.conn.SetReadDeadline(time.Now().Add(5 * time.Millisecond))
		resp := make([]byte, 1)
		_, err = c.conn.Read(resp)
		if err != nil {
			// timeout or disconnect; treat as "pass" but keep the client
			// (a slow script shouldn't kill the connection on one miss)
			continue
		}

		if resp[0] == '1' {
			block = true
		}
	}

	clients = live
	return block
}
