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

	argparse "github.com/rsa17826/go-arg-lib"
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

		eventBus <- WireEvent{
			Sec:   ev.Time.Sec,
			Usec:  ev.Time.Usec,
			Type:  ev.Type,
			Code:  ev.Code,
			Value: ev.Value,
			// Device:
		}
	}
}
func mouseReader(rootMouse *input.RealMouse) {
	for {
		ev, err := rootMouse.ReadNextInput()
		if err != nil {
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
func waitRelease(dev interface {
	GetPressedKeys() ([]uint16, error)
	ReadNextInput() (input.InputEvent, error)
}) {
	for {
		pressed, err := dev.GetPressedKeys()
		if err != nil {
			panic(err)
		}
		if len(pressed) == 0 {
			break
		}

		fmt.Printf("Release all buttons/keys: %v\n", pressed)
		for {
			ev, err := dev.ReadNextInput()
			if err != nil {
				panic(err)
			}
			if ev.Value == 0 {
				break
			}
		}
	}
}

func main() {
	var kbdIDs []string
	var mouseIDs []string
	argparse.ParseArgs([]argparse.ArgumentData{
		{Keys: []string{"keyboard", "k"}, AfterCount: 1, VarArgs: true, Target: &kbdIDs, Description: "the keyboards to hook"},
		{Keys: []string{"mouse", "m"}, AfterCount: 1, VarArgs: true, Target: &mouseIDs, Description: "the mice to hook"},
	})

	var kbds []*input.RealKeyboard
	var mice []*input.RealMouse
	for _, id := range kbdIDs {
		kbd, err := input.FindAndOpenKeyboard(id)
		kbds = append(kbds, kbd)
		if err != nil {
			panic(err)
		}
		defer kbd.Close()
	}

	for _, id := range mouseIDs {
		mouse, err := input.FindAndOpenMouse(id)
		mice = append(mice, mouse)
		if err != nil {
			panic(err)
		}
		defer mouse.Close()
	}

	vkb, err := input.CreateVirtualKeyboard("vRoot kbd")
	if err != nil {
		panic(err)
	}
	vmouse, err := input.CreateVirtualMouse("vRoot mouse")
	if err != nil {
		panic(err)
	}
	for _, kbd := range kbds {
		waitRelease(kbd)
		err = kbd.Grab()
		if err != nil {
			panic(err)
		}
	}
	for _, mouse := range mice {
		waitRelease(mouse)
		err = mouse.Grab()
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("started")

	go startSocketServer()
	defer closeConnection()
	for _, kbd := range kbds {
		go keyboardReader(kbd)
	}
	for _, mouse := range mice {
		go mouseReader(mouse)
	}

	var ctrlPressed bool
	for ev := range eventBus {
		if ev.Type == input.EV_KEY {
			if ev.Code == input.KEY_LEFTCTRL {
				ctrlPressed = ev.Value == 1
			}
			if ev.Code == input.KEY_ESC && ctrlPressed {
				os.Exit(5)
			}
		}
		blocked := filterClients(ev)
		broadcast(ev)

		if !blocked {
			switch ev.Type {
			case input.EV_KEY:
				if ev.Code >= 256 {
					vmouse.SendEvent(ev.Type, ev.Code, ev.Value)
					vmouse.Sync()
				} else {
					vkb.SendEvent(ev.Type, ev.Code, ev.Value)
					vkb.Sync()
				}

			case input.EV_REL:
				vmouse.SendEvent(ev.Type, ev.Code, ev.Value)
				vmouse.Sync()

			default:
				vkb.SendEvent(ev.Type, ev.Code, ev.Value)
				vkb.Sync()
			}
		}
	}
}

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
