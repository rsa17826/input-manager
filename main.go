package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	argparse "github.com/rsa17826/go-arg-lib"
	input "github.com/rsa17826/go-input-lib"
	. "github.com/rsa17826/input-manager/IMan"
)

type Client struct {
	conn   net.Conn
	reader *bufio.Reader
	mode   ServerMode
	send   chan WireEvent
	dead   bool
}

var (
	clients    []*Client
	clientsMu  sync.Mutex
	socketPath = "/tmp/kbd_manager.sock"

	// Mutexes to make sure concurrent writing to virtual devices from
	// main loop and inject loops doesn't cause raw layout corruption
	vkbMu    sync.Mutex
	vmouseMu sync.Mutex

	vkb    *input.VirtualKeyboard
	vmouse *input.VirtualMouse
)

var eventBus = make(chan WireEvent, 1024)

func startSocketServer() {
	os.Remove(socketPath)

	l, err := net.Listen("unix", socketPath)
	if err != nil {
		panic(err)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			continue
		}
		go handleNewConnection(conn)
	}
}

func closeConnection() {
	os.Remove(socketPath)
}

func handleNewConnection(conn net.Conn) {
	// Allocate a 1-byte buffer to read the mode
	buf := make([]byte, 1)
	_, err := conn.Read(buf)
	if err != nil {
		conn.Close()
		return
	}

	// Convert the raw byte back into your ServerMode enum type
	mode := ServerMode(buf[0])

	if mode != ModePassthrough && mode != ModeBlocking && mode != ModeInjection && mode != ModeVirtListen {
		fmt.Printf("Unknown mode %q, closing connection\n", mode)
		conn.Close()
		return
	}

	c := &Client{
		conn:   conn,
		reader: bufio.NewReader(conn), // Keep the reader if you still need it later for data streams
		mode:   mode,                  // This should now be typed as ServerMode in your Client struct
		send:   make(chan WireEvent, 256),
	}

	clientsMu.Lock()
	clients = append(clients, c)
	clientsMu.Unlock()

	fmt.Printf("New client context registered: %s\n", mode)

	// Both LISTEN and LISTEN_VIRT are read-only stream consumers
	if mode == ModePassthrough || mode == ModeVirtListen {
		go func() {
			listenWriter(c)
			clientsMu.Lock()
			c.dead = true
			clientsMu.Unlock()
			fmt.Printf("%s client disconnected\n", mode)
		}()
	}

	// Handle execution loop for client sending instructions down the pipeline
	if mode == ModeInjection {
		go func() {
			handleInjectionReader(c)
			clientsMu.Lock()
			c.dead = true
			clientsMu.Unlock()
			fmt.Printf("INJECT client disconnected\n")
		}()
	}
}

// handleInjectionReader reads WireEvents sent *from* an INJECT client and passes them to vdevs
func handleInjectionReader(c *Client) {
	defer c.conn.Close()
	var ev WireEvent

	for {
		err := binary.Read(c.reader, binary.LittleEndian, &ev)
		if err != nil {
			if err != io.EOF {
				fmt.Printf("INJECT connection read error: %v\n", err)
			}
			return
		}

		// Direct routing execution blocks mirrored cleanly from your main loop routing
		switch ev.Type {
		case input.EV_KEY:
			if ev.Code >= 256 {
				vmouseMu.Lock()
				vmouse.SendEvent(ev.Type, ev.Code, ev.Value)
				vmouse.Sync()
				vmouseMu.Unlock()
			} else {
				vkbMu.Lock()
				vkb.SendEvent(ev.Type, ev.Code, ev.Value)
				vkb.Sync()
				vkbMu.Unlock()
			}
		case input.EV_REL:
			vmouseMu.Lock()
			vmouse.SendEvent(ev.Type, ev.Code, ev.Value)
			vmouse.Sync()
			vmouseMu.Unlock()
		default:
			vkbMu.Lock()
			vkb.SendEvent(ev.Type, ev.Code, ev.Value)
			vkb.Sync()
			vkbMu.Unlock()
		}

		// Notify LISTEN_VIRT clients that this event reached the virtual device
		broadcastVirt(ev)
	}
}

func listenWriter(c *Client) {
	for ev := range c.send {
		err := binary.Write(c.conn, binary.LittleEndian, ev)
		if err != nil {
			c.conn.Close()
			return
		}
	}
}

// broadcastReal sends real input events to all LISTEN clients.
func broadcastReal(ev WireEvent) {
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
		}
	}
	clients = live
}

// broadcastVirt sends events that reached the virtual devices to all LISTEN_VIRT clients.
func broadcastVirt(ev WireEvent) {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	for _, c := range clients {
		if c.dead || c.mode != ModeVirtListen {
			continue
		}
		select {
		case c.send <- ev:
		default:
		}
	}
}

func keyboardReader(kbd *input.RealKeyboard) {
	for {
		ev, err := kbd.ReadNextInput()
		if err != nil {
			fmt.Println("KBD Read Error:", err)
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

func mouseReader(mouse *input.RealMouse) {
	for {
		ev, err := mouse.ReadNextInput()
		if err != nil {
			fmt.Println("MOUSE Read Error:", err)
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
	var disablePanicButton bool
	argparse.ParseArgs([]argparse.ArgumentData{
		{Keys: []string{"keyboard", "k"}, AfterCount: 1, VarArgs: true, Target: &kbdIDs, Description: "the keyboards to hook", AllowDupes: true},
		{Keys: []string{"mouse", "m"}, AfterCount: 1, VarArgs: true, Target: &mouseIDs, Description: "the mice to hook", AllowDupes: true},
		{Keys: []string{"disablePanicButton"}, AfterCount: 0, VarArgs: false, Target: &disablePanicButton, Description: "disables the ability to use ctrl+esc to exit the app even when the keyboard is locked", AllowDupes: false},
	})
	if len(kbdIDs) == 0 && len(mouseIDs) == 0 {
		argparse.PrintHelpAndExit()
	}
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

	var err error
	vkb, err = input.CreateVirtualKeyboard("vRoot kbd")
	if err != nil {
		panic(err)
	}
	vmouse, err = input.CreateVirtualMouse("vRoot mouse")
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
		if !disablePanicButton {
			if ev.Type == input.EV_KEY {
				if ev.Code == input.KEY_LEFTCTRL {
					ctrlPressed = ev.Value == 1
				}
				if ev.Code == input.KEY_ESC && ctrlPressed {
					os.Exit(5)
				}
			}
		}
		blocked := filterClients(ev)
		broadcastReal(ev)

		if !blocked {
			switch ev.Type {
			case input.EV_KEY:
				if ev.Code >= 256 {
					vmouseMu.Lock()
					vmouse.SendEvent(ev.Type, ev.Code, ev.Value)
					vmouse.Sync()
					vmouseMu.Unlock()
				} else {
					vkbMu.Lock()
					vkb.SendEvent(ev.Type, ev.Code, ev.Value)
					vkb.Sync()
					vkbMu.Unlock()
				}

			case input.EV_REL:
				vmouseMu.Lock()
				vmouse.SendEvent(ev.Type, ev.Code, ev.Value)
				vmouse.Sync()
				vmouseMu.Unlock()

			default:
				vkbMu.Lock()
				vkb.SendEvent(ev.Type, ev.Code, ev.Value)
				vkb.Sync()
				vkbMu.Unlock()
			}
			broadcastVirt(ev)
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

		c.conn.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
		resp := make([]byte, 1)
		_, err = io.ReadFull(c.reader, resp)
		if err != nil {
			continue
		}

		if resp[0] == 1 {
			block = true
		}
	}

	clients = live
	return block
}
