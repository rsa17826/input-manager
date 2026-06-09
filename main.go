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

	linuxnotify "github.com/esiqveland/notify"
	"github.com/godbus/dbus/v5"
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

	if mode != ModeListen && mode != ModeBlocking && mode != ModeInjection && mode != ModeVirtListen {
		fmt.Printf("Unknown mode %d, closing connection\n", int(mode))
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

	fmt.Printf("New client context registered: %d\n", mode)

	// Both LISTEN and LISTEN_VIRT are read-only stream consumers
	if mode == ModeListen || mode == ModeVirtListen {
		go func() {
			listenWriter(c)
			clientsMu.Lock()
			c.dead = true
			clientsMu.Unlock()
			fmt.Printf("%d client disconnected\n", mode)
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
		case input.EV_REL, input.EV_ABS:
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
		if c.mode != ModeListen {
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

// 0 = Low, 1 = Normal, 2 = Critical
func notify(msg string, level byte) {
	conn, err := dbus.SessionBus()
	if err != nil {
		fmt.Println("Failed to connect to DBus:", err)
		return
	}
	note := linuxnotify.Notification{
		AppName:    "input manager",
		ReplacesID: 0,
		Summary:    "input manager",
		Body:       msg,
		Hints: map[string]dbus.Variant{
			"urgency": dbus.MakeVariant(level),
		},
	}

	_, err = linuxnotify.SendNotification(conn, note)
	if err != nil {
		fmt.Println("Failed to send notification:", err)
	}
}
func main() {
	var kbdIDs []string
	var mouseIDs []string
	var disablePanicButton bool
	var maxX int16
	var maxY int16
	argparse.ParseArgs([]argparse.ArgumentData{
		{Keys: []string{"keyboard", "k"}, AfterCount: 1, VarArgs: true, Target: &kbdIDs, Description: "the keyboards to hook", AllowDupes: true},
		{Keys: []string{"mouse", "m"}, AfterCount: 1, VarArgs: true, Target: &mouseIDs, Description: "the mice to hook", AllowDupes: true},
		{Keys: []string{"disablePanicButton"}, AfterCount: 0, VarArgs: false, Target: &disablePanicButton, Description: "disables the ability to use ctrl+esc to exit the app even when the keyboard is locked", AllowDupes: false},
		{Keys: []string{"maxX"}, AfterCount: 1, VarArgs: false, Target: &maxX, Description: "max screen x", AllowDupes: false, Default: []any{1920}},
		{Keys: []string{"maxY"}, AfterCount: 1, VarArgs: false, Target: &maxY, Description: "max screen y", AllowDupes: false, Default: []any{1080}},
	})
	// print(maxX)
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
	vmouse, err = input.CreateVirtualMouse("vRoot mouse", input.WithAbsRange(int32(maxX), int32(maxY)))
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
					ctrlPressed = ev.Value >= 0
				}
				if ev.Code == input.KEY_ESC && ctrlPressed {
					notify("Panic Pressed, Exiting...", 2)
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

			case input.EV_REL, input.EV_ABS:
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
