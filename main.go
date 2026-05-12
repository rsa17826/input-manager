package main

import (
	"bufio"
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
}

const (
	ModePassthrough = "LISTEN" // Just wants a copy
	ModeBlocking    = "FILTER" // Wants to intercept
)

var (
	clients    []*Client
	clientsMu  sync.Mutex
	socketPath = "/tmp/kbd_manager.sock"
)

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
	// First line from client should be "LISTEN" or "FILTER"
	mode, _ := bufio.NewReader(conn).ReadString('\n')
	mode = strings.TrimSpace(mode)

	clientsMu.Lock()
	clients = append(clients, &Client{conn: conn, mode: mode})
	clientsMu.Unlock()

	fmt.Printf("New client connected in %s mode\n", mode)
}
func main() {
	// 1. Open physical keyboard
	kbdPath, err := input.FindDevice("id:usb-0c45_USB_Wired_Keyboard-event-kbd")
	if err != nil {
		panic(err)
	}
	rootKbd, err := input.OpenKeyboard(kbdPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open device: %v\n", err)
		os.Exit(1)
	}
	defer rootKbd.Close()
	err = rootKbd.Grab()
	if err != nil {
		panic(err)
	}

	vKb, err := input.CreateVirtualKeyboard("root kbd")
	if err != nil {
		panic(err)
	}
	go startSocketServer()
	defer closeConnection()
	var ev input.InputEvent
	var ctrlPressed bool
	for {
		ev, err = rootKbd.ReadNextInput()
		if ev.Type != input.EV_KEY {
			continue
		}
		if ev.Code == input.KEY_LEFTCTRL {
			ctrlPressed = ev.Value == 1
		}
		if ev.Code == input.KEY_ESC && ctrlPressed {
			os.Exit(5)
		}
		if err != nil {
			panic(err)
		}
		isBlocked := false
		clientsMu.Lock()
		for _, c := range clients {
			// Set a very short deadline (2ms) so we don't lag the hardware
			c.conn.SetDeadline(time.Now().Add(2 * time.Millisecond))

			// Send the event
			_, err := fmt.Fprintf(c.conn, "%d,%d,%d\n", ev.Type, ev.Code, ev.Value)
			if err != nil {
				c.conn.Close()
			} else if c.mode == ModeBlocking {
				resp := make([]byte, 1)
				_, err := c.conn.Read(resp)
				if err == nil && resp[0] == '1' {
					isBlocked = true
				}
			}
		}
		clientsMu.Unlock()

		if !isBlocked {
			vKb.SendEvent(ev.Type, ev.Code, ev.Value)
			vKb.Sync()
		}
	}
}

// func main() {
// 	// id:usb-0c45_USB_Wired_Keyboard-event-kbd
// 	// id:usb-04d9_USB_Gaming_Mouse-event-mouse
// 	// id:usb-04d9_USB_Gaming_Mouse-if01-event-kbd
// 	kbd, err := input.FindDevice("id:usb-0c45_USB_Wired_Keyboard-event-kbd")
// 	if err != nil {
// 		panic(err)
// 	}
// 	kbd.Press()
// }
