package main

import (
	"encoding/binary"
	"fmt"
	"os"

	input "github.com/rsa17826/go-input-lib"
)

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

	// 2. Create Virtual Keyboard (uinput)
	// This is what the OS actually "hears" when we allow a key through
	vKb, err := input.CreateVirtualKeyboard("root kbd")
	if err != nil {
		panic(err)
	}
	var ev input.InputEvent
	for {
		if err := binary.Read(rootKbd, binary.NativeEndian, &ev); err != nil {
			fmt.Fprintf(os.Stderr, "read error: %v\n", err)
			return
		}
		if ev.Type != input.EV_KEY {
			continue
		}
		switch ev.Value {
		case 1: // key down
			pressKey(ev.Code)
		case 0: // key up
			releaseKey(ev.Code)
			// value 2 = repeat - ignore
		}
		if err != nil {
			panic(err)
		}
		// --- EMERGENCY KILL SWITCH ---
		// Detect Physical Ctrl + Esc
		if isKillCombo(event) {
			cleanupAndExit()
		}

		// --- LOGIC GATE ---
		if shouldBlock(event) {
			// Notify blocking macro scripts via socket
			// DO NOT write to vKb
		} else {
			// Passthrough: Write the physical event to the virtual keyboard
			vKb.WriteEvent(event)
			// Also notify passthrough scripts (Input Display)
			notifyPassthrough(event)
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
