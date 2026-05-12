package main

import (
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
	err = rootKbd.Grab()
	if err != nil {
		panic(err)
	}

	vKb, err := input.CreateVirtualKeyboard("root kbd")
	if err != nil {
		panic(err)
	}
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
		// --- LOGIC GATE ---
		if shouldBlock(event) {
			// Notify blocking macro scripts via socket
			// DO NOT write to vKb
		} else {
			// Also notify passthrough scripts (Input Display)
			vKb.SendEvent(ev.Type, ev.Code, ev.Value)
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
