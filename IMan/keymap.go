package IMan

import (
	"fmt"

	"github.com/rsa17826/go-input-lib"
)

// applyToKeyMap updates the local key state from an incoming event.
// Only EV_KEY events are tracked; value 0 = released (removed), >0 = held.
func (self *ManagerConnection) applyToKeyMap(ev WireEvent, mode ServerMode) {
	if ev.Type != evKeyType {
		return
	}
	switch mode {
	case ModeListen:
		{
			self.realKeyMapMu.Lock()
			if self.realKeyMap == nil {
				self.realKeyMap = make(map[uint16]int32)
			}
			if ev.Value == 0 {
				delete(self.realKeyMap, ev.Code)
			} else {
				self.realKeyMap[ev.Code] = ev.Value
			}
			self.realKeyMapMu.Unlock()
		}
	case ModeVirtListen:
		{
			self.virtKeyMapMu.Lock()
			if self.virtKeyMap == nil {
				self.virtKeyMap = make(map[uint16]int32)
			}
			if ev.Value == 0 {
				delete(self.virtKeyMap, ev.Code)
			} else {
				self.virtKeyMap[ev.Code] = ev.Value
			}
			self.virtKeyMapMu.Unlock()
		}
	}
}

// EnableKeyMap activates local key-state tracking.
//
// autoRead=false — the keymap is updated as a side-effect of each ReadNext call.
//
// autoRead=true  — a background goroutine consumes all events and updates the map
// automatically. ReadNext will return an error in this mode; use IsPressed /
// KeyValue / PressedKeys to interrogate state instead.
// For ModeFilter connections the auto-read goroutine automatically responds with
// BlockInput(0) (pass-through) for every event it consumes.
func (self *ManagerConnection) EnableKeyMap(autoRead bool) error {
	self.realKeyMapMu.Lock()
	self.keyMapEnabled = true
	self.keyMapAuto = autoRead
	if self.listenConn != nil && self.realKeyMap == nil {
		self.realKeyMap = make(map[uint16]int32)
	}
	if self.virtListenConn != nil && self.virtKeyMap == nil {
		self.virtKeyMap = make(map[uint16]int32)
	}
	self.realKeyMapMu.Unlock()
	if self.listenConn == nil && self.virtListenConn == nil {
		return fmt.Errorf("keymap can only be enabled when using either ModeListen or ModeVirtListen")

	}
	if !autoRead {
		return nil
	}
	if self.filterConn != nil {
		return fmt.Errorf("don't use ModeFilter and autoRead at the same time, use ModeListen instead")
	}

	go func() {
		for {
			select {
			case re, ok := <-self.eventChan:
				if !ok {
					return
				}
				self.applyToKeyMap(re.Event, re.From)
			case <-self.closeChan:
				return
			}
		}
	}()
	return nil
}

func (self *ManagerConnection) ShiftPressed() bool {
	if self.listenConn != nil {
		return self.IsPressedReal(input.KEY_LEFTSHIFT) || self.IsPressedReal(input.KEY_RIGHTSHIFT)
	}
	if self.virtListenConn != nil {
		return self.IsPressedVirt(input.KEY_LEFTSHIFT) || self.IsPressedVirt(input.KEY_RIGHTSHIFT)
	}
	return false
}
func (self *ManagerConnection) ShiftPressedReal() bool {
	return self.IsPressedReal(input.KEY_LEFTSHIFT) || self.IsPressedReal(input.KEY_RIGHTSHIFT)
}
func (self *ManagerConnection) ShiftPressedVirt() bool {
	return self.IsPressedVirt(input.KEY_LEFTSHIFT) || self.IsPressedVirt(input.KEY_RIGHTSHIFT)
}
func (self *ManagerConnection) CtrlPressed() bool {
	if self.listenConn != nil {
		return self.IsPressedReal(input.KEY_LEFTCTRL) || self.IsPressedReal(input.KEY_RIGHTCTRL)
	}
	if self.virtListenConn != nil {
		return self.IsPressedVirt(input.KEY_LEFTCTRL) || self.IsPressedVirt(input.KEY_RIGHTCTRL)
	}
	return false
}
func (self *ManagerConnection) CtrlPressedReal() bool {
	return self.IsPressedReal(input.KEY_LEFTCTRL) || self.IsPressedReal(input.KEY_RIGHTCTRL)
}
func (self *ManagerConnection) CtrlPressedVirt() bool {
	return self.IsPressedVirt(input.KEY_LEFTCTRL) || self.IsPressedVirt(input.KEY_RIGHTCTRL)
}
func (self *ManagerConnection) AltPressed() bool {
	if self.listenConn != nil {
		return self.IsPressedReal(input.KEY_LEFTALT) || self.IsPressedReal(input.KEY_RIGHTALT)
	}
	if self.virtListenConn != nil {
		return self.IsPressedVirt(input.KEY_LEFTALT) || self.IsPressedVirt(input.KEY_RIGHTALT)
	}
	return false
}
func (self *ManagerConnection) AltPressedReal() bool {
	return self.IsPressedReal(input.KEY_LEFTALT) || self.IsPressedReal(input.KEY_RIGHTALT)
}
func (self *ManagerConnection) AltPressedVirt() bool {
	return self.IsPressedVirt(input.KEY_LEFTALT) || self.IsPressedVirt(input.KEY_RIGHTALT)
}
func (self *ManagerConnection) MetaPressed() bool {
	if self.listenConn != nil {
		return self.IsPressedReal(input.KEY_LEFTMETA) || self.IsPressedReal(input.KEY_RIGHTMETA)
	}
	if self.virtListenConn != nil {
		return self.IsPressedVirt(input.KEY_LEFTMETA) || self.IsPressedVirt(input.KEY_RIGHTMETA)
	}
	return false
}
func (self *ManagerConnection) MetaPressedReal() bool {
	return self.IsPressedReal(input.KEY_LEFTMETA) || self.IsPressedReal(input.KEY_RIGHTMETA)
}
func (self *ManagerConnection) MetaPressedVirt() bool {
	return self.IsPressedVirt(input.KEY_LEFTMETA) || self.IsPressedVirt(input.KEY_RIGHTMETA)
}

// IsPressed reports whether the given key code is currently held down.
// Requires EnableKeyMap to have been called; returns false otherwise.
func (self *ManagerConnection) IsPressed(code uint16) bool {
	if self.listenConn != nil {
		return self.IsPressedReal(code)
	}
	if self.virtListenConn != nil {
		return self.IsPressedVirt(code)
	}
	return false
}
func (self *ManagerConnection) IsPressedReal(code uint16) bool {
	self.realKeyMapMu.RLock()
	defer self.realKeyMapMu.RUnlock()
	return self.realKeyMap[code] > 0
}
func (self *ManagerConnection) IsPressedVirt(code uint16) bool {
	self.virtKeyMapMu.RLock()
	defer self.virtKeyMapMu.RUnlock()
	return self.virtKeyMap[code] > 0
}

// KeyValue returns the raw value for a key code: 0 = up, 1 = down, 2 = repeat.
// Requires EnableKeyMap to have been called; returns 0 if not held or not tracking.
func (self *ManagerConnection) KeyValue(code uint16) int32 {
	if self.listenConn != nil {
		return self.KeyValueReal(code)
	}
	if self.virtListenConn != nil {
		return self.KeyValueVirt(code)
	}
	return 0
}
func (self *ManagerConnection) KeyValueVirt(code uint16) int32 {
	self.virtKeyMapMu.RLock()
	defer self.virtKeyMapMu.RUnlock()
	return self.virtKeyMap[code]
}
func (self *ManagerConnection) KeyValueReal(code uint16) int32 {
	self.realKeyMapMu.RLock()
	defer self.realKeyMapMu.RUnlock()
	return self.realKeyMap[code]
}

// PressedKeys returns a snapshot of all key codes currently considered held.
// Requires EnableKeyMap to have been called.
func (self *ManagerConnection) PressedKeys() []uint16 {
	if self.listenConn != nil {
		return self.PressedKeysReal()
	}
	if self.virtListenConn != nil {
		return self.PressedKeysVirt()
	}
	return []uint16{}
}
func (self *ManagerConnection) PressedKeysReal() []uint16 {
	self.realKeyMapMu.RLock()
	defer self.realKeyMapMu.RUnlock()
	keys := make([]uint16, 0, len(self.realKeyMap))
	for code := range self.realKeyMap {
		keys = append(keys, code)
	}
	return keys
}
func (self *ManagerConnection) PressedKeysVirt() []uint16 {
	self.virtKeyMapMu.RLock()
	defer self.virtKeyMapMu.RUnlock()
	keys := make([]uint16, 0, len(self.virtKeyMap))
	for code := range self.virtKeyMap {
		keys = append(keys, code)
	}
	return keys
}
