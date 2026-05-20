package inputevents

// WireEvent represents the hardware event layout passed over the wire.
type WireEvent struct {
	Sec   int64
	Usec  int64
	Type  uint16
	Code  uint16
	Value int32
}

// Client modes available for connection routing.
const (
	ModePassthrough = "LISTEN"
	ModeBlocking    = "FILTER"
	ModeInjection   = "INJECT"
	ModeVirtListen  = "LISTEN_VIRT"
)
