package counter

type trackingCounter struct {
	baseCounter
}

// NewTrackingCounter returns a counter keeps track of them item being counted
// to avoid incrementing twice for it if we see it from 2 difference sources,
// or decrementing when it hasn't been accounted for after our process was started.
// TODO: Not Implemented!
func NewTrackingCounter() *trackingCounter {
	return &trackingCounter{
		baseCounter: *NewBaseCounter(),
	}
}
