package amqp

type eventFunc func() interface{}
type errFunc func() error

// event is a helper function that indicates
// if the events channel is nil
func (r *rabbitmq) event(fn eventFunc) {
	if r.events != nil {
		select {
		case <-r.ctx.Done():
			return
		case r.events <- fn():
		}
	}
}

// e is a helper function that indicates
// if the events channel is nil
func (r *rabbitmq) err(fn errFunc) {
	if r.errors != nil {
		select {
		case <-r.ctx.Done():
			return
		case r.errors <- fn():
		}
	}
}

// Events creates a channel to receive events from the atomizer and
// return the channel for handling
func (r *rabbitmq) Events(buffer int) <-chan interface{} {
	if buffer < 0 {
		buffer = 0
	}

	r.eventsMu.Lock()
	defer r.eventsMu.Unlock()

	if r.events == nil {
		r.events = make(chan interface{}, buffer)
	}

	return r.events
}

// Errors creates a channel to receive errors from the atomizer and
// return the channel for handling
func (r *rabbitmq) Errors(buffer int) <-chan error {
	if buffer < 0 {
		buffer = 0
	}

	r.errorsMu.Lock()
	defer r.errorsMu.Unlock()

	if r.errors == nil {
		r.errors = make(chan error, buffer)
	}

	return r.errors
}
