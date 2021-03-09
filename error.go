package amqp

// Error implementation for the AMQP conductor
type Error struct {
	e string
}

func (e *Error) Error() string {
	return e.String()
}

func (e *Error) String() string {
	return e.e
}
