// Copyright © 2019 Developer Network, LLC
//
// This file is subject to the terms and conditions defined in
// file 'LICENSE', which is part of this source code package.

package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"atomizer.io/engine"
	"devnw.com/alog"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

const (
	// DEFAULTADDRESS is the address to connect to rabbitmq
	DEFAULTADDRESS string = "amqp://guest:guest@localhost:5672/"
)

// Connect uses the connection string that is passed in to initialize
// the rabbitmq conductor
func Connect(
	ctx context.Context,
	connectionstring,
	inqueue string,
) (c engine.Conductor, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &Error{fmt.Sprintf("panic %v", r)}
		}
	}()

	if ctx == nil {
		ctx = context.Background()
	}

	// initialize the context of the conductor
	ctx, cancel := context.WithCancel(ctx)

	mq := &rabbitmq{
		ctx:         ctx,
		cancel:      cancel,
		in:          inqueue,
		uuid:        uuid.New().String(),
		electrons:   make(map[string]chan<- *engine.Properties),
		electronsMu: sync.Mutex{},
		pubs:        make(map[string]chan []byte),
		pubsmutty:   sync.Mutex{},
	}

	if connectionstring == "" {
		return nil, &Error{"empty connection string"}
	}

	// TODO: Add additional validation here for formatting later

	// Dial the connection
	connection, err := amqp.Dial(connectionstring)
	if err != nil {
		defer mq.cancel()
		return nil, &Error{
			fmt.Sprintf("error connecting to rabbitmq | %s", err.Error()),
		}
	}

	// Setup cleanup to run when the context closes
	go mq.Cleanup()

	mq.connection = connection
	alog.Printf("conductor established [%s]", mq.uuid)

	return mq, nil
}

// The rabbitmq struct uses the amqp library to connect to rabbitmq in order
// to send and receive from the message queue.
type rabbitmq struct {
	ctx    context.Context
	cancel context.CancelFunc

	// Incoming Requests
	in string

	// Queue for receiving results of sent messages
	uuid   string
	sender sync.Map

	electrons   map[string]chan<- *engine.Properties
	electronsMu sync.Mutex
	once        sync.Once

	connection *amqp.Connection

	pubs      map[string]chan []byte
	pubsmutty sync.Mutex
}

func (r *rabbitmq) Cleanup() {
	<-r.ctx.Done()
	_ = r.connection.Close()
}

// Receive gets the atoms from the source that are available to atomize.
// Part of the Conductor interface
func (r *rabbitmq) Receive(ctx context.Context) <-chan *engine.Electron {
	electrons := make(chan *engine.Electron)

	go func(electrons chan<- *engine.Electron) {
		defer close(electrons)

		in := r.getReceiver(ctx, r.in)

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-in:
				if !ok {
					return
				}

				e := &engine.Electron{}
				err := json.Unmarshal(msg, e)
				if err != nil {
					alog.Errorf(err, "unable to parse electron %s", string(msg))
				}

				r.sender.Store(e.ID, e.SenderID)

				select {
				case <-ctx.Done():
					return
				case electrons <- e:
					alog.Printf("electron [%s] received by conductor", e.ID)
				}
			}
		}
	}(electrons)

	return electrons
}

func (r *rabbitmq) fanResults(ctx context.Context) {
	results := r.getReceiver(ctx, r.uuid)

	go func(results <-chan []byte) {
		defer func() {
			if rec := recover(); rec != nil {
				r.cancel()
			}
		}()

		alog.Printf("conductor [%s] receiver initialized", r.uuid)

		for {
			select {
			case <-ctx.Done():
				return
			case result, ok := <-results:
				if !ok {
					panic("conductor results channel closed")
				}

				go r.fanIn(result)
			}
		}
	}(results)
}

func (r *rabbitmq) pop(key string) (chan<- *engine.Properties, bool) {
	r.electronsMu.Lock()
	defer r.electronsMu.Unlock()

	// Pull the results channel for the electron
	c, ok := r.electrons[key]
	if !ok || c == nil {
		return nil, false
	}

	delete(r.electrons, key)
	return c, true
}

func (r *rabbitmq) fanIn(result []byte) {
	// Unwrap the object
	p := &engine.Properties{}
	if err := json.Unmarshal(result, &p); err != nil {
		alog.Errorf(err, "error while un-marshaling results for conductor [%s]", r.uuid)
		return
	}

	alog.Printf("received electron [%s] result from node", p.ElectronID)

	c, ok := r.pop(p.ElectronID)
	if !ok {
		return
	}

	defer close(c)

	select {
	case <-r.ctx.Done():
		return
	case c <- p: // push the result onto the channel
		alog.Printf("sent electron [%s] results to channel", p.ElectronID)
	}
}

// Gets the list of messages that have been sent to the queue and returns
// them as a channel of byte arrays
func (r *rabbitmq) getReceiver(
	ctx context.Context,
	queue string,
) <-chan []byte {
	// Create the inbound processing exchanges and queues
	c, err := r.connection.Channel()
	if err != nil {
		return nil
	}

	_, err = c.QueueDeclare(
		queue, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil
	}

	// Prefetch variables
	err = c.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)

	if err != nil {
		return nil
	}

	in, err := c.Consume(

		queue, // Queue
		"",    // consumer
		true,  // auto ack
		false, // exclusive
		false, // no local
		false, // no wait
		nil,   // args
	)

	if err != nil {
		return nil
	}

	out := make(chan []byte)
	go func(in <-chan amqp.Delivery, out chan<- []byte) {
		defer func() {
			_ = c.Close()
			close(out)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-in:
				if !ok {
					return
				}

				out <- msg.Body
			}
		}
	}(in, out)

	return out
}

// Complete mark the completion of an electron instance with applicable statistics
func (r *rabbitmq) Complete(ctx context.Context, properties *engine.Properties) (err error) {
	if s, ok := r.sender.Load(properties.ElectronID); ok {
		if senderID, ok := s.(string); ok {
			var result []byte
			if result, err = json.Marshal(&properties); err == nil {
				r.publish(ctx, senderID, result)
				alog.Printf("sent results for electron [%s] to sender [%s]", properties.ElectronID, senderID)
			}
		}
	}

	return err
}

// Publishes an electron for processing or publishes a completed electron's properties
func (r *rabbitmq) publish(ctx context.Context, queue string, message []byte) {
	select {
	case <-ctx.Done():
		return
	case r.getPublisher(ctx, queue) <- message:
	}
}

// TODO: re-evaluate the errors here and determine if they should panic instead
func (r *rabbitmq) getPublisher(ctx context.Context, queue string) chan<- []byte {
	r.pubsmutty.Lock()
	defer r.pubsmutty.Unlock()

	p := r.pubs[queue]

	// create the channel used for publishing and setup a go channel to monitor for publishing requests
	if p == nil {
		// Create the channel and update the map
		p = make(chan []byte)
		r.pubs[queue] = p

		// Create the new publisher and start the monitoring loop
		go func(
			ctx context.Context,
			connection *amqp.Connection,
			p <-chan []byte,
		) {
			c, err := connection.Channel()
			if err != nil {
				alog.Error(err)
				return
			}

			defer func() {
				_ = c.Close()
			}()

			_, err = c.QueueDeclare(
				queue, // name
				true,  // durable
				false, // delete when unused
				false, // exclusive
				false, // no-wait
				nil,   // arguments
			)

			if err != nil {
				alog.Error(err)
				return
			}

			for {
				select {
				case <-ctx.Done():
					return
				case msg, ok := <-p:
					if !ok {
						return
					}

					err := c.Publish(
						"",    // exchange
						queue, // routing key
						false, // mandatory
						false, // immediate
						amqp.Publishing{
							ContentType: "application/json",
							Body:        msg,
						})

					if err != nil {
						alog.Error(err)
						continue
					}
				}
			}
		}(ctx, r.connection, p)
	}

	return p
}

// Sends electrons back out through the conductor for additional processing
func (r *rabbitmq) Send(ctx context.Context, electron *engine.Electron) (<-chan *engine.Properties, error) {
	var e []byte
	var err error
	respond := make(chan *engine.Properties)

	if electron == nil {
		defer close(respond)
		return respond, &Error{"nil electron"}
	}

	// setup the results fan out
	r.once.Do(func() { r.fanResults(ctx) })

	// TODO: Add in timeout here
	go func(ctx context.Context, electron *engine.Electron, respond chan<- *engine.Properties) {
		electron.SenderID = r.uuid

		if e, err = json.Marshal(electron); err == nil {
			// Register the electron return channel prior to publishing the request
			r.electronsMu.Lock()
			r.electrons[electron.ID] = respond
			r.electronsMu.Unlock()

			// publish the request to the message queue
			r.publish(ctx, r.in, e)
			alog.Printf("sent electron [%s] for processing\n", electron.ID)
		} else {
			alog.Errorf(err, "error while marshaling electron [%s]", electron.ID)
		}
	}(ctx, electron, respond)

	return respond, err
}

func (r *rabbitmq) Close() {
	// cancel out the internal context cleaning up the rabbit connection and channel
	r.cancel()
}
