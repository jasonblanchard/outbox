package main

import (
	"flag"
	"fmt"
	"time"

	dto "./dto"
	logger "./logger"
	pgstore "./store/pg"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
)

// Store message store
type Store interface {
	GetPendingMessages(limit int) ([]dto.Message, error)
	SetMessagesToInFlight(messages []dto.Message) error
	UpdateMessageToSent(id int) error
}

func handleDispatch(logger logger.Logger, store Store, nc *nats.Conn, dispatch chan []dto.Message, dispatchDoneNotifier chan bool) {
	messages := <-dispatch
	logger.Debugf("Dispatching %d messages", len(messages))

	for i := 0; i < len(messages); i++ {
		message := messages[i]
		logger.Debugf("Dispatching message %d", messages[i].Id)
		nc.Publish(message.Topic, message.Payload)
		store.UpdateMessageToSent(messages[i].Id)
	}

	// TODO: Handle error
	// update remaining messages back to store with status == pending
	// Return error status to main thread
	// Have main thread drop all messages and start over
	// Should we keep track of failure state? dl it? skip after n tries?

	dispatchDoneNotifier <- true
}

func main() {
	verbose := flag.Bool("verbose", false, "Turn on log levels")
	flag.Parse()

	loglevel := "info"
	if *verbose == true {
		loglevel = "debug"
	}
	logger := logger.New(loglevel)
	pollRate := 2 * time.Second
	bufferSize := 5
	storeConnStr := "postgres://outbox:outbox@localhost:5432/outbox_test?sslmode=disable"
	natsConnStr := nats.DefaultURL
	buffer := make([]dto.Message, 0)
	dispatch := make(chan []dto.Message, bufferSize)
	dispatchDoneNotifier := make(chan bool)
	shouldSendToDispatch := true

	logger.Info("Initializing...")

	store, err := pgstore.New(storeConnStr)

	nc, err := nats.Connect(natsConnStr)
	if err != nil {
		panic(fmt.Sprintf("Cannot connect to Nats: %s", err))
	}
	logger.Info("Connected to NATS")

	logger.Info("Starting poll loop")
	for {
		logger.Debugf("%d messages in buffer", len(buffer))

		if len(buffer) == 0 {
			logger.Debug("Hydrating buffer")
			buffer, _ = store.GetPendingMessages(bufferSize)
		}

		// TODO: Change to switch since these are mutually exclusive?
		if (len(buffer) > 0) && (shouldSendToDispatch == true) {
			store.SetMessagesToInFlight(buffer)

			logger.Debug("Sending to dispatch")
			go handleDispatch(logger, store, nc, dispatch, dispatchDoneNotifier)
			dispatch <- buffer
			buffer = make([]dto.Message, 0)
			shouldSendToDispatch = false
			continue
		}

		if (len(buffer) > 0) && (shouldSendToDispatch == false) {
			logger.Debug("Buffer full but dispatcher blocked, waiting...")
			select {
			case result := <-dispatchDoneNotifier:
				logger.Debug("Buffer unblocked, continuing")
				shouldSendToDispatch = result
				continue
			}
		}

		logger.Debugf("Nothing to dispatch, sleeping for %d", pollRate)
		time.Sleep(pollRate)
	}
}
