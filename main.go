package main

import (
	"flag"
	"fmt"
	"time"

	natsbroker "./broker/nats"
	dto "./dto"
	logger "./logger"
	pgstore "./store/pg"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
)

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

	// TODO: Use different store dpending on flag
	store, err := pgstore.New(storeConnStr)
	if err != nil {
		panic(fmt.Sprintf("Cannot connect to store: %s", err))
	}

	broker, err := natsbroker.New(natsConnStr)
	if err != nil {
		panic(fmt.Sprintf("Cannot connect to broker: %s", err))
	}

	logger.Info("Starting poll loop")
	for {
		logger.Debugf("%d messages in buffer", len(buffer))

		if len(buffer) == 0 {
			logger.Debug("Hydrating buffer")
			buffer, err = store.GetPendingMessages(bufferSize)
			if err != nil {
				panic(fmt.Sprintf("Error getting messages: %s", err))
			}
		}

		// TODO: Change to switch since these are mutually exclusive?
		if (len(buffer) > 0) && (shouldSendToDispatch == true) {
			store.SetMessagesToInFlight(buffer)

			logger.Debug("Sending to dispatch")
			go broker.Publish(logger, store, dispatch, dispatchDoneNotifier)
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
