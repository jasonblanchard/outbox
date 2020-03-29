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
)

// Store message store
type Store interface {
	GetPendingMessages(limit int) ([]dto.Message, error)
	SetMessagesToInFlight(messages []dto.Message) error
	UpdateMessageToSent(id int) error
}

// Broker message broker
type Broker interface {
	// TODO: natsbroker.Store instead of local Store is weird here, the interfaces should match... am I doing it wrong?
	Publish(logger logger.Logger, store natsbroker.Store, dispatch chan []dto.Message, dispatchDoneNotifier chan bool)
}

func main() {
	verbose := flag.Bool("verbose", false, "Turn on log levels")
	pollRateFlag := flag.Int("pollRate", 2000, "Poll rate in milliseconds")
	bufferSizeFlag := flag.Int("bufferSize", 5, "Number of records to pull each loop")
	storeType := flag.String("store", "postgres", "Store type. Options are: postgres")
	brokerType := flag.String("broker", "nats", "Broker type. Options are: nats")
	pgUser := flag.String("pgUser", "", "Postgres user")
	pgPassword := flag.String("pgPassword", "", "Postgres password")
	pgHost := flag.String("pgHost", "localhost", "Postgres host")
	pgPort := flag.String("pgPort", "5432", "Postgres port")
	pgTable := flag.String("pgTable", "", "Postgres table")
	pgUseSsl := flag.Bool("pgUseSsl", false, "Postgres SSL mode")
	natsHost := flag.String("natsHost", "127.0.0.1", "NATS host")
	natsPort := flag.String("natsPort", "4222", "NATS port")
	// TODO: Flags for NATS auth

	flag.Parse()

	loglevel := "info"
	if *verbose == true {
		loglevel = "debug"
	}
	logger := logger.New(loglevel)
	pollRateMilliseconds := time.Duration(*pollRateFlag)
	pollRate := pollRateMilliseconds * time.Millisecond
	bufferSize := *bufferSizeFlag

	var sslMode string
	if *pgUseSsl {
		sslMode = "enable"
	} else {
		sslMode = "disable"
	}
	postgresConnectionString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", *pgUser, *pgPassword, *pgHost, *pgPort, *pgTable, sslMode)

	buffer := make([]dto.Message, 0)
	dispatch := make(chan []dto.Message, bufferSize)
	dispatchDoneNotifier := make(chan bool)
	shouldSendToDispatch := true

	logger.Infof("Initializing with pollRate %d milliseconds, bufferSize %d \n\n", pollRateMilliseconds, bufferSize)

	var store Store
	var storeErr error
	switch *storeType {
	case "postgres":
		store, storeErr = pgstore.Connect(postgresConnectionString)
		if storeErr != nil {
			panic(fmt.Sprintf("Cannot connect to store: %s", storeErr))
		}
		logger.Infof("Connected to store postgres://%s:[FILTERED]@%s:%s/%s?sslmode=%s", *pgUser, *pgHost, *pgPort, *pgTable, sslMode)
	default:
		panic(fmt.Sprintf("%s is not a valid store type", *storeType))
	}

	var broker Broker
	var brokerError error
	switch *brokerType {
	case "nats":
		natsConnectionString := fmt.Sprintf("nats://%s:%s", *natsHost, *natsPort)
		broker, brokerError = natsbroker.Connect(natsConnectionString)
		if brokerError != nil {
			panic(fmt.Sprintf("Cannot connect to broker: %s", brokerError))
		}
		logger.Infof("Connected to broker %s", natsConnectionString)
	default:
		panic(fmt.Sprintf("%s is not a valid broker type", *brokerType))
	}

	logger.Info("Starting poll loop")
	for {
		logger.Debugf("%d messages in buffer", len(buffer))

		if len(buffer) == 0 {
			logger.Debug("Hydrating buffer")
			var getPendingMessagesErr error
			buffer, getPendingMessagesErr = store.GetPendingMessages(bufferSize)
			if getPendingMessagesErr != nil {
				panic(fmt.Sprintf("Error getting messages: %s", getPendingMessagesErr))
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

		logger.Debugf("Nothing to dispatch, sleeping for %d milliseconds", pollRateMilliseconds)
		time.Sleep(pollRate)
	}
}
