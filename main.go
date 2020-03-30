package main

import (
	"flag"
	"fmt"
	"time"

	broker "./broker"
	natsbroker "./broker/nats"
	logger "./logger"
	poller "./poller"
	store "./store"
	pgstore "./store/pg"
	_ "github.com/lib/pq"
)

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

	logger.Infof("Initializing with pollRate %d milliseconds, bufferSize %d \n\n", pollRateMilliseconds, bufferSize)

	var store store.Store
	var storeErr error
	switch *storeType {
	case "postgres":
		var sslMode string
		if *pgUseSsl {
			sslMode = "enable"
		} else {
			sslMode = "disable"
		}
		postgresConnectionString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", *pgUser, *pgPassword, *pgHost, *pgPort, *pgTable, sslMode)
		store, storeErr = pgstore.Connect(postgresConnectionString)
		if storeErr != nil {
			panic(fmt.Sprintf("Cannot connect to store: %s", storeErr))
		}
		logger.Infof("Connected to store postgres://%s:[FILTERED]@%s:%s/%s?sslmode=%s", *pgUser, *pgHost, *pgPort, *pgTable, sslMode)
	default:
		panic(fmt.Sprintf("%s is not a valid store type", *storeType))
	}

	var broker broker.Broker
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
	poller.Run(logger, store, broker, bufferSize, pollRate)
}
