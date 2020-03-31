package main

import (
	"flag"
	"fmt"

	configuration "./configuration"
	poller "./poller"
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

	configArgs := configuration.Args{StoreType: *storeType, PgUseSsl: *pgUseSsl, PgUser: *pgUser, PgPassword: *pgPassword, PgHost: *pgHost, PgPort: *pgPort, PgTable: *pgTable, BrokerType: *brokerType, NatsHost: *natsHost, NatsPort: *natsPort, BufferSize: *bufferSizeFlag, PollRateMilliseconds: *pollRateFlag, Verbose: *verbose}
	config, err := configuration.Initialize(configArgs)
	if err != nil {
		panic(fmt.Sprintf("Configuration error: %s", err))
	}

	config.Logger.Info("Starting poll loop")
	poller.Run(config.Logger, config.Store, config.Broker, config.BufferSize, config.PollRate)
}
