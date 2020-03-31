package configuration

import (
	"fmt"
	"time"

	broker "../broker"
	natsbroker "../broker/nats"
	logger "../logger"
	store "../store"
	pgstore "../store/pg"
)

// Configuration holds configuration state
type Configuration struct {
	Logger     logger.Logger
	Store      store.Store
	Broker     broker.Broker
	BufferSize int
	PollRate   time.Duration
}

// Args initialize arguments
type Args struct {
	StoreType            string
	PgUseSsl             bool
	PgUser               string
	PgPassword           string
	PgHost               string
	PgPort               string
	PgTable              string
	BrokerType           string
	NatsHost             string
	NatsPort             string
	BufferSize           int
	PollRateMilliseconds int
	Verbose              bool
}

// Initialize create a configuration object
func Initialize(args Args) (Configuration, error) {
	config := Configuration{BufferSize: args.BufferSize}
	var err error

	loglevel := "info"
	if args.Verbose == true {
		loglevel = "debug"
	}
	config.Logger = logger.New(loglevel)

	config.Logger.Infof("Initializing with pollRate %d milliseconds, bufferSize %d \n\n", args.PollRateMilliseconds, args.BufferSize)

	pollRate := time.Duration(args.PollRateMilliseconds) * time.Millisecond // TODO Derive in config?
	config.PollRate = pollRate

	var store store.Store
	switch args.StoreType {
	case "postgres":
		var sslMode string
		if args.PgUseSsl {
			sslMode = "enable"
		} else {
			sslMode = "disable"
		}
		postgresConnectionString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", args.PgUser, args.PgPassword, args.PgHost, args.PgPort, args.PgTable, sslMode)
		store, err = pgstore.Connect(postgresConnectionString)
		if err != nil {
			panic(fmt.Sprintf("Cannot connect to store: %s", err))
		}
		config.Logger.Infof("Connected to store postgres://%s:[FILTERED]@%s:%s/%s?sslmode=%s", args.PgUser, args.PgHost, args.PgPort, args.PgTable, sslMode)
	default:
		panic(fmt.Sprintf("%s is not a valid store type", args.StoreType))
	}

	config.Store = store

	var broker broker.Broker
	switch args.BrokerType {
	case "nats":
		natsConnectionString := fmt.Sprintf("nats://%s:%s", args.NatsHost, args.NatsPort)
		broker, err = natsbroker.Connect(natsConnectionString)
		if err != nil {
			panic(fmt.Sprintf("Cannot connect to broker: %s", err))
		}
		config.Logger.Infof("Connected to broker %s", natsConnectionString)
	default:
		panic(fmt.Sprintf("%s is not a valid broker type", args.BrokerType))
	}

	config.Broker = broker

	return config, err
}
