package nats

import (
	dto "../../dto"
	logger "../../logger"
	store "../../store"
	"github.com/nats-io/nats.go"
)

// Broker Holds a NATS connection
type Broker struct {
	connection *nats.Conn
}

// Connect creates a new NATS broker connection
func Connect(connectionString string) (Broker, error) {
	nc, err := nats.Connect(connectionString)
	broker := Broker{connection: nc}
	return broker, err
}

// Publish publishes the message
func (broker Broker) Publish(logger logger.Logger, store store.Store, messages chan []dto.Message, done chan bool) {
	buffer := <-messages
	logger.Debugf("Publishing %d messages", len(messages))

	for i := 0; i < len(buffer); i++ {
		message := buffer[i]
		logger.Debugf("Publishing message %d", buffer[i].Id)
		broker.connection.Publish(message.Topic, message.Payload)
		store.UpdateMessageToSent(buffer[i].Id)
	}

	// TODO: Handle error
	// Update remaining messages back to store with status == pending
	// Send error to an error to error channel (or false to done channel?)
	// Should we keep track of failure state? dl it? skip after n tries?

	done <- true
}
