package nats

import (
	dto "../../dto"
	logger "../../logger"
	"github.com/nats-io/nats.go"
)

// Broker Holds a NATS connection
type Broker struct {
	connection *nats.Conn
}

// Store message store
type Store interface {
	GetPendingMessages(limit int) ([]dto.Message, error)
	SetMessagesToInFlight(messages []dto.Message) error
	UpdateMessageToSent(id int) error
}

// Connect creates a new NATS broker connection
func Connect(connectionString string) (Broker, error) {
	nc, err := nats.Connect(connectionString)
	broker := Broker{connection: nc}
	return broker, err
}

// Publish publiches the message
func (broker Broker) Publish(logger logger.Logger, store Store, dispatch chan []dto.Message, dispatchDoneNotifier chan bool) {
	messages := <-dispatch
	logger.Debugf("Dispatching %d messages", len(messages))

	for i := 0; i < len(messages); i++ {
		message := messages[i]
		logger.Debugf("Dispatching message %d", messages[i].Id)
		broker.connection.Publish(message.Topic, message.Payload)
		store.UpdateMessageToSent(messages[i].Id)
	}

	// TODO: Handle error
	// update remaining messages back to store with status == pending
	// Return error status to main thread
	// Have main thread drop all messages and start over
	// Should we keep track of failure state? dl it? skip after n tries?

	dispatchDoneNotifier <- true
}
