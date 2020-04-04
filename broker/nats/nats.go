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
	nc, err := nats.Connect(connectionString, nats.ReconnectBufSize(1))
	broker := Broker{connection: nc}
	return broker, err
}

// Publish publishes the message
func (broker Broker) Publish(logger logger.Logger, store store.Store, messages chan []dto.Message, done chan bool, brokerError chan error) {
	buffer := <-messages
	logger.Debugf("Publishing %d messages", len(messages))
	var err error
	var lastSuccessIndex int
	var didError bool

	for i := 0; i < len(buffer); i++ {
		message := buffer[i]
		logger.Debugf("Publishing message %d", buffer[i].Id)
		err = broker.connection.Publish(message.Topic, message.Payload)
		if err != nil {
			logger.Infof("Error publishing messages: %s", err)
			didError = true
			if i == 0 {
				lastSuccessIndex = i
			} else {
				lastSuccessIndex = i - 1
			}

			break
		} else {
			store.UpdateMessageToSent(buffer[i].Id)
		}
	}

	if didError {
		messagesToReturn := buffer[lastSuccessIndex:]
		store.UpdateMessagesToPending(messagesToReturn)
		brokerError <- err
		// Should we keep track of failure state? dl it? skip after n tries?
	}

	done <- true
}
