package poller

import (
	"fmt"
	"time"

	broker "../broker"
	dto "../dto"
	logger "../logger"
	store "../store"
)

// Run run the thing
func Run(logger logger.Logger, store store.Store, broker broker.Broker, bufferSize int, pollRate time.Duration) {
	buffer := make([]dto.Message, 0)
	messages := make(chan []dto.Message, bufferSize)
	brokerDone := make(chan bool)
	brokerError := make(chan error)
	shouldSendToBroker := true

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

		switch {
		case (len(buffer) > 0) && (shouldSendToBroker == true):
			store.SetMessagesToInFlight(buffer)

			logger.Debug("Sending to broker")
			go broker.Publish(logger, store, messages, brokerDone, brokerError)
			messages <- buffer
			buffer = make([]dto.Message, 0)
			shouldSendToBroker = false

		case (len(buffer) > 0) && (shouldSendToBroker == false):
			logger.Debug("Buffer full but broker blocked, waiting...")
			select {
			case result := <-brokerDone:
				logger.Debug("Buffer unblocked, continuing")
				shouldSendToBroker = result
			case result := <-brokerError:
				logger.Infof("Error sending message: %d", result)
				buffer = make([]dto.Message, 0)
			}
		default:
			logger.Debugf("Nothing to publish, sleeping for %d milliseconds", pollRate/1000000)
			time.Sleep(pollRate)
		}
	}
}
