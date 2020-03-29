package broker

import (
	dto "../dto"
	logger "../logger"
	store "../store"
)

// Broker message broker
type Broker interface {
	Publish(logger logger.Logger, store store.Store, dispatch chan []dto.Message, dispatchDoneNotifier chan bool)
}
