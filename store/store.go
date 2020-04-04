package store

import (
	dto "../dto"
)

// Store message store
type Store interface {
	GetPendingMessages(limit int) ([]dto.Message, error)
	SetMessagesToInFlight(messages []dto.Message) error
	UpdateMessageToSent(id int) error
	UpdateMessagesToPending(messages []dto.Message) error
}
