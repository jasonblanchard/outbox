package dto

// Message Represents the message we want to dispatch
type Message struct {
	Id      int
	Status  string
	Topic   string
	Payload []byte
}
