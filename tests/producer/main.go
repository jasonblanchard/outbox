package main

import (
	"log"

	nats "github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Panic(err)
	}
	nc.Publish("foo", []byte("Hello World"))
	defer nc.Close()

	// for i := 0; i <= 10; i++ {
	// 	nc, _ := nats.Connect(nats.DefaultURL)
	// 	nc.Publish("foo", []byte("Hello World"))
	// }
}
