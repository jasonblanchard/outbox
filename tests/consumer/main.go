package main

import (
	"log"

	nats "github.com/nats-io/nats.go"
)

func main() {
	nc, _ := nats.Connect(nats.DefaultURL)

	// wg := sync.WaitGroup{}
	// wg.Add(1)

	// nc.Subscribe("foo", func(m *nats.Msg) {
	// 	log.Printf("Async: %s\n", string(m.Data))
	// })

	ch := make(chan *nats.Msg, 64)
	nc.ChanSubscribe("foo", ch)
	// msg := <-ch
	for msg := range ch {
		log.Printf("Channel: %s\n", string(msg.Data))
	}

	// wg.Wait()
	defer nc.Close()
}
