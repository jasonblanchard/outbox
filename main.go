package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

// Record TODO
type Record struct {
	id      int
	status  string
	topic   string
	payload string
}

func rowsToRecords(rows *sql.Rows) []Record {
	buffer := make([]Record, 0)
	for rows.Next() {
		record := Record{}
		err := rows.Scan(&record.id, &record.status, &record.topic, &record.payload)
		if err != nil {
			panic(fmt.Sprintf("Error scanning row: %s", err))
		}

		buffer = append(buffer, record)
	}
	return buffer
}

func getPendingRecords(db *sql.DB, bufferSize int) []Record {
	rows, err := db.Query("SELECT * FROM messages WHERE status = 'pending' LIMIT $1", bufferSize)
	if err != nil {
		panic(fmt.Sprintf("Error reading records: %s", err))
	}
	defer rows.Close()
	return rowsToRecords(rows)
}

func main() {
	pollRate := 2 * time.Second
	bufferSize := 5
	storeConnStr := "postgres://outbox:outbox@localhost:5432/outbox_test?sslmode=disable"
	buffer := make([]Record, 0)
	dispatch := make(chan []Record, 5)
	dispatchDoneNotifier := make(chan bool)
	shouldSendToDispatch := true

	db, err := sql.Open("postgres", storeConnStr)
	if err != nil {
		panic(fmt.Sprintf("Cannot connect to store: %s", err))
	}

	go func() {
		records := <-dispatch
		log.Printf("Dispatching %d messages", len(records))
		// TODO For each, do the NATS publish
		// On success, update record in DB to status = sent
		time.Sleep(1 * time.Second)
		dispatchDoneNotifier <- true
	}()

	for {
		if len(buffer) == 0 {
			log.Println("Hydrating buffer")
			buffer = getPendingRecords(db, bufferSize)
			log.Printf("%d records in buffer", len(buffer))
		}

		if (len(buffer) > 0) && (shouldSendToDispatch == true) {
			// TODO For each message, set status == inflight
			log.Println("Sending to dispatch")
			dispatch <- buffer
			buffer = make([]Record, 0)
			shouldSendToDispatch = false
			continue
		}

		if (len(buffer) > 0) && (shouldSendToDispatch == false) {
			log.Println("Buffer full but dispatcher blocked, waiting...")
			select {
			case result := <-dispatchDoneNotifier:
				log.Println("Buffer unblocked, continuing")
				shouldSendToDispatch = result
			}
		}

		log.Printf("Message record buffer full, sleeping for %d", pollRate)
		time.Sleep(pollRate)

		// if len(buffer) == 0 {
		// 	log.Println("Hydrating buffer")

		// 	buffer = getPendingRecords(db, bufferSize)

		// 	log.Printf("%d records in buffer", len(buffer))

		// 	if shouldSendToDispatch {
		// 		// TODO For each message, set status == inflight
		// 		log.Println("Sending to dispatch")
		// 		dispatch <- buffer
		// 		buffer = make([]Record, 0)
		// 		shouldSendToDispatch = false
		// 	}
		// } else if (len(buffer) > 0) && (shouldSendToDispatch == false) {
		// 	log.Println("Buffer full but dispatcher blocked, waiting...")
		// 	select {
		// 	case result := <-dispatchDoneNotifier:
		// 		log.Println("Buffer unblocked, continuing")
		// 		shouldSendToDispatch = result
		// 	}
		// } else {
		// 	log.Printf("Message record buffer full, sleeping for %d", pollRate)
		// 	time.Sleep(pollRate)
		// }
	}

	// Dispatcher channel, for each message record
	// nc.Publish topic with payload
	// if err, update message back to store with status == pending TODO: Should we keep track of failure state? dl it? skip after n tries?
	// if success, update message back to store with status == sent
	// return status to main thread
}
