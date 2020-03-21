package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/lib/pq"
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
	rows, err := db.Query("SELECT * FROM messages WHERE status = 'pending' ORDER BY id LIMIT $1", bufferSize)
	if err != nil {
		panic(fmt.Sprintf("Error reading records: %s", err))
	}
	defer rows.Close()
	return rowsToRecords(rows)
}

func setRecordsToInFlight(db *sql.DB, records []Record) {
	ids := make([]int, len(records))
	for i, record := range records {
		ids[i] = record.id
	}

	_, err := db.Query("UPDATE messages SET status = 'inflight' WHERE id = ANY($1)", pq.Array(ids))
	if err != nil {
		panic(fmt.Sprintf("Error updaing messages: %s", err))
	}
}

func updateMessageToSend(db *sql.DB, id int) {
	_, err := db.Query("UPDATE messages SET status = 'sent' WHERE id = $1", id)
	if err != nil {
		panic(fmt.Sprintf("Error updateing messages: %s", err))
	}
}

func main() {
	pollRate := 2 * time.Second
	bufferSize := 5
	storeConnStr := "postgres://outbox:outbox@localhost:5432/outbox_test?sslmode=disable"
	buffer := make([]Record, 0)
	dispatch := make(chan []Record, bufferSize)
	dispatchDoneNotifier := make(chan bool)
	shouldSendToDispatch := true

	db, err := sql.Open("postgres", storeConnStr)
	if err != nil {
		panic(fmt.Sprintf("Cannot connect to store: %s", err))
	}

	for {
		log.Printf("%d records in buffer", len(buffer))

		if len(buffer) == 0 {
			log.Println("Hydrating buffer")
			buffer = getPendingRecords(db, bufferSize)
		}

		if (len(buffer) > 0) && (shouldSendToDispatch == true) {
			setRecordsToInFlight(db, buffer)

			log.Println("Sending to dispatch")
			go func() {
				records := <-dispatch
				log.Printf("Dispatching %d messages", len(records))

				for i := 0; i < len(records); i++ {
					// TODO: NATS publish
					log.Printf("Dispatching message %d", records[i].id)
					updateMessageToSend(db, records[i].id)
				}

				time.Sleep(3 * time.Second)
				dispatchDoneNotifier <- true
			}()
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
				continue
			}
		}

		log.Printf("Message record buffer full, sleeping for %d", pollRate)
		time.Sleep(pollRate)
	}

	// Dispatcher channel, for each message record
	// nc.Publish topic with payload
	// if err, update message back to store with status == pending TODO: Should we keep track of failure state? dl it? skip after n tries?
	// if success, update message back to store with status == sent
	// return status to main thread
}
