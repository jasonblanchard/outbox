package main

import (
	"database/sql"
	"flag"
	"fmt"
	"time"

	logger "./logger"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
)

// Record Represents the message we want to dispatch
type Record struct {
	id      int
	status  string
	topic   string
	payload []byte
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

func handleDispatch(logger logger.Logger, db *sql.DB, nc *nats.Conn, dispatch chan []Record, dispatchDoneNotifier chan bool) {
	records := <-dispatch
	logger.Debugf("Dispatching %d messages", len(records))

	for i := 0; i < len(records); i++ {
		record := records[i]
		logger.Debugf("Dispatching message %d", records[i].id)
		nc.Publish(record.topic, record.payload)
		updateMessageToSend(db, records[i].id)
	}

	// TODO: Handle error
	// update remaining messages back to store with status == pending
	// Return error status to main thread
	// Have main thread drop all messages and start over
	// Should we keep track of failure state? dl it? skip after n tries?

	dispatchDoneNotifier <- true
}

func main() {
	verbose := flag.Bool("verbose", false, "Turn on log levels")
	flag.Parse()

	loglevel := "info"
	if *verbose == true {
		loglevel = "debug"
	}
	logger := logger.New(loglevel)
	pollRate := 2 * time.Second
	bufferSize := 5
	storeConnStr := "postgres://outbox:outbox@localhost:5432/outbox_test?sslmode=disable"
	natsConnStr := nats.DefaultURL
	buffer := make([]Record, 0)
	dispatch := make(chan []Record, bufferSize)
	dispatchDoneNotifier := make(chan bool)
	shouldSendToDispatch := true

	logger.Info("Initializing...")

	db, err := sql.Open("postgres", storeConnStr)
	if err != nil {
		panic(fmt.Sprintf("Cannot connect to store: %s", err))
	}
	logger.Infof("Connected to Postgres")

	nc, err := nats.Connect(natsConnStr)
	if err != nil {
		panic(fmt.Sprintf("Cannot connect to Nats: %s", err))
	}
	logger.Info("Connected to NATS")

	logger.Info("Starting poll loop")
	for {
		logger.Debugf("%d records in buffer", len(buffer))

		if len(buffer) == 0 {
			logger.Debug("Hydrating buffer")
			buffer = getPendingRecords(db, bufferSize)
		}

		// TODO: Change to switch since these are mutually exclusive?
		if (len(buffer) > 0) && (shouldSendToDispatch == true) {
			setRecordsToInFlight(db, buffer)

			logger.Debug("Sending to dispatch")
			go handleDispatch(logger, db, nc, dispatch, dispatchDoneNotifier)
			dispatch <- buffer
			buffer = make([]Record, 0)
			shouldSendToDispatch = false
			continue
		}

		if (len(buffer) > 0) && (shouldSendToDispatch == false) {
			logger.Debug("Buffer full but dispatcher blocked, waiting...")
			select {
			case result := <-dispatchDoneNotifier:
				logger.Debug("Buffer unblocked, continuing")
				shouldSendToDispatch = result
				continue
			}
		}

		logger.Debugf("Nothing to dispatch, sleeping for %d", pollRate)
		time.Sleep(pollRate)
	}
}
