package pg

import (
	"database/sql"
	"fmt"

	dto "../../dto"

	// TODO: Do I need this?
	"github.com/lib/pq"
	// TODO: Do I need this?
	_ "github.com/lib/pq"
)

// Store holds the DB connection
type Store struct {
	connection *sql.DB
}

// New initializes a Postgres store
func New(connectionString string) (Store, error) {
	db, err := sql.Open("postgres", connectionString)
	store := Store{connection: db}
	return store, err
}

func rowsToRecords(rows *sql.Rows) []dto.Message {
	buffer := make([]dto.Message, 0)
	for rows.Next() {
		record := dto.Message{}
		err := rows.Scan(&record.Id, &record.Status, &record.Topic, &record.Payload)
		if err != nil {
			panic(fmt.Sprintf("Error scanning row: %s", err))
		}

		buffer = append(buffer, record)
	}
	return buffer
}

// GetPendingRecords gets pending records
func (store Store) GetPendingRecords(limit int) ([]dto.Message, error) {
	rows, err := store.connection.Query("SELECT * FROM messages WHERE status = 'pending' ORDER BY id LIMIT $1", limit)

	defer rows.Close()
	return rowsToRecords(rows), err
}

// SetRecordsToInFlight sets records to inflight
func (store Store) SetRecordsToInFlight(records []dto.Message) error {
	ids := make([]int, len(records))
	for i, record := range records {
		ids[i] = record.Id
	}

	_, err := store.connection.Query("UPDATE messages SET status = 'inflight' WHERE id = ANY($1)", pq.Array(ids))
	return err
}

// UpdateMessageToSend updates messages to send
func (store Store) UpdateMessageToSend(id int) error {
	_, err := store.connection.Query("UPDATE messages SET status = 'sent' WHERE id = $1", id)
	return err
}
