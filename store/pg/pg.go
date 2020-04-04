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

// Connect initializes a Postgres store connection
func Connect(connectionString string) (Store, error) {
	db, err := sql.Open("postgres", connectionString)
	store := Store{connection: db}
	return store, err
}

func rowsToMessages(rows *sql.Rows) []dto.Message {
	buffer := make([]dto.Message, 0)
	for rows.Next() {
		message := dto.Message{}
		err := rows.Scan(&message.Id, &message.Status, &message.Topic, &message.Payload)
		if err != nil {
			panic(fmt.Sprintf("Error scanning row: %s", err))
		}

		buffer = append(buffer, message)
	}
	return buffer
}

// GetPendingMessages gets pending messages
func (store Store) GetPendingMessages(limit int) ([]dto.Message, error) {
	rows, err := store.connection.Query("SELECT * FROM messages WHERE status = 'pending' ORDER BY id LIMIT $1", limit)

	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return rowsToMessages(rows), err
}

// SetMessagesToInFlight sets messages to inflight
func (store Store) SetMessagesToInFlight(messages []dto.Message) error {
	ids := make([]int, len(messages))
	for i, message := range messages {
		ids[i] = message.Id
	}

	_, err := store.connection.Query("UPDATE messages SET status = 'inflight' WHERE id = ANY($1)", pq.Array(ids))
	return err
}

// UpdateMessageToSent updates messages to send
func (store Store) UpdateMessageToSent(id int) error {
	_, err := store.connection.Query("UPDATE messages SET status = 'sent' WHERE id = $1", id)
	return err
}

// UpdateMessagesToPending updates messages back to pending
func (store Store) UpdateMessagesToPending(messages []dto.Message) error {
	ids := make([]int, len(messages))
	for i, message := range messages {
		ids[i] = message.Id
	}

	_, err := store.connection.Query("UPDATE messages SET status = 'pending' WHERE id = ANY($1)", pq.Array(ids))
	return err
}
