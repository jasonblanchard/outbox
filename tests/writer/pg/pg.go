package pg

import (
	"database/sql"
	"fmt"
	"log"
	"time"
)

// Read TODO
func Read(db *sql.DB) {
	rows, err := db.Query("SELECT * FROM messages")
	if err != nil {
		log.Fatal(err)
	}

	var (
		id      int
		status  string
		topic   string
		payload string
	)

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&id, &status, &topic, &payload)
		if err != nil {
			log.Fatal(err)
		}
		log.Println(id, "|", status, "|", topic, "|", payload)
	}
}

// Write TODO
func Write(db *sql.DB, message string) {
	payload := fmt.Sprintf("hello %s", time.Now().UTC().String())
	if message != "" {
		payload = message
	}
	_, err := db.Query("INSERT INTO messages (topic, payload) VALUES ('foo', $1)", payload)
	if err != nil {
		log.Fatal(err)
	}
}

// Clean TODO
func Clean(db *sql.DB) {
	_, err := db.Query("DELETE FROM messages")
	if err != nil {
		log.Fatal(err)
	}
}
