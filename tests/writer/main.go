package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
)

func read(db *sql.DB) {
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

func write(db *sql.DB) {
	_, err := db.Query("INSERT INTO messages (topic, payload) VALUES ('foo', $1)", fmt.Sprintf("hello %s", time.Now().UTC().String()))
	if err != nil {
		log.Fatal(err)
	}
}

func clean(db *sql.DB) {
	_, err := db.Query("DELETE FROM messages")
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	var op string
	if len(os.Args) < 2 {
		op = "read"
	} else {
		op = os.Args[1]
	}

	connStr := "postgres://outbox:outbox@localhost:5432/outbox_test?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	log.Print(fmt.Sprintf("== Running op: \"%s\" ==", op))

	switch op {
	case "read":
		read(db)
	case "write":
		write(db)
	case "clean":
		clean(db)
	default:
		read(db)
	}

	log.Print("== Done ==")
}
