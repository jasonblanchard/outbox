package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
)

func cmd_read(db *sql.DB) {
	rows, err := db.Query("SELECT * FROM messages")
	if err != nil {
		log.Fatal(err)
	}

	var (
		id      int
		status  string
		payload string
	)

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&id, &status, &payload)
		if err != nil {
			log.Fatal(err)
		}
		log.Println(id, "|", status, "|", payload)
	}
}

func cmd_write(db *sql.DB) {
	_, err := db.Query("INSERT INTO messages (payload) VALUES ($1)", fmt.Sprintf("hello %s", time.Now().UTC().String()))
	if err != nil {
		log.Fatal(err)
	}
}

func cmd_clean(db *sql.DB) {
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
		cmd_read(db)
	case "write":
		cmd_write(db)
	case "clean":
		cmd_clean(db)
	default:
		cmd_read(db)
	}

	log.Print("== Done ==")
}
