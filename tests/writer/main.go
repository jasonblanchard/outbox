package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	pg "./pg"

	_ "github.com/lib/pq"
)

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
		pg.Read(db)
	case "write":
		pg.Write(db, "")
	case "clean":
		pg.Clean(db)
	default:
		pg.Read(db)
	}

	log.Print("== Done ==")
}
