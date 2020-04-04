package tests

import (
	"database/sql"
	"testing"
	"time"

	pg "./writer/pg"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
)

func TestTheTest(t *testing.T) {
	if false {
		t.Errorf("The test failed")
	}
}
func TestPostgresAndNATS(t *testing.T) {
	connStr := "postgres://outbox:outbox@localhost:5432/outbox_test?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Error("Can't connect to postgres")
	}

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Error("Can't connect to NATS")
	}

	pg.Clean(db)

	pg.Write(db, "first")
	pg.Write(db, "second")
	pg.Write(db, "third")

	sub, err := nc.SubscribeSync("foo")
	results := make([]string, 3)
	for i := range results {
		message, err := sub.NextMsg(3 * time.Second)
		if err != nil {
			t.Errorf("Error getting result %d: %d", i, err)
		}
		results[i] = string(message.Data)
	}

	expects := []string{"first", "second", "third"}
	for i := range results {
		if results[i] != expects[i] {
			t.Errorf("message %d received %s, wanted %s", i, results[i], expects[i])
		}
	}
}
