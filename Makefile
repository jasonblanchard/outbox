POSTGRES_URL=postgres://outbox:outbox@localhost:5432/outbox_test?sslmode=disable

create_db:
	createdb -U outbox -e -O outbox outbox_test

migrations_up:
	migrate --database ${POSTGRESQL_URL} --path ./tests/db/migrations up

migrations_down:
	migrate --database ${POSTGRESQL_URL} --path ./tests/db/migrations down

clean_db:
	dropdb outbox_test

test:
	go test ./...
