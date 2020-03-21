# Dependencies
- https://github.com/golang-migrate/migrate

# Setting up the DB
```
createuser -e -d -P -E outbox
# If prompted, set password to outbox

make create_db

# Check it
psql -U  outbox outbox_test

# Set env for migrations
export POSTGRESQL_URL='postgres://outbox:outbox@localhost:5432/outbox_test?sslmode=disable'
```
# Configuration vars:
- store connection string
- adapter type (pg for now)
- buffer size
- poll rate
