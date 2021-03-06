CREATE TYPE status AS ENUM ('pending', 'inflight', 'sent');

CREATE TABLE IF NOT EXISTS messages(
   id serial PRIMARY KEY,
   status status DEFAULT 'pending',
   topic VARCHAR(200),
   payload TEXT
);
