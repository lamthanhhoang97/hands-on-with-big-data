# TPC-H PostgreSQL Database

- Start the server
```bash
# Start the Postgres service
docker compose up -d;

# Copy the file from local to container
docker cp supplier.tbl <container id>:./

# Connect to the service
docker exec -it <container id> bash

## Inside the container
psql -d tpch_db -U tpch_user; # connect to database inside the container
\d # list tables in the database

# Create a table
-- supplier
CREATE TABLE IF NOT EXISTS "supplier" (
  "s_suppkey"     INT,
  "s_name"        CHAR(25),
  "s_address"     VARCHAR(40),
  "s_nationkey"   INT,
  "s_phone"       CHAR(15),
  "s_acctbal"     DECIMAL(15,2),
  "s_comment"     VARCHAR(101),
  "s_dummy"       VARCHAR(10),
  PRIMARY KEY ("s_suppkey"));

# Copy local file to a table
\copy "supplier"   from './supplier.tbl'    DELIMITER '|' CSV;
```