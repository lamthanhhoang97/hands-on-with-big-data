# Odoo database

## How-to enable Logical Replication and CDC in PostgreSQL

- Ensure `wal_level` is set to `logical`
```sql
    SHOW wal_level;
```

- Set `REPLICA IDENTITY` to `FULL` for each table to capture `before` and `after` value for each change log in WAL
```sql
    ALTER TABLE sale_order REPLICA IDENTITY FULL
```

- View `REPLICA IDENTITY` for all tables
```sql
SELECT
    c.relname AS table_name,
    CASE c.relreplident
        WHEN 'd' THEN 'DEFAULT (uses primary key)'
        WHEN 'n' THEN 'NOTHING'
        WHEN 'f' THEN 'FULL (all columns)'
        WHEN 'i' THEN 'USING INDEX'
        ELSE 'Unknown'
    END AS replica_identity,
    pg_relation_size(c.oid) AS table_size_bytes
FROM
    pg_class c
WHERE
    c.relkind = 'r' -- 'r' for regular tables
    AND c.relnamespace = (
        SELECT oid 
        FROM pg_namespace 
        WHERE nspname = 'public' -- Adjust schema name if needed
    ) 
ORDER BY
    c.relname;
```