CREATE TABLE IF NOT EXISTS "part_supplier_fact" (
    "part_supplier_key" SERIAL PRIMARY KEY,
    "ps_partkey"     INT,
    "ps_suppkey"     INT,
    "ps_availqty"    INT,
    "ps_supplycost"  DECIMAL(15,2),
    "ps_comment"     VARCHAR(199)
);

insert into part_supplier_fact
(
    ps_partkey,
    ps_suppkey,
    ps_availqty,
    ps_supplycost,
    ps_comment
)
select
    ps_partkey,
    ps_suppkey,
    ps_availqty,
    ps_supplycost,
    ps_comment
from partsupp