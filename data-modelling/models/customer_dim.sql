CREATE TABLE IF NOT EXISTS "customer_dim" (
  "c_custkey"     INT,
  "c_name"        VARCHAR(25),
  "c_address"     VARCHAR(40),
  "c_phone"       CHAR(15),
  "c_acctbal"     DECIMAL(15,2),
  "c_mktsegment"  CHAR(10),
  "c_comment"     VARCHAR(117),
  "n_nation" CHAR(25),
  "r_region" CHAR(25),
  PRIMARY KEY ("c_custkey")
);

insert into customer_dim (
    c_custkey,
    c_name,
    c_address,
    c_phone,
    c_acctbal,
    c_mktsegment,
    c_comment,
    n_nation,
    r_region
)
select
    c_custkey,
    c_name,
    c_address,
    c_phone,
    c_acctbal,
    c_mktsegment,
    c_comment,
    n.n_name,
    r.r_name
from customer c
left join nation n on c.c_nationkey = n.n_nationkey
left join region r on n.n_regionkey = r.r_regionkey;
