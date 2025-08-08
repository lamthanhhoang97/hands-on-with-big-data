create table if not exists "supplier_dim" (
	s_suppkey INT,
    s_name CHAR(25),
    s_address varchar(40),
    s_phone char(15),
    s_acctbal DECIMAL(15,2),
    s_comment varchar(101),
    n_nation char(25),
    r_region char(25),
	PRIMARY KEY ("s_suppkey")
);

insert into supplier_dim
select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    s_acctbal,
    s_comment,
    n.n_name as nation,
    r.r_name as region
from supplier s
left join nation n on s.s_nationkey = n.n_nationkey
left join region r on r.r_regionkey = n.n_nationkey;