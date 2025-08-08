select
    d.n_nation,
    sum(f.l_extendedprice * (1 - f.l_discount)) as revenue
from "tpch-hdfs"."user".hadoopuser.analytics.fact_line_item f
inner join "tpch-hdfs"."user".hadoopuser.analytics."dim_customer" d
    on f.o_custkey = d.c_custkey
where
    d.r_region = 'ASIA' and
    f.o_orderdate >= '1994-01-01' and
    f.o_orderdate < date_add('1994-01-01', 365)
group by d.n_nation
order by revenue desc;