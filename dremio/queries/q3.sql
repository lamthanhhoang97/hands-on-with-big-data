select
    f.l_orderkey,
    sum(f.l_extendedprice * (1 - f.l_discount)) as revenue,
    f.o_orderdate,
    f.o_shippriority
from "tpch-hdfs"."user".hadoopuser.analytics.fact_line_item f
left join "tpch-hdfs"."user".hadoopuser.analytics.dim_customer cd on cd.c_custkey = f.o_custkey
where
    cd.c_mktsegment = 'BUILDING' and
    f.o_orderdate < '1995-03-15' and
    f.l_shipdate > '1995-03-15'
group by
    f.l_orderkey,
    f.o_orderdate,
    f.o_shippriority
order by 
    revenue desc,
    f.o_orderdate