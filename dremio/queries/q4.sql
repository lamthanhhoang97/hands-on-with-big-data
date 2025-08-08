select
    f.o_orderpriority,
    count(DISTINCT f.l_orderkey) as order_count
from "tpch-hdfs"."user".hadoopuser.analytics.fact_line_item f
where
    f.o_orderdate >= '1993-07-01' AND
    f.o_orderdate < DATE_ADD('1993-07-01', 90) AND
    f.l_commitdate < f.l_shipdate
group by f.o_orderpriority
order by f.o_orderpriority;