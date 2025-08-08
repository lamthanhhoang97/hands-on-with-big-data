select
    f.l_orderkey,
    sum(f.l_extendedprice * (1 - f.l_discount)) as revenue,
    od.o_orderdate,
    od.o_shippriority
from line_item_fact f
inner join order_dim od on od.o_orderkey = f.l_orderkey
inner join customer_dim cd on cd.c_custkey = f.c_custkey
where
    cd.c_mktsegment = 'BUILDING' and
    od.o_orderdate < '1995-03-15' and
    f.l_shipdate > '1995-03-15'
group by
    f.l_orderkey,
    od.o_orderdate,
    od.o_shippriority
order by 
    revenue desc,
    od.o_orderdate