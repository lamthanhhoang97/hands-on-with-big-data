with 
    mininum_cost_part_t as (
        select
            f.ps_partkey,
            min(f.ps_supplycost) as min_supply_cost
        from part_supplier_fact f
        left join supplier_dim sd on f.ps_suppkey = sd.s_suppkey
        left join part_dim pd on f.ps_partkey = pd.p_partkey
        where
            sd.r_region = 'EUROPE' and
            pd.p_size = 15 and
            pd.p_type like '%BRASS'
        group by
            f.ps_partkey
    ),
    suppliers_with_min_supply_cost as (
        select
            f.ps_partkey,
            f.ps_suppkey,
            t.min_supply_cost,
            sd.s_name,
            sd.n_nation,
            sd.s_acctbal,
            pd.p_mfgr
        from part_supplier_fact f
        inner join mininum_cost_part_t t
            on 
				f.ps_partkey = t.ps_partkey and 
				f.ps_supplycost = t.min_supply_cost
        inner join supplier_dim sd
            on sd.s_suppkey = f.ps_suppkey
        inner join part_dim pd
            on pd.p_partkey = f.ps_partkey
        order by
            f.ps_partkey asc,
            sd.s_acctbal desc       
    )
    select 
        * 
    from suppliers_with_min_supply_cost