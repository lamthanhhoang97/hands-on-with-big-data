-- CREATE TABLE tpch.fact_line_item ENGINE=DeltaLake('http://minio:9000/data-warehouse/fact_line_item', 'lLZ6r6vDyQvxq5WaTJ9w', 'WkxauVOq61tg0DnFQNSNnI8TbKfl9qxHBK3zILZ8')

-- show tables from tpch;

select * from tpch.fact_line_item where record_start_date is null limit 100;