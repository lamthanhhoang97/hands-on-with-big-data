select * from supplier order by s_suppkey desc;

update supplier set s_acctbal = s_acctbal + 100 where s_suppkey <= 1000;

-- primary key is not auto-increment
insert into supplier (
	s_suppkey,
	s_name,
	s_address,
	s_nationkey,
	s_phone,
	s_acctbal,
	s_comment
) values (1000001, 'New Supplier', 'Ho Chi Minh city, Viet Nam', 17, '27-918-335-1736', 2049.40, 'Record to delete');


delete from supplier where s_suppkey = 1000001;

-- another delete event followed by insert and update event
insert into supplier (
	s_suppkey,
	s_name,
	s_address,
	s_nationkey,
	s_phone,
	s_acctbal,
	s_comment
) values (1000002, 'New Supplier', 'Ho Chi Minh city, Viet Nam', 17, '27-918-335-1736', 2024.56, 'Record to delete');


update supplier set s_acctbal = s_acctbal - 100 where s_suppkey = 1000002;

delete from supplier where s_suppkey = 1000002;