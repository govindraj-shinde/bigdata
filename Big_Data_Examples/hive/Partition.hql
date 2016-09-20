-- Creating partitioned table
CREATE TABLE orders_partitioned_static (
order_id int,
order_date string,
order_customer_id int,
order_status string
)
PARTITIONED BY (order_year string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

-- Adding static partitions
alter table orders_partitioned_static add partition (order_year='2015');

-- Copying data into static partition
-- Load command works when the files being copied only have data with order_date from 2015
-- Using insert command

insert into table orders_partitioned_static partition (order_year='2015');


select * from orders where order_date = '2015';

