CREATE TABLE IF NOT EXISTS sales_demo_source
(
o_orderkey INTEGER,
o_orderstatus STRING,
o_totalprice DOUBLE,
o_orderdate STRING,
o_orderpriority STRING,
o_shippriority INTEGER,
l_linenumber INTEGER,
l_quantity DOUBLE,
l_extendedprice DOUBLE,
l_discount DOUBLE,
l_tax DOUBLE,
l_returnflag STRING,
l_linestatus STRING,
l_shipdate STRING,
l_commitdate STRING,
l_receiptdate STRING,
l_shipmode STRING,
order_year STRING,
ps_availqty INTEGER,
ps_supplycost DOUBLE,
s_name STRING,
s_acctbal DOUBLE,
s_nation STRING,
s_region STRING,
p_name STRING,
p_mfgr STRING,
p_brand STRING,
p_type STRING,
p_size INTEGER,
p_container STRING,
p_retailprice DOUBLE,
c_name STRING,
c_phone STRING,
c_acctbal DOUBLE,
c_mktsegment STRING,
c_nation STRING,
c_region STRING,
p_year STRING,
p_month STRING
)
using csv
OPTIONS (path "s3a://tpchdataset/sales_demo_partitioned/sales_demo_par")
--OPTIONS (path "/mnt1/sales_demo_source")
partitioned by ( p_year, p_month );

select count(*) from sales_demo_source;

drop olap index sales_demo_index on sales_demo_source;

create olap index sales_demo_index on sales_demo_source
dimension p_name is not nullable
timestamp dimension l_shipdate spark timestampformat "yyyy-MM-dd"
is index timestamp
is nullable nullvalue "1992-01-01T00:00:00.000"
dimensions "o_orderkey,o_orderstatus,o_orderdate,o_orderpriority,o_shippriority,l_linenumber,l_returnflag,l_linestatus,l_commitdate,l_receiptdate,l_shipmode,order_year,s_name,s_nation,s_region,p_name,p_mfgr,p_brand,p_type,p_size,p_container,c_mktsegment,c_name,c_phone,c_region,c_nation" 
metrics "o_totalprice,l_quantity,l_extendedprice,l_discount,l_tax,ps_availqty,ps_supplycost,s_acctbal,p_retailprice,c_acctbal"
OPTIONS (path "s3a://tpchdataset/sales_demo_index/sales_demo_index", nonAggregateQueryHandling "push_project_and_filters",avgSizePerPartition "400mb",
avgNumRowsPerPartition "1500000",
preferredSegmentSize "400mb",
rowFlushBoundary "100000")
PARTITION BY p_year,p_month;


select c_mktsegment, count(*) from sales_demo_source group by c_mktsegment;

select distinct p_year, p_month from sales_demo_source;

explain select count(*) from sales_demo_source;

select sum(l_extendedprice) / 7.0 as avg_yearly
from sales_demo_source
where c_nation = "MOROCCO"
and l_quantity < (
select 0.2 * avg(l_quantity)
from sales_demo_source
where cast(l_shipdate AS timestamp) BETWEEN cast('1994-04-09 00:00:00' AS timestamp) AND cast('1997-04-09 00:00:00' AS timestamp) 
and c_nation = "MOROCCO"
)
;

explain
select sum(l_extendedprice * l_discount) as revenue
from sales_demo_source
where l_shipdate >= '1994-01-01'
and l_shipdate < date_add(cast('1994-01-01' as date), 65)
and l_discount between .06 - 0.01 and .06 + 0.01
and l_quantity < 24 ;

explain


select sum(l_extendedprice * l_discount) as revenue
from sales_demo_source
where cast(l_shipdate as timestamp) >= '1994-01-01' and cast(l_shipdate as timestamp) < date_add(cast('1994-01-01' as timestamp), 90)
and l_discount > 0.1 and
l_quantity < 24
;

-- inserts

insert overwrite olap index sales_demo_index of sales_demo_source partitions p_year="1992", p_month="10";

insert overwrite olap index sales_demo_index of sales_demo_source partitions p_year="1992";