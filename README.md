# Spark Druid Package

Spark-Druid package enables *Logical Plans* written against a **raw event dataset** or a **star schema** to be rewritten
to take advantage of a **[Drud Index](http://druid.io/)** of the data. It comprises of a [Druid DataSource](https://github.com/SparklineData/spark-druid-olap/blob/master/src/main/scala/org/sparklinedata/druid/DefaultSource.scala) 
that wraps the _underlying dataset_, and a [Druid Planner](https://github.com/SparklineData/spark-druid-olap/blob/master/src/main/scala/org/apache/spark/sql/sources/druid/DruidPlanner.scala) 
that contains a set of *Rewrite Rules* to convert 'Project-Filter-Aggregation-Having-Sort-Limit' plans to Druid Index 
Rest calls.

![Overall Picture](https://github.com/SparklineData/spark-druid-olap/blob/master/docs/druidSparkOverall.png)

There are 2 scenarios where the combination of Druid and Spark
adds value:
* For existing deployments of Druid, this can expose the Druid Index as a
  DataSource in Spark. This provides a proper SQL interface(with
  jdbc/odbc access) to the Druid dataset; *it also enables
  analytics in Spark(SQL + advanced analytics) to utilize the fast navigation/aggregation
  capabilities of the Druid Index*. The second point is the key: the
  goal is **fast SQL++ access on a denormalized/flat event
  Dataset. One that marries the rich programming model of Spark with
  the fast access and aggregation capabilities of Druid**.
- The use of Druid technology as an OLAP
  index for **Star Schemas residing in Spark**. This is a classic OLAP
  space/performance tradeoff: a specialized secondary index to provide **BI Acceleration**

The [design document](https://github.com/SparklineData/spark-druid-olap/blob/master/docs/SparkDruid.pdf) 
is good(albeit dated) source of information about Spark Druid connectivity, and the Rewrite Rules in place. We have **benchmarked** a set
of representative queries from the [TPCH benchmark](http://www.tpc.org/tpch/spec/tpch2.8.0.pdf). 
For slice and dice queries like TPCH Q7 we see an improvement in the order of 10x. The benchmark results
and an analysis are in the Design document(a more detailed description of the benchmark is available 
[here](https://github.com/SparklineData/spark-druid-olap/blob/master/docs/benchmark/BenchMarkDetails.pdf))

We now support Rewrites for __Star Joins__, __Aggregates__ including _Cubes_ and _Rollups_, __Order Bys__, __Filters__
with special handling for Date Predicates. 

## Use Case 1: Indexing a Flat/Denormalized Dataset
If you have your raw event store is in **Deep Storage**(hdfs/s3) and you have a Druid index for this 
dataset, you can use the Spark-Druid package in the following way. Here we give the example of
the **flattened** TPCH dataset. Details of this is setup, and how a Druid index is created are in the
Benchmark document.

```scala
// 1. create table /register the Raw Event DataSet
sql("""
CREATE TEMPORARY TABLE orderlineitempartsupplierbase 
  ( 
     o_orderkey      INTEGER, 
     o_custkey       INTEGER, 
     o_orderstatus   STRING, 
     o_totalprice    DOUBLE, 
     o_orderdate     STRING, 
     o_orderpriority STRING, 
     o_clerk         STRING, 
     o_shippriority  INTEGER, 
     o_comment       STRING, 
     l_partkey       INTEGER, 
     l_suppkey       INTEGER, 
     l_linenumber    INTEGER, 
     l_quantity      DOUBLE, 
     l_extendedprice DOUBLE, 
...
  ) 
using com.databricks.spark.csv options (
     path "orderLineItemPartSupplierCustomer.sample/", 
     header "false", 
     delimiter "|"
     ) 
""")

// 2. create temporary table using the Druid DataSource
CREATE temporary TABLE orderlineitempartsupplier 
   using org.sparklinedata.druid options (
    sourcedataframe "orderLineItemPartSupplierBase", 
    timedimensioncolumn "l_shipdate", 
    druiddatasource "tpch", 
    druidhost "localhost", 
    druidport "8082"
)
```

* For an entire list of columns see the benchmark docs.
* Here I am loading the raw event datset using the CSV package. But it could be any
Spark DataFrame.
* The **orderlineitempartsupplier** is a DataFrame in Spark that you can query as though it has
the same Schema as the DataFrame it wraps, /orderLineItemPartSupplierBase/. It defintions contains
information about the Druid Index: connection information, column mapping information, rewrites allowed etc.

Now a Query like:

```sql
SELECT s_nation,
       c_nation,
       Year(Datetime( ` l_shipdate ` )) AS l_year,
       Sum(l_extendedprice)             AS extendedPrice
FROM   orderlineitempartsupplier
WHERE  ( ( s_nation = 'FRANCE'
           AND c_nation = 'GERMANY' )
          OR ( c_nation = 'FRANCE'
               AND s_nation = 'GERMANY' ) )
GROUP  BY s_nation,
          c_nation,
          Year(Datetime( ` l_shipdate ` ))
```

with a Logical Plan:
```scala
Aggregate [s_nation#88,c_nation#104,scalaUDF(scalaUDF(l_shipdate#71))], ...
 Project [s_nation#88,c_nation#104,l_shipdate#71,l_extendedprice#66]
  Filter (((s_nation#88 = FRANCE) && (c_nation#104 = GERMANY)) || ...
   Relation[o_orderkey#53,o_custkey#54,o_orderstatus#55,o_totalprice#56,o_orderdate#57,....
```

is rewritten to a Spark Plan:
```scala
Project [s_nation#88,c_nation#104,CAST(l_shipdate#172, IntegerType) AS...
 PhysicalRDD [s_nation#88,...], DruidRDD[12] at RDD at DruidRDD.scala:34
```

The Druid Query executed for this rewritten plan is [here](https://github.com/SparklineData/spark-druid-olap/blob/master/docs/benchmark/druid/queries/q7.json)


## Use Case: BI Acceleration using an OLAP index
Consider the same TPCH Dataset. We also support linking a Druid Index with a **Star Schema**. 
In this case the Druid Index is used to answer __Slice and Dice__ BI queries quickly. Answering such queries is the
reason for using Druid so the _acceleration_ should come as no surprise. 

What this package does is, it enables Users and Tools to continue to Query the Star Schema using SQL;
the [Druid Planner](https://github.com/SparklineData/spark-druid-olap/blob/master/src/main/scala/org/apache/spark/sql/sources/druid/DruidPlanner.scala)
 figures out __parts__ of the plan that can be pushed to Druid for very fast execution.

Here are the steps to define and query Star Schemas, the example is for the TPCH dataset:

1. Define the tables in the TPCH Star Schema. 
This is a step you would do even when Druid Rewrites were not involved.
```scala
// for e.g. one could define the Part table as
sql(s"""CREATE TEMPORARY TABLE part(p_partkey integer, p_name string,
           |      p_mfgr string, p_brand string, p_type string, p_size integer, p_container string,
           |      p_retailprice double,
           |      p_comment string)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("part")}",
      header "false", delimiter "|")""".stripMargin)
```
For details on how to setup all the tables see [StarSchemaBaseTest](https://github.com/SparklineData/spark-druid-olap/blob/master/src/test/scala/org/sparklinedata/druid/client/StarSchemaBaseTest.scala)

2. Define the Star Schema
The extra information needed for Query Rewrites are the key relationships between Fact and Dimension
tables. This is provided by  a [StarSchema Defintion](https://github.com/SparklineData/spark-druid-olap/blob/master/src/main/scala/org/sparklinedata/druid/metadata/StarSchemaInfo.scala)
For e.g. here is the json for describing the join between lineitem and orders tables.
```json
{
    "leftTable" : "lineitem",
    "rightTable" : "orders",
    "relationType" : "n-1",
    "joinCondition" : [ {
      "leftAttribute" : "l_orderkey",
      "rightAttribute" : "o_orderkey"
    } ]
}
```
For the complete Star Schema specification for TPCH see [BaseTest](https://github.com/SparklineData/spark-druid-olap/blob/master/src/test/scala/org/sparklinedata/druid/client/BaseTest.scala)

Please read the scaladoc for *StarSchema* as there are restrictions on the kinds of Star Schemas that
are supported. Most notably we don't support multiple join paths from the Fact Table to a Dimension
table. We describe how this can be overcome by using aliases/views. We also support *Snowflake* schemas.

3. Now the Fact table DataFrame is used to connect the Star Schema tables with the Druid Index. So
the lineitem DataFrame is defined as
```scala
s"""CREATE TEMPORARY TABLE lineitem
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "lineItemBase",
      timeDimensionColumn "l_shipdate",
      druidDatasource "tpch",
      druidHost "localhost",
      druidPort "8082",
      columnMapping '$colMapping',
      functionalDependencies '$functionalDependencies',
      starSchema '$starSchema')""".stripMargin
```
This is very similar to defining the DataFrame for the Flat Table Use Case, the additional Star Schema
information links the Fact Table with all the Dimension tables.

Now Star Schema Queries are rewritten as Druid Queries. So for e.g. the TPCH Q3:
```sql
      select
      o_orderkey,
      sum(l_extendedprice) as price, o_orderdate,
      o_shippriority
      from customer,
orders,
lineitem
      where c_mktsegment = 'BUILDING' and dateIsBefore(dateTime(`o_orderdate`),dateTime("1995-03-15")) and dateIsAfter(dateTime(`l_shipdate`),dateTime("1995-03-15"))
and c_custkey = o_custkey
and l_orderkey = o_orderkey
      group by o_orderkey,
      o_orderdate,
      o_shippriority
```

Its Logical Plan is:
```scala
Aggregate [o_orderkey#122,o_orderdate#126,o_shippriority#129], [o_orderkey#122,sum(l_extendedprice#184) AS price#195,o_orderdate#126,o_shippriority#129]
 Project [o_orderkey#122,o_orderdate#126,o_shippriority#129,l_extendedprice#184]
  Join Inner, Some((l_orderkey#179 = o_orderkey#122))
   Project [o_orderdate#126,o_orderkey#122,o_shippriority#129]
    Join Inner, Some((c_custkey#152 = o_custkey#123))
     Project [c_custkey#152]
      Filter (c_mktsegment#158 = BUILDING)
       Relation[c_custkey#152,c_name#153,c_address#154,c_nationkey#155,c_phone#156,c_acctbal#157,c_mktsegment#158,c_comment#159] CsvRelation(/Users/hbutani/tpch/datascale1/customer/,false,|,",null,PERMISSIVE,COMMONS,false,false,StructType(StructField(c_custkey,IntegerType,true), StructField(c_name,StringType,true), StructField(c_address,StringType,true), StructField(c_nationkey,IntegerType,true), StructField(c_phone,StringType,true), StructField(c_acctbal,DoubleType,true), StructField(c_mktsegment,StringType,true), StructField(c_comment,StringType,true)))
     Project [o_orderdate#126,o_custkey#123,o_orderkey#122,o_shippriority#129]
      Filter UDF(UDF(o_orderdate#126),UDF(1995-03-15))
       Relation[o_orderkey#122,o_custkey#123,o_orderstatus#124,o_totalprice#125,o_orderdate#126,o_orderpriority#127,o_clerk#128,o_shippriority#129,o_comment#130] CsvRelation(/Users/hbutani/tpch/datascale1/orders/,false,|,",null,PERMISSIVE,COMMONS,false,false,StructType(StructField(o_orderkey,IntegerType,true), StructField(o_custkey,IntegerType,true), StructField(o_orderstatus,StringType,true), StructField(o_totalprice,DoubleType,true), StructField(o_orderdate,StringType,true), StructField(o_orderpriority,StringType,true), StructField(o_clerk,StringType,true), StructField(o_shippriority,IntegerType,true), StructField(o_comment,StringType,true)))
   Project [l_extendedprice#184,l_orderkey#179]
    Filter UDF(UDF(l_shipdate#189),UDF(1995-03-15))
     Relation[l_orderkey#179,l_partkey#180,l_suppkey#181,l_linenumber#182,l_quantity#183,l_extendedprice#184,l_discount#185,l_tax#186,l_returnflag#187,l_linestatus#188,l_shipdate#189,l_commitdate#190,l_receiptdate#191,l_shipinstruct#192,l_shipmode#193,l_comment#194] DruidRelation(DruidRelationInfo(DruidClientInfo(localhost,8082),lineItemBase,l_shipdate,DruidDataSource(tpch,List(1993-01-01T00:00:00.000Z/1997-12-31T00:00:00.000Z),Map(s_phone -> DruidDimension(s_phone,STRING,149850,6366), p_comment -> DruidDimension(p_comment,STRING,140086,8985), o_shippriority -> DruidDimension(o_shippriority,STRING,9990,1), l_returnflag -> DruidDimension(l_returnflag,STRING,9990,2), p_name -> DruidDimension(p_name,STRING,327685,9761), l_comment -> DruidDimension(l_comment,STRING,270607,9927), l_linestatus -> DruidDimension(l_linestatus,STRING,9990,1), c_acctbal -> DruidMetric(c_acctbal,FLOAT,79920), count -> DruidMetric(count,LONG,79920), s_address -> DruidDimension(s_address,STRING,248606,6366), o_orderdate -> DruidDimension(o_orderdate,STRING,239760,150), l_shipmode -> DruidDimension(l_shipmode,STRING,42692,7), o_custkey -> DruidDimension(o_custkey,STRING,52524,4996), l_shipinstruct -> DruidDimension(l_shipinstruct,STRING,119218,4), sum_ps_availqty -> DruidMetric(sum_ps_availqty,LONG,79920), sum_l_quantity -> DruidMetric(sum_l_quantity,LONG,79920), s_nation -> DruidDimension(s_nation,STRING,70941,25), p_type -> DruidDimension(p_type,STRING,205896,150), s_region -> DruidDimension(s_region,STRING,68079,5), __time -> DruidTimeDimension(__time,LONG,99900), o_orderpriority -> DruidDimension(o_orderpriority,STRING,84011,5), p_retailprice -> DruidDimension(p_retailprice,STRING,68681,7872), c_name -> DruidDimension(c_name,STRING,179820,4996), p_mfgr -> DruidDimension(p_mfgr,STRING,139860,5), order_year -> DruidDimension(order_year,STRING,39960,2), l_receiptdate -> DruidDimension(l_receiptdate,STRING,99900,60), c_region -> DruidDimension(c_region,STRING,66885,5), s_name -> DruidDimension(s_name,STRING,179820,6366), l_linenumber -> DruidDimension(l_linenumber,STRING,9990,7), l_tax -> DruidMetric(l_tax,FLOAT,79920), s_acctbal -> DruidDimension(s_acctbal,STRING,67734,6349), p_container -> DruidDimension(p_container,STRING,75678,40), p_brand -> DruidDimension(p_brand,STRING,79920,25), l_extendedprice -> DruidMetric(l_extendedprice,FLOAT,79920), o_clerk -> DruidDimension(o_clerk,STRING,149850,999), o_orderstatus -> DruidDimension(o_orderstatus,STRING,9990,1), l_partkey -> DruidDimension(l_partkey,STRING,54346,9761), s_comment -> DruidDimension(s_comment,STRING,629822,6366), l_discount -> DruidMetric(l_discount,FLOAT,79920), p_size -> DruidDimension(p_size,STRING,18157,50), ps_comment -> DruidDimension(ps_comment,STRING,1235471,9926), l_commitdate -> DruidDimension(l_commitdate,STRING,99900,202), c_nation -> DruidDimension(c_nation,STRING,70769,25), c_comment -> DruidDimension(c_comment,STRING,726834,4996), ps_partkey -> DruidDimension(ps_partkey,STRING,54346,9761), c_address -> DruidDimension(c_address,STRING,251326,4996), ps_supplycost -> DruidMetric(ps_supplycost,FLOAT,79920), c_mktsegment -> DruidDimension(c_mktsegment,STRING,89882,5), l_suppkey -> DruidDimension(l_suppkey,STRING,38836,6366), o_totalprice -> DruidMetric(o_totalprice,FLOAT,79920), o_orderkey -> DruidDimension(o_orderkey,STRING,68082,6439), c_phone -> DruidDimension(c_phone,STRING,149850,4996), o_comment -> DruidDimension(o_comment,STRING,487951,6438), ps_suppkey -> DruidDimension(ps_suppkey,STRING,38836,6366)),8591061),Map(s_phone -> DruidDimension(s_phone,STRING,149850,6366), p_comment -> DruidDimension(p_comment,STRING,140086,8985), o_shippriority -> DruidDimension(o_shippriority,STRING,9990,1), l_returnflag -> DruidDimension(l_returnflag,STRING,9990,2), p_name -> DruidDimension(p_name,STRING,327685,9761), l_comment -> DruidDimension(l_comment,STRING,270607,9927), l_linestatus -> DruidDimension(l_linestatus,STRING,9990,1), c_acctbal -> DruidMetric(c_acctbal,FLOAT,79920), s_address -> DruidDimension(s_address,STRING,248606,6366), o_orderdate -> DruidDimension(o_orderdate,STRING,239760,150), l_shipmode -> DruidDimension(l_shipmode,STRING,42692,7), o_custkey -> DruidDimension(o_custkey,STRING,52524,4996), sn_name -> DruidDimension(s_nation,STRING,70941,25), l_shipinstruct -> DruidDimension(l_shipinstruct,STRING,119218,4), l_quantity -> DruidMetric(sum_l_quantity,LONG,79920), p_type -> DruidDimension(p_type,STRING,205896,150), o_orderpriority -> DruidDimension(o_orderpriority,STRING,84011,5), sr_name -> DruidDimension(s_region,STRING,68079,5), p_retailprice -> DruidDimension(p_retailprice,STRING,68681,7872), c_name -> DruidDimension(c_name,STRING,179820,4996), p_mfgr -> DruidDimension(p_mfgr,STRING,139860,5), l_receiptdate -> DruidDimension(l_receiptdate,STRING,99900,60), s_name -> DruidDimension(s_name,STRING,179820,6366), l_linenumber -> DruidDimension(l_linenumber,STRING,9990,7), l_tax -> DruidMetric(l_tax,FLOAT,79920), s_acctbal -> DruidDimension(s_acctbal,STRING,67734,6349), p_container -> DruidDimension(p_container,STRING,75678,40), l_shipdate -> DruidTimeDimension(__time,LONG,99900), ps_availqty -> DruidMetric(sum_ps_availqty,LONG,79920), p_brand -> DruidDimension(p_brand,STRING,79920,25), l_extendedprice -> DruidMetric(l_extendedprice,FLOAT,79920), o_clerk -> DruidDimension(o_clerk,STRING,149850,999), o_orderstatus -> DruidDimension(o_orderstatus,STRING,9990,1), l_partkey -> DruidDimension(l_partkey,STRING,54346,9761), s_comment -> DruidDimension(s_comment,STRING,629822,6366), l_discount -> DruidMetric(l_discount,FLOAT,79920), p_size -> DruidDimension(p_size,STRING,18157,50), ps_comment -> DruidDimension(ps_comment,STRING,1235471,9926), l_commitdate -> DruidDimension(l_commitdate,STRING,99900,202), c_comment -> DruidDimension(c_comment,STRING,726834,4996), ps_partkey -> DruidDimension(ps_partkey,STRING,54346,9761), c_address -> DruidDimension(c_address,STRING,251326,4996), cn_name -> DruidDimension(c_nation,STRING,70769,25), ps_supplycost -> DruidMetric(ps_supplycost,FLOAT,79920), c_mktsegment -> DruidDimension(c_mktsegment,STRING,89882,5), l_suppkey -> DruidDimension(l_suppkey,STRING,38836,6366), o_totalprice -> DruidMetric(o_totalprice,FLOAT,79920), o_orderkey -> DruidDimension(o_orderkey,STRING,68082,6439), c_phone -> DruidDimension(c_phone,STRING,149850,4996), o_comment -> DruidDimension(o_comment,STRING,487951,6438), ps_suppkey -> DruidDimension(ps_suppkey,STRING,38836,6366), cr_name -> DruidDimension(c_region,STRING,66885,5)),FunctionalDependencies(DruidDataSource(tpch,List(1993-01-01T00:00:00.000Z/1997-12-31T00:00:00.000Z),Map(s_phone -> DruidDimension(s_phone,STRING,149850,6366), p_comment -> DruidDimension(p_comment,STRING,140086,8985), o_shippriority -> DruidDimension(o_shippriority,STRING,9990,1), l_returnflag -> DruidDimension(l_returnflag,STRING,9990,2), p_name -> DruidDimension(p_name,STRING,327685,9761), l_comment -> DruidDimension(l_comment,STRING,270607,9927), l_linestatus -> DruidDimension(l_linestatus,STRING,9990,1), c_acctbal -> DruidMetric(c_acctbal,FLOAT,79920), count -> DruidMetric(count,LONG,79920), s_address -> DruidDimension(s_address,STRING,248606,6366), o_orderdate -> DruidDimension(o_orderdate,STRING,239760,150), l_shipmode -> DruidDimension(l_shipmode,STRING,42692,7), o_custkey -> DruidDimension(o_custkey,STRING,52524,4996), l_shipinstruct -> DruidDimension(l_shipinstruct,STRING,119218,4), sum_ps_availqty -> DruidMetric(sum_ps_availqty,LONG,79920), sum_l_quantity -> DruidMetric(sum_l_quantity,LONG,79920), s_nation -> DruidDimension(s_nation,STRING,70941,25), p_type -> DruidDimension(p_type,STRING,205896,150), s_region -> DruidDimension(s_region,STRING,68079,5), __time -> DruidTimeDimension(__time,LONG,99900), o_orderpriority -> DruidDimension(o_orderpriority,STRING,84011,5), p_retailprice -> DruidDimension(p_retailprice,STRING,68681,7872), c_name -> DruidDimension(c_name,STRING,179820,4996), p_mfgr -> DruidDimension(p_mfgr,STRING,139860,5), order_year -> DruidDimension(order_year,STRING,39960,2), l_receiptdate -> DruidDimension(l_receiptdate,STRING,99900,60), c_region -> DruidDimension(c_region,STRING,66885,5), s_name -> DruidDimension(s_name,STRING,179820,6366), l_linenumber -> DruidDimension(l_linenumber,STRING,9990,7), l_tax -> DruidMetric(l_tax,FLOAT,79920), s_acctbal -> DruidDimension(s_acctbal,STRING,67734,6349), p_container -> DruidDimension(p_container,STRING,75678,40), p_brand -> DruidDimension(p_brand,STRING,79920,25), l_extendedprice -> DruidMetric(l_extendedprice,FLOAT,79920), o_clerk -> DruidDimension(o_clerk,STRING,149850,999), o_orderstatus -> DruidDimension(o_orderstatus,STRING,9990,1), l_partkey -> DruidDimension(l_partkey,STRING,54346,9761), s_comment -> DruidDimension(s_comment,STRING,629822,6366), l_discount -> DruidMetric(l_discount,FLOAT,79920), p_size -> DruidDimension(p_size,STRING,18157,50), ps_comment -> DruidDimension(ps_comment,STRING,1235471,9926), l_commitdate -> DruidDimension(l_commitdate,STRING,99900,202), c_nation -> DruidDimension(c_nation,STRING,70769,25), c_comment -> DruidDimension(c_comment,STRING,726834,4996), ps_partkey -> DruidDimension(ps_partkey,STRING,54346,9761), c_address -> DruidDimension(c_address,STRING,251326,4996), ps_supplycost -> DruidMetric(ps_supplycost,FLOAT,79920), c_mktsegment -> DruidDimension(c_mktsegment,STRING,89882,5), l_suppkey -> DruidDimension(l_suppkey,STRING,38836,6366), o_totalprice -> DruidMetric(o_totalprice,FLOAT,79920), o_orderkey -> DruidDimension(o_orderkey,STRING,68082,6439), c_phone -> DruidDimension(c_phone,STRING,149850,4996), o_comment -> DruidDimension(o_comment,STRING,487951,6438), ps_suppkey -> DruidDimension(ps_suppkey,STRING,38836,6366)),8591061),List(FunctionalDependency(c_name,c_address,1-1), FunctionalDependency(c_phone,c_address,1-1), FunctionalDependency(c_name,c_mktsegment,n-1), FunctionalDependency(c_name,c_comment,1-1), FunctionalDependency(c_name,c_nation,n-1), FunctionalDependency(c_nation,c_region,n-1)),DependencyGraph([[Lscala.Enumeration$Value;@5d53ac9c)),StarSchema(StarSchemaInfo(lineitem,List(StarRelationInfo(lineitem,orders,n-1,List(EqualityCondition(l_orderkey,o_orderkey))), StarRelationInfo(lineitem,partsupp,n-1,List(EqualityCondition(l_partkey,ps_partkey), EqualityCondition(l_suppkey,ps_suppkey))), StarRelationInfo(partsupp,part,n-1,List(EqualityCondition(ps_partkey,p_partkey))), StarRelationInfo(partsupp,supplier,n-1,List(EqualityCondition(ps_suppkey,s_suppkey))), StarRelationInfo(orders,customer,n-1,List(EqualityCondition(o_custkey,c_custkey))), StarRelationInfo(customer,custnation,n-1,List(EqualityCondition(c_nationkey,cn_nationkey))), StarRelationInfo(custnation,custregion,n-1,List(EqualityCondition(cn_regionkey,cr_regionkey))), StarRelationInfo(supplier,suppnation,n-1,List(EqualityCondition(s_nationkey,sn_nationkey))), StarRelationInfo(suppnation,suppregion,n-1,List(EqualityCondition(sn_regionkey,sr_regionkey))))),StarTable(lineitem,None),Map(lineitem -> StarTable(lineitem,None), suppnation -> StarTable(suppnation,Some(StarRelation(supplier,n-1,Set((sn_nationkey,s_nationkey))))), custregion -> StarTable(custregion,Some(StarRelation(custnation,n-1,Set((cr_regionkey,cn_regionkey))))), custnation -> StarTable(custnation,Some(StarRelation(customer,n-1,Set((cn_nationkey,c_nationkey))))), supplier -> StarTable(supplier,Some(StarRelation(partsupp,n-1,Set((s_suppkey,ps_suppkey))))), suppregion -> StarTable(suppregion,Some(StarRelation(suppnation,n-1,Set((sr_regionkey,sn_regionkey))))), customer -> StarTable(customer,Some(StarRelation(orders,n-1,Set((c_custkey,o_custkey))))), orders -> StarTable(orders,Some(StarRelation(lineitem,n-1,Set((o_orderkey,l_orderkey))))), part -> StarTable(part,Some(StarRelation(partsupp,n-1,Set((p_partkey,ps_partkey))))), partsupp -> StarTable(partsupp,Some(StarRelation(lineitem,n-1,Set((ps_partkey,l_partkey), (ps_suppkey,l_suppkey)))))),Map(s_phone -> StarTable(supplier,Some(StarRelation(partsupp,n-1,Set((s_suppkey,ps_suppkey))))), cr_comment -> StarTable(custregion,Some(StarRelation(custnation,n-1,Set((cr_regionkey,cn_regionkey))))), p_comment -> StarTable(part,Some(StarRelation(partsupp,n-1,Set((p_partkey,ps_partkey))))), o_shippriority -> StarTable(orders,Some(StarRelation(lineitem,n-1,Set((o_orderkey,l_orderkey))))), l_returnflag -> StarTable(lineitem,None), p_name -> StarTable(part,Some(StarRelation(partsupp,n-1,Set((p_partkey,ps_partkey))))), l_comment -> StarTable(lineitem,None), l_linestatus -> StarTable(lineitem,None), c_acctbal -> StarTable(customer,Some(StarRelation(orders,n-1,Set((c_custkey,o_custkey))))), sn_regionkey -> StarTable(suppnation,Some(StarRelation(supplier,n-1,Set((sn_nationkey,s_nationkey))))), s_address -> StarTable(supplier,Some(StarRelation(partsupp,n-1,Set((s_suppkey,ps_suppkey))))), o_orderdate -> StarTable(orders,Some(StarRelation(lineitem,n-1,Set((o_orderkey,l_orderkey))))), cn_comment -> StarTable(custnation,Some(StarRelation(customer,n-1,Set((cn_nationkey,c_nationkey))))), sr_comment -> StarTable(suppregion,Some(StarRelation(suppnation,n-1,Set((sr_regionkey,sn_regionkey))))), l_shipmode -> StarTable(lineitem,None), o_custkey -> StarTable(orders,Some(StarRelation(lineitem,n-1,Set((o_orderkey,l_orderkey))))), sn_name -> StarTable(suppnation,Some(StarRelation(supplier,n-1,Set((sn_nationkey,s_nationkey))))), cn_regionkey -> StarTable(custnation,Some(StarRelation(customer,n-1,Set((cn_nationkey,c_nationkey))))), sn_nationkey -> StarTable(suppnation,Some(StarRelation(supplier,n-1,Set((sn_nationkey,s_nationkey))))), l_shipinstruct -> StarTable(lineitem,None), l_quantity -> StarTable(lineitem,None), sr_regionkey -> StarTable(suppregion,Some(StarRelation(suppnation,n-1,Set((sr_regionkey,sn_regionkey))))), s_nationkey -> StarTable(supplier,Some(StarRelation(partsupp,n-1,Set((s_suppkey,ps_suppkey))))), p_type -> StarTable(part,Some(StarRelation(partsupp,n-1,Set((p_partkey,ps_partkey))))), o_orderpriority -> StarTable(orders,Some(StarRelation(lineitem,n-1,Set((o_orderkey,l_orderkey))))), sr_name -> StarTable(suppregion,Some(StarRelation(suppnation,n-1,Set((sr_regionkey,sn_regionkey))))), p_retailprice -> StarTable(part,Some(StarRelation(partsupp,n-1,Set((p_partkey,ps_partkey))))), s_suppkey -> StarTable(supplier,Some(StarRelation(partsupp,n-1,Set((s_suppkey,ps_suppkey))))), c_name -> StarTable(customer,Some(StarRelation(orders,n-1,Set((c_custkey,o_custkey))))), p_mfgr -> StarTable(part,Some(StarRelation(partsupp,n-1,Set((p_partkey,ps_partkey))))), l_receiptdate -> StarTable(lineitem,None), s_name -> StarTable(supplier,Some(StarRelation(partsupp,n-1,Set((s_suppkey,ps_suppkey))))), l_linenumber -> StarTable(lineitem,None), l_tax -> StarTable(lineitem,None), s_acctbal -> StarTable(supplier,Some(StarRelation(partsupp,n-1,Set((s_suppkey,ps_suppkey))))), p_container -> StarTable(part,Some(StarRelation(partsupp,n-1,Set((p_partkey,ps_partkey))))), l_shipdate -> StarTable(lineitem,None), ps_availqty -> StarTable(partsupp,Some(StarRelation(lineitem,n-1,Set((ps_partkey,l_partkey), (ps_suppkey,l_suppkey))))), p_brand -> StarTable(part,Some(StarRelation(partsupp,n-1,Set((p_partkey,ps_partkey))))), c_nationkey -> StarTable(customer,Some(StarRelation(orders,n-1,Set((c_custkey,o_custkey))))), cn_nationkey -> StarTable(custnation,Some(StarRelation(customer,n-1,Set((cn_nationkey,c_nationkey))))), l_extendedprice -> StarTable(lineitem,None), o_clerk -> StarTable(orders,Some(StarRelation(lineitem,n-1,Set((o_orderkey,l_orderkey))))), o_orderstatus -> StarTable(orders,Some(StarRelation(lineitem,n-1,Set((o_orderkey,l_orderkey))))), l_partkey -> StarTable(lineitem,None), s_comment -> StarTable(supplier,Some(StarRelation(partsupp,n-1,Set((s_suppkey,ps_suppkey))))), l_discount -> StarTable(lineitem,None), p_size -> StarTable(part,Some(StarRelation(partsupp,n-1,Set((p_partkey,ps_partkey))))), ps_comment -> StarTable(partsupp,Some(StarRelation(lineitem,n-1,Set((ps_partkey,l_partkey), (ps_suppkey,l_suppkey))))), l_commitdate -> StarTable(lineitem,None), cr_regionkey -> StarTable(custregion,Some(StarRelation(custnation,n-1,Set((cr_regionkey,cn_regionkey))))), p_partkey -> StarTable(part,Some(StarRelation(partsupp,n-1,Set((p_partkey,ps_partkey))))), c_custkey -> StarTable(customer,Some(StarRelation(orders,n-1,Set((c_custkey,o_custkey))))), c_comment -> StarTable(customer,Some(StarRelation(orders,n-1,Set((c_custkey,o_custkey))))), ps_partkey -> StarTable(partsupp,Some(StarRelation(lineitem,n-1,Set((ps_partkey,l_partkey), (ps_suppkey,l_suppkey))))), c_address -> StarTable(customer,Some(StarRelation(orders,n-1,Set((c_custkey,o_custkey))))), cn_name -> StarTable(custnation,Some(StarRelation(customer,n-1,Set((cn_nationkey,c_nationkey))))), ps_supplycost -> StarTable(partsupp,Some(StarRelation(lineitem,n-1,Set((ps_partkey,l_partkey), (ps_suppkey,l_suppkey))))), sn_comment -> StarTable(suppnation,Some(StarRelation(supplier,n-1,Set((sn_nationkey,s_nationkey))))), c_mktsegment -> StarTable(customer,Some(StarRelation(orders,n-1,Set((c_custkey,o_custkey))))), l_suppkey -> StarTable(lineitem,None), o_totalprice -> StarTable(orders,Some(StarRelation(lineitem,n-1,Set((o_orderkey,l_orderkey))))), o_orderkey -> StarTable(orders,Some(StarRelation(lineitem,n-1,Set((o_orderkey,l_orderkey))))), l_orderkey -> StarTable(lineitem,None), c_phone -> StarTable(customer,Some(StarRelation(orders,n-1,Set((c_custkey,o_custkey))))), o_comment -> StarTable(orders,Some(StarRelation(lineitem,n-1,Set((o_orderkey,l_orderkey))))), ps_suppkey -> StarTable(partsupp,Some(StarRelation(lineitem,n-1,Set((ps_partkey,l_partkey), (ps_suppkey,l_suppkey))))), cr_name -> StarTable(custregion,Some(StarRelation(custnation,n-1,Set((cr_regionkey,cn_regionkey))))))),1000000,100000,true),None)

```
Without the Druid Index the Query will be executed by joining Customer with Orders and then 
with LineItem. The Join Output will then be Aggregated.

But The Query is Rewritten to use the Drud Index. Its Physical Plan is:
```scala
Project [cast(o_orderkey#122 as int) AS o_orderkey#122,alias-1#197 AS price#195,o_orderdate#126,cast(o_shippriority#129 as int) AS o_shippriority#129]
 Scan DruidRelation(DruidRelationInfo(DruidClientInfo(localhost,8082),lineItemBase,l_shipdate,DruidDataSource(tpch,List(1993-01-01T00:00:00.000Z/1997-12-31T00:00:00.000Z),Map(s_phone -> DruidDimension(s_phone,STRING,149850,6366), p_comment -> DruidDimension(p_comment,STRING,140086,8985), o_shippriority -> DruidDimension(o_shippriority,STRING,9990,1), l_returnflag -> DruidDimension(l_returnflag,STRING,9990,2), p_name -> DruidDimension(p_name,STRING,327685,9761), l_comment -> DruidDimension(l_comment,STRING,270607,9927), l_linestatus -> DruidDimension(l_linestatus,STRING,9990,1), c_acctbal -> DruidMetric(c_acctbal,FLOAT,79920), count -> DruidMetric(count,LONG,79920), s_address -> DruidDimension(s_address,STRING,248606,6366), o_orderdate -> DruidDimension(o_orderdate,STRING,239760,150), l_shipmode -> DruidDimension(l_shipmode,STRING,42692,7), o_custkey -> DruidDimension(o_custkey,STRING,52524,4996), l_shipinstruct -> DruidDimension(l_shipinstruct,STRING,119218,4), sum_ps_availqty -> DruidMetric(sum_ps_availqty,LONG,79920), sum_l_quantity -> DruidMetric(sum_l_quantity,LONG,79920), s_nation -> DruidDimension(s_nation,STRING,70941,25), p_type -> DruidDimension(p_type,STRING,205896,150), s_region -> DruidDimension(s_region,STRING,68079,5), __time -> DruidTimeDimension(__time,LONG,99900), o_orderpriority -> DruidDimension(o_orderpriority,STRING,84011,5), p_retailprice -> DruidDimension(p_retailprice,STRING,68681,7872), c_name -> DruidDimension(c_name,STRING,179820,4996), p_mfgr -> DruidDimension(p_mfgr,STRING,139860,5), order_year -> DruidDimension(order_year,STRING,39960,2), l_receiptdate -> DruidDimension(l_receiptdate,STRING,99900,60), c_region -> DruidDimension(c_region,STRING,66885,5), s_name -> DruidDimension(s_name,STRING,179820,6366), l_linenumber -> DruidDimension(l_linenumber,STRING,9990,7), l_tax -> DruidMetric(l_tax,FLOAT,79920), s_acctbal -> DruidDimension(s_acctbal,STRING,67734,6349), p_container -> DruidDimension(p_container,STRING,75678,40), p_brand -> DruidDimension(p_brand,STRING,79920,25), l_extendedprice -> DruidMetric(l_extendedprice,FLOAT,79920), o_clerk -> DruidDimension(o_clerk,STRING,149850,999), o_orderstatus -> DruidDimension(o_orderstatus,STRING,9990,1), l_partkey -> DruidDimension(l_partkey,STRING,54346,9761), s_comment -> DruidDimension(s_comment,STRING,629822,6366), l_discount -> DruidMetric(l_discount,FLOAT,79920), p_size -> DruidDimension(p_size,STRING,18157,50), ps_comment -> DruidDimension(ps_comment,STRING,1235471,9926), l_commitdate -> DruidDimension(l_commitdate,STRING,99900,202), c_nation -> DruidDimension(c_nation,STRING,70769,25), c_comment -> DruidDimension(c_comment,STRING,726834,4996), ps_partkey -> DruidDimension(ps_partkey,STRING,54346,9761), c_address -> DruidDimension(c_address,STRING,251326,4996), ps_supplycost -> DruidMetric(ps_supplycost,FLOAT,79920), c_mktsegment -> DruidDimension(c_mktsegment,STRING,89882,5), l_suppkey -> DruidDimension(l_suppkey,STRING,38836,6366), o_totalprice -> DruidMetric(o_totalprice,FLOAT,79920), o_orderkey -> DruidDimension(o_orderkey,STRING,68082,6439), c_phone -> DruidDimension(c_phone,STRING,149850,4996), o_comment -> DruidDimension(o_comment,STRING,487951,6438), ps_suppkey -> DruidDimension(ps_suppkey,STRING,38836,6366)),8591061),Map(s_phone -> DruidDimension(s_phone,STRING,149850,6366), p_comment -> DruidDimension(p_comment,STRING,140086,8985), o_shippriority -> DruidDimension(o_shippriority,STRING,9990,1), l_returnflag -> DruidDimension(l_returnflag,STRING,9990,2), p_name -> DruidDimension(p_name,STRING,327685,9761), l_comment -> DruidDimension(l_comment,STRING,270607,9927), l_linestatus -> DruidDimension(l_linestatus,STRING,9990,1), c_acctbal -> DruidMetric(c_acctbal,FLOAT,79920), s_address -> DruidDimension(s_address,STRING,248606,6366), o_orderdate -> DruidDimension(o_orderdate,STRING,239760,150), l_shipmode -> DruidDimension(l_shipmode,STRING,42692,7), o_custkey -> DruidDimension(o_custkey,STRING,52524,4996), sn_name -> DruidDimension(s_nation,STRING,70941,25), l_shipinstruct -> DruidDimension(l_shipinstruct,STRING,119218,4), l_quantity -> DruidMetric(sum_l_quantity,LONG,79920), p_type -> DruidDimension(p_type,STRING,205896,150), o_orderpriority -> DruidDimension(o_orderpriority,STRING,84011,5), sr_name -> DruidDimension(s_region,STRING,68079,5), p_retailprice -> DruidDimension(p_retailprice,STRING,68681,7872), c_name -> DruidDimension(c_name,STRING,179820,4996), p_mfgr -> DruidDimension(p_mfgr,STRING,139860,5), l_receiptdate -> DruidDimension(l_receiptdate,STRING,99900,60), s_name -> DruidDimension(s_name,STRING,179820,6366), l_linenumber -> DruidDimension(l_linenumber,STRING,9990,7), l_tax -> DruidMetric(l_tax,FLOAT,79920), s_acctbal -> DruidDimension(s_acctbal,STRING,67734,6349), p_container -> DruidDimension(p_container,STRING,75678,40), l_shipdate -> DruidTimeDimension(__time,LONG,99900), ps_availqty -> DruidMetric(sum_ps_availqty,LONG,79920), p_brand -> DruidDimension(p_brand,STRING,79920,25), l_extendedprice -> DruidMetric(l_extendedprice,FLOAT,79920), o_clerk -> DruidDimension(o_clerk,STRING,149850,999), o_orderstatus -> DruidDimension(o_orderstatus,STRING,9990,1), l_partkey -> DruidDimension(l_partkey,STRING,54346,9761), s_comment -> DruidDimension(s_comment,STRING,629822,6366), l_discount -> DruidMetric(l_discount,FLOAT,79920), p_size -> DruidDimension(p_size,STRING,18157,50), ps_comment -> DruidDimension(ps_comment,STRING,1235471,9926), l_commitdate -> DruidDimension(l_commitdate,STRING,99900,202), c_comment -> DruidDimension(c_comment,STRING,726834,4996), ps_partkey -> DruidDimension(ps_partkey,STRING,54346,9761), c_address -> DruidDimension(c_address,STRING,251326,4996), cn_name -> DruidDimension(c_nation,STRING,70769,25), ps_supplycost -> DruidMetric(ps_supplycost,FLOAT,79920), c_mktsegment -> DruidDimension(c_mktsegment,STRING,89882,5), l_suppkey -> DruidDimension(l_suppkey,STRING,38836,6366), o_totalprice -> DruidMetric(o_totalprice,FLOAT,79920), o_orderkey -> DruidDimension(o_orderkey,STRING,68082,6439), c_phone -> DruidDimension(c_phone,STRING,149850,4996), o_comment -> DruidDimension(o_comment,STRING,487951,6438), ps_suppkey -> DruidDimension(ps_suppkey,STRING,38836,6366), cr_name -> DruidDimension(c_region,STRING,66885,5)),FunctionalDependencies(DruidDataSource(tpch,List(1993-01-01T00:00:00.000Z/1997-12-31T00:00:00.000Z),Map(s_phone -> DruidDimension(s_phone,STRING,149850,6366), p_comment -> DruidDimension(p_comment,STRING,140086,8985), o_shippriority -> DruidDimension(o_shippriority,STRING,9990,1), l_returnflag -> DruidDimension(l_returnflag,STRING,9990,2), p_name -> DruidDimension(p_name,STRING,327685,9761), l_comment -> DruidDimension(l_comment,STRING,270607,9927), l_linestatus -> DruidDimension(l_linestatus,STRING,9990,1), c_acctbal -> DruidMetric(c_acctbal,FLOAT,79920), count -> DruidMetric(count,LONG,79920), s_address -> DruidDimension(s_address,STRING,248606,6366), o_orderdate -> DruidDimension(o_orderdate,STRING,239760,150), l_shipmode -> DruidDimension(l_shipmode,STRING,42692,7), o_custkey -> DruidDimension(o_custkey,STRING,52524,4996), l_shipinstruct -> DruidDimension(l_shipinstruct,STRING,119218,4), sum_ps_availqty -> DruidMetric(sum_ps_availqty,LONG,79920), sum_l_quantity -> DruidMetric(sum_l_quantity,LONG,79920), s_nation -> DruidDimension(s_nation,STRING,70941,25), p_type -> DruidDimension(p_type,STRING,205896,150), s_region -> DruidDimension(s_region,STRING,68079,5), __time -> DruidTimeDimension(__time,LONG,99900), o_orderpriority -> DruidDimension(o_orderpriority,STRING,84011,5), p_retailprice -> DruidDimension(p_retailprice,STRING,68681,7872), c_name -> DruidDimension(c_name,STRING,179820,4996), p_mfgr -> DruidDimension(p_mfgr,STRING,139860,5), order_year -> DruidDimension(order_year,STRING,39960,2), l_receiptdate -> DruidDimension(l_receiptdate,STRING,99900,60), c_region -> DruidDimension(c_region,STRING,66885,5), s_name -> DruidDimension(s_name,STRING,179820,6366), l_linenumber -> DruidDimension(l_linenumber,STRING,9990,7), l_tax -> DruidMetric(l_tax,FLOAT,79920), s_acctbal -> DruidDimension(s_acctbal,STRING,67734,6349), p_container -> DruidDimension(p_container,STRING,75678,40), p_brand -> DruidDimension(p_brand,STRING,79920,25), l_extendedprice -> DruidMetric(l_extendedprice,FLOAT,79920), o_clerk -> DruidDimension(o_clerk,STRING,149850,999), o_orderstatus -> DruidDimension(o_orderstatus,STRING,9990,1), l_partkey -> DruidDimension(l_partkey,STRING,54346,9761), s_comment -> DruidDimension(s_comment,STRING,629822,6366), l_discount -> DruidMetric(l_discount,FLOAT,79920), p_size -> DruidDimension(p_size,STRING,18157,50), ps_comment -> DruidDimension(ps_comment,STRING,1235471,9926), l_commitdate -> DruidDimension(l_commitdate,STRING,99900,202), c_nation -> DruidDimension(c_nation,STRING,70769,25), c_comment -> DruidDimension(c_comment,STRING,726834,4996), ps_partkey -> DruidDimension(ps_partkey,STRING,54346,9761), c_address -> DruidDimension(c_address,STRING,251326,4996), ps_supplycost -> DruidMetric(ps_supplycost,FLOAT,79920), c_mktsegment -> DruidDimension(c_mktsegment,STRING,89882,5), l_suppkey -> DruidDimension(l_suppkey,STRING,38836,6366), o_totalprice -> DruidMetric(o_totalprice,FLOAT,79920), o_orderkey -> DruidDimension(o_orderkey,STRING,68082,6439), c_phone -> DruidDimension(c_phone,STRING,149850,4996), o_comment -> DruidDimension(o_comment,STRING,487951,6438), ps_suppkey -> DruidDimension(ps_suppkey,STRING,38836,6366)),8591061),List(FunctionalDependency(c_name,c_address,1-1), FunctionalDependency(c_phone,c_address,1-1), FunctionalDependency(c_name,c_mktsegment,n-1), FunctionalDependency(c_name,c_comment,1-1), FunctionalDependency(c_name,c_nation,n-1), FunctionalDependency(c_nation,c_region,n-1)),DependencyGraph([[Lscala.Enumeration$Value;@5d53ac9c)),StarSchema(StarSchemaInfo(lineitem,List(StarRelationInfo(lineitem,orders,n-1,List(EqualityCondition(l_orderkey,o_orderkey))), StarRelationInfo(lineitem,partsupp,n-1,List(EqualityCondition(l_partkey,ps_partkey), EqualityCondition(l_suppkey,ps_suppkey))), StarRelationInfo(partsupp,part,n-1,List(EqualityCondition(ps_partkey,p_partkey))), StarRelationInfo(partsupp,supplier,n-1,List(EqualityCondition(ps_suppkey,s_suppkey))), StarRelationInfo(orders,customer,n-1,List(EqualityCondition(o_custkey,c_custkey))), StarRelationInfo(customer,custnation,n-1,List(EqualityCondition(c_nationkey,cn_nationkey))), StarRelationInfo(custnation,custregion,n-1,List(EqualityCondition(cn_regionkey,cr_regionkey))), StarRelationInfo(supplier,suppnation,n-1,List(EqualityCondition(s_nationkey,sn_nationkey))), StarRelationInfo(suppnation,suppregion,n-1,List(EqualityCondition(sn_regionkey,sr_regionkey))))),StarTable(lineitem,None),Map(lineitem -> StarTable(lineitem,None), suppnation -> StarTable(suppnation,Some(StarRelation(supplier,n-1,Set((sn_nationkey,s_nationkey))))), custregion -> StarTable(custregion,Some(StarRelation(custnation,n-1,Set((cr_regionkey,cn_regionkey))))), custnation -> StarTable(custnation,Some(StarRelation(customer,n-1,Set((cn_nationkey,c_nationkey))))), supplier -> StarTable(supplier,Some(StarRelation(partsupp,n-1,Set((s_suppkey,ps_suppkey))))), suppregion -> StarTable(suppregion,Some(StarRelation(suppnation,n-1,Set((sr_regionkey,sn_regionkey))))), customer -> StarTable(customer,Some(StarRelation(orders,n-1,Set((c_custkey,o_custkey))))), orders -> StarTable(orders,Some(StarRelation(lineitem,n-1,Set((o_orderkey,l_orderkey))))), part -> StarTable(part,Some(StarRelation(partsupp,n-1,Set((p_partkey,ps_partkey))))), partsupp -> StarTable(partsupp,Some(StarRelation(lineitem,n-1,Set((ps_partkey,l_partkey), (ps_suppkey,l_suppkey)))))),Map(s_phone -> StarTable(supplier,Some(StarRelation(partsupp,n-1,Set((s_suppkey,ps_suppkey))))), cr_comment -> StarTable(custregion,Some(StarRelation(custnation,n-1,Set((cr_regionkey,cn_regionkey))))), p_comment -> StarTable(part,Some(StarRelation(partsupp,n-1,Set((p_partkey,ps_partkey))))), o_shippriority -> StarTable(orders,Some(StarRelation(lineitem,n-1,Set((o_orderkey,l_orderkey))))), l_returnflag -> StarTable(lineitem,None), p_name -> StarTable(part,Some(StarRelation(partsupp,n-1,Set((p_partkey,ps_partkey))))), l_comment -> StarTable(lineitem,None), l_linestatus -> StarTable(lineitem,None), c_acctbal -> StarTable(customer,Some(StarRelation(orders,n-1,Set((c_custkey,o_custkey))))), sn_regionkey -> StarTable(suppnation,Some(StarRelation(supplier,n-1,Set((sn_nationkey,s_nationkey))))), s_address -> StarTable(supplier,Some(StarRelation(partsupp,n-1,Set((s_suppkey,ps_suppkey))))), o_orderdate -> StarTable(orders,Some(StarRelation(lineitem,n-1,Set((o_orderkey,l_orderkey))))), cn_comment -> StarTable(custnation,Some(StarRelation(customer,n-1,Set((cn_nationkey,c_nationkey))))), sr_comment -> StarTable(suppregion,Some(StarRelation(suppnation,n-1,Set((sr_regionkey,sn_regionkey))))), l_shipmode -> StarTable(lineitem,None), o_custkey -> StarTable(orders,Some(StarRelation(lineitem,n-1,Set((o_orderkey,l_orderkey))))), sn_name -> StarTable(suppnation,Some(StarRelation(supplier,n-1,Set((sn_nationkey,s_nationkey))))), cn_regionkey -> StarTable(custnation,Some(StarRelation(customer,n-1,Set((cn_nationkey,c_nationkey))))), sn_nationkey -> StarTable(suppnation,Some(StarRelation(supplier,n-1,Set((sn_nationkey,s_nationkey))))), l_shipinstruct -> StarTable(lineitem,None), l_quantity -> StarTable(lineitem,None), sr_regionkey -> StarTable(suppregion,Some(StarRelation(suppnation,n-1,Set((sr_regionkey,sn_regionkey))))), s_nationkey -> StarTable(supplier,Some(StarRelation(partsupp,n-1,Set((s_suppkey,ps_suppkey))))), p_type -> StarTable(part,Some(StarRelation(partsupp,n-1,Set((p_partkey,ps_partkey))))), o_orderpriority -> StarTable(orders,Some(StarRelation(lineitem,n-1,Set((o_orderkey,l_orderkey))))), sr_name -> StarTable(suppregion,Some(StarRelation(suppnation,n-1,Set((sr_regionkey,sn_regionkey))))), p_retailprice -> StarTable(part,Some(StarRelation(partsupp,n-1,Set((p_partkey,ps_partkey))))), s_suppkey -> StarTable(supplier,Some(StarRelation(partsupp,n-1,Set((s_suppkey,ps_suppkey))))), c_name -> StarTable(customer,Some(StarRelation(orders,n-1,Set((c_custkey,o_custkey))))), p_mfgr -> StarTable(part,Some(StarRelation(partsupp,n-1,Set((p_partkey,ps_partkey))))), l_receiptdate -> StarTable(lineitem,None), s_name -> StarTable(supplier,Some(StarRelation(partsupp,n-1,Set((s_suppkey,ps_suppkey))))), l_linenumber -> StarTable(lineitem,None), l_tax -> StarTable(lineitem,None), s_acctbal -> StarTable(supplier,Some(StarRelation(partsupp,n-1,Set((s_suppkey,ps_suppkey))))), p_container -> StarTable(part,Some(StarRelation(partsupp,n-1,Set((p_partkey,ps_partkey))))), l_shipdate -> StarTable(lineitem,None), ps_availqty -> StarTable(partsupp,Some(StarRelation(lineitem,n-1,Set((ps_partkey,l_partkey), (ps_suppkey,l_suppkey))))), p_brand -> StarTable(part,Some(StarRelation(partsupp,n-1,Set((p_partkey,ps_partkey))))), c_nationkey -> StarTable(customer,Some(StarRelation(orders,n-1,Set((c_custkey,o_custkey))))), cn_nationkey -> StarTable(custnation,Some(StarRelation(customer,n-1,Set((cn_nationkey,c_nationkey))))), l_extendedprice -> StarTable(lineitem,None), o_clerk -> StarTable(orders,Some(StarRelation(lineitem,n-1,Set((o_orderkey,l_orderkey))))), o_orderstatus -> StarTable(orders,Some(StarRelation(lineitem,n-1,Set((o_orderkey,l_orderkey))))), l_partkey -> StarTable(lineitem,None), s_comment -> StarTable(supplier,Some(StarRelation(partsupp,n-1,Set((s_suppkey,ps_suppkey))))), l_discount -> StarTable(lineitem,None), p_size -> StarTable(part,Some(StarRelation(partsupp,n-1,Set((p_partkey,ps_partkey))))), ps_comment -> StarTable(partsupp,Some(StarRelation(lineitem,n-1,Set((ps_partkey,l_partkey), (ps_suppkey,l_suppkey))))), l_commitdate -> StarTable(lineitem,None), cr_regionkey -> StarTable(custregion,Some(StarRelation(custnation,n-1,Set((cr_regionkey,cn_regionkey))))), p_partkey -> StarTable(part,Some(StarRelation(partsupp,n-1,Set((p_partkey,ps_partkey))))), c_custkey -> StarTable(customer,Some(StarRelation(orders,n-1,Set((c_custkey,o_custkey))))), c_comment -> StarTable(customer,Some(StarRelation(orders,n-1,Set((c_custkey,o_custkey))))), ps_partkey -> StarTable(partsupp,Some(StarRelation(lineitem,n-1,Set((ps_partkey,l_partkey), (ps_suppkey,l_suppkey))))), c_address -> StarTable(customer,Some(StarRelation(orders,n-1,Set((c_custkey,o_custkey))))), cn_name -> StarTable(custnation,Some(StarRelation(customer,n-1,Set((cn_nationkey,c_nationkey))))), ps_supplycost -> StarTable(partsupp,Some(StarRelation(lineitem,n-1,Set((ps_partkey,l_partkey), (ps_suppkey,l_suppkey))))), sn_comment -> StarTable(suppnation,Some(StarRelation(supplier,n-1,Set((sn_nationkey,s_nationkey))))), c_mktsegment -> StarTable(customer,Some(StarRelation(orders,n-1,Set((c_custkey,o_custkey))))), l_suppkey -> StarTable(lineitem,None), o_totalprice -> StarTable(orders,Some(StarRelation(lineitem,n-1,Set((o_orderkey,l_orderkey))))), o_orderkey -> StarTable(orders,Some(StarRelation(lineitem,n-1,Set((o_orderkey,l_orderkey))))), l_orderkey -> StarTable(lineitem,None), c_phone -> StarTable(customer,Some(StarRelation(orders,n-1,Set((c_custkey,o_custkey))))), o_comment -> StarTable(orders,Some(StarRelation(lineitem,n-1,Set((o_orderkey,l_orderkey))))), ps_suppkey -> StarTable(partsupp,Some(StarRelation(lineitem,n-1,Set((ps_partkey,l_partkey), (ps_suppkey,l_suppkey))))), cr_name -> StarTable(custregion,Some(StarRelation(custnation,n-1,Set((cr_regionkey,cn_regionkey))))))),1000000,100000,true),Some(DruidQuery(GroupByQuerySpec(groupBy,tpch,List(DefaultDimensionSpec(default,o_orderkey,o_orderkey), DefaultDimensionSpec(default,o_orderdate,o_orderdate), DefaultDimensionSpec(default,o_shippriority,o_shippriority)),None,None,Left(all),Some(LogicalFilterSpec(and,List(SelectorFilterSpec(selector,c_mktsegment,BUILDING), JavascriptFilterSpec(javascript,o_orderdate,function(x) { return(x < '1995-03-15T08:00:00.000Z') })))),List(FunctionAggregationSpec(doubleSum,alias-1,l_extendedprice)),None,List(1995-03-15T08:00:00.001Z/1997-12-31T00:00:00.000Z)),List(1995-03-15T08:00:00.001Z/1997-12-31T00:00:00.000Z),Some(List(DruidOperatorAttribute(ExprId(122),o_orderkey,StringType), DruidOperatorAttribute(ExprId(126),o_orderdate,StringType), DruidOperatorAttribute(ExprId(129),o_shippriority,StringType), DruidOperatorAttribute(ExprId(197),alias-1,DoubleType))))))[o_orderkey#122,o_orderdate#126,o_shippriority#129,alias-1#197]
```
The joins and aggregation operations in this case are pushed down to the Druid Index.

For more TPCH query examples see [JoinTest](https://github.com/SparklineData/spark-druid-olap/blob/master/src/test/scala/org/sparklinedata/druid/client/JoinTest.scala)

## Requirements

This library requires Spark 1.5+. It embeds the [spark-datetime package](https://github.com/SparklineData/spark-datetime)

## Linking
You can link against this library in your program at the following coordiates:

```
groupId: Sparklinedata
artifactId: spark-druid-olap
version: 0.0.2
```

## Using with Spark shell
This package can be added to  Spark using the `--packages` command line option.  For example, to include it when starting the spark shell:

```
$ bin/spark-shell --packages Sparklinedata:spark-druid-olap:0.0.1
```


## Building from Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is 
automatically downloaded by the included shell script. 
To build a JAR file simply run `build/sbt package` from the project root. 
