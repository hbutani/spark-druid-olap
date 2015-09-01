# Spark Druid Package

Spark-Druid package enables *Logical Plans* written against a **raw event dataset** to be rewritten
to take advantage of a **[Drud Index](http://druid.io/)** of the Event data. It comprises of a [Druid DataSource](https://github.com/SparklineData/spark-druid-olap/blob/master/src/main/scala/org/sparklinedata/druid/DefaultSource.scala) 
that wraps the _raw event dataset_, and a [Druid Planner](https://github.com/SparklineData/spark-druid-olap/blob/master/src/main/scala/org/apache/spark/sql/sources/druid/DruidPlanner.scala) 
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
  the fast access and aggregation capabilities of Druid**. *Currently we are focused on this use case*. 
- Longer term , we envision using the Druid technology as an OLAP
  index for Star Schemas residing in Spark. This is a classic OLAP
  space/performance tradeoff: a specialized secondary index to speed
  up queries.

Please read the [design document](https://github.com/SparklineData/spark-druid-olap/blob/master/docs/SparkDruid.pdf) 
for details on Spark Druid connectivity, and the Rewrite Rules in place(and planned). We have **benchmarked** a set
of representative queries from the [TPCH benchmark](http://www.tpc.org/tpch/spec/tpch2.8.0.pdf). 
For slice and dice queries like TPCH Q7 we see an improvement in the order of 10x. The benchmark results
and an analysis are in the Design document(a more detailed description of the benchmark is available 
[here](https://github.com/SparklineData/spark-druid-olap/blob/master/docs/benchmark/BenchMarkDetails.pdf))

So if you have your raw event store is in **Deep Storage**(hdfs/s3) and you have a Druid index for this 
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



## Requirements

This library requires Spark 1.4+. It embeds the [spark-datetime package](https://github.com/SparklineData/spark-datetime)

## Linking
You can link against this library in your program at the following coordiates:

```
groupId: Sparklinedata
artifactId: spark-druid-olap
version: 0.0.1
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
