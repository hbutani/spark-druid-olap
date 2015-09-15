/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sparklinedata.druid.tools

import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.sparklinedata.spark.dateTime.Functions._
import org.sparklinedata.spark.dateTime.dsl.expressions._
import org.apache.spark.sql.sources.druid.DruidPlanner
import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

case class Config(nameNode : String = "",
                  tpchFlatDir : String = "",
                  druidBroker : String = "",
                  druidDataSource : String = "",
                  showResults : Boolean = false)

object TpchBenchMark {

  val baseFlatTableName = "orderLineItemPartSupplierBase"
  val druidBackedTable = "orderLineItemPartSupplier"

  val tpchFlatSchema = StructType(Seq(
    StructField("o_orderkey", IntegerType),
    StructField("o_custkey", IntegerType),
    StructField("o_orderstatus", StringType),
    StructField("o_totalprice", DoubleType),
    StructField("o_orderdate", StringType),
    StructField("o_orderpriority", StringType),
    StructField("o_clerk", StringType),
    StructField("o_shippriority", IntegerType),
    StructField("o_comment", StringType),
    StructField("l_partkey", IntegerType),
    StructField("l_suppkey", IntegerType),
    StructField("l_linenumber", IntegerType),
    StructField("l_quantity", DoubleType),
    StructField("l_extendedprice", DoubleType),
    StructField("l_discount", DoubleType),
    StructField("l_tax", DoubleType),
    StructField("l_returnflag", StringType),
    StructField("l_linestatus", StringType),
    StructField("l_shipdate", StringType),
    StructField("l_commitdate", StringType),
    StructField("l_receiptdate", StringType),
    StructField("l_shipinstruct", StringType),
    StructField("l_shipmode", StringType),
    StructField("l_comment", StringType),
    StructField("order_year", StringType),
    StructField("ps_partkey", IntegerType),
    StructField("ps_suppkey", IntegerType),
    StructField("ps_availqty", IntegerType),
    StructField("ps_supplycost", DoubleType),
    StructField("ps_comment", StringType),
    StructField("s_name", StringType),
    StructField("s_address", StringType),
    StructField("s_phone", StringType),
    StructField("s_acctbal", DoubleType),
    StructField("s_comment", StringType),
    StructField("s_nation", StringType),
    StructField("s_region", StringType),
    StructField("p_name", StringType),
    StructField("p_mfgr", StringType),
    StructField("p_brand", StringType),
    StructField("p_type", StringType),
    StructField("p_size", IntegerType),
    StructField("p_container", StringType),
    StructField("p_retailprice", DoubleType),
    StructField("p_comment", StringType),
    StructField("c_name", StringType),
    StructField("c_address", StringType),
    StructField("c_phone", StringType),
    StructField("c_acctbal", DoubleType),
    StructField("c_mktsegment", StringType),
    StructField("c_comment", StringType),
    StructField("c_nation", StringType),
    StructField("c_region", StringType)
  ))

  def registerBaseDF(sqlCtx: SQLContext, cfg: Config): Unit = {
    val sc = sqlCtx.sparkContext
    val inputLocation = s"hdfs://${cfg.nameNode}/user/hive/${cfg.tpchFlatDir}"
    val rdd1 = sc.textFile(inputLocation).map(r
    => r.split("\\|")).map(p => Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8),
      p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), p(19), p(20), p(21),
      p(22), p(23), p(24), p(25), p(26), p(27), p(28), p(29), p(30), p(31), p(32), p(33), p(34),
      p(35), p(36), p(37), p(38), p(39), p(40), p(41), p(42), p(43), p(44), p(45), p(46), p(47),
      p(48), p(49), p(50), p(51), p(52))
      )
    val df1 = sqlCtx.createDataFrame(rdd1, tpchFlatSchema)
    df1.registerTempTable(baseFlatTableName)
  }

  // scalastyle:off line.size.limit
  def registerDruidDS(sqlCtx : SQLContext, cfg : Config) : Unit = {

    sqlCtx.sql(s"""CREATE TEMPORARY TABLE $druidBackedTable
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "$baseFlatTableName",
      timeDimensionColumn "l_shipdate",
      druidDatasource "${cfg.druidDataSource}",
      druidHost "${cfg.druidBroker}",
      druidPort "8082",
      functionalDependencies '[   {"col1" : "c_name", "col2" : "c_address", "type" : "1-1"},   {"col1" : "c_phone", "col2" : "c_address", "type" : "1-1"},   {"col1" : "c_name", "col2" : "c_mktsegment", "type" : "n-1"},   {"col1" : "c_name", "col2" : "c_comment", "type" : "1-1"},   {"col1" : "c_name", "col2" : "c_nation", "type" : "n-1"},   {"col1" : "c_nation", "col2" : "c_region", "type" : "n-1"} ]     ')
""")

  }

  def init(sqlCtx: SQLContext, cfg: Config): Unit = {
    register(sqlCtx)
    DruidPlanner(sqlCtx)
    registerBaseDF(sqlCtx, cfg)
    registerDruidDS(sqlCtx, cfg)
  }

  def queries(sqlCtx : SQLContext) : Seq[(String, DataFrame)] = {

    val basicAgg = ("Basic Aggregation",
      sqlCtx.sql(s"""select l_returnflag, l_linestatus, count(*),
        sum(l_extendedprice) as s, max(ps_supplycost) as m,
        avg(ps_availqty) as a,count(distinct o_orderkey)
          from $druidBackedTable
          group by l_returnflag, l_linestatus"""))

    val shipDtPredicateA = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val intervalAndDimFilters = ("ShipDate range + Nation predicate",
      sqlCtx.sql(
        date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplier
      ) t
      where $shipDtPredicateA and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
      group by f,s

""")
      )

    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicate2 = dateTime('l_shipdate) > (dateTime("1995-12-01"))

    val shipDteRange = ("Ship Date Range",

      sqlCtx.sql(
        date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplier
      ) t
      where $shipDtPredicate and $shipDtPredicate2
      group by f,s
      """
      )
      )

    val shipDtPredicateL = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicateH = dateTime('l_shipdate) > (dateTime("1995-12-01"))

    val projFiltRange = ("SubQuery + nation,Type predicates + ShipDate Range",
      sqlCtx.sql(
        date"""
      select s_nation,
      count(*) as count_order,
      sum(l_extendedprice) as s,
      max(ps_supplycost) as m,
      avg(ps_availqty) as a,
      count(distinct o_orderkey)
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate,
         s_region, s_nation, c_nation, p_type,
         l_extendedprice, ps_supplycost, ps_availqty, o_orderkey
         from orderLineItemPartSupplier
         where p_type = 'ECONOMY ANODIZED STEEL'
      ) t
      where $shipDtPredicateL and
            $shipDtPredicateH and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
      group by s_nation
"""))

    val q1 = ("TPCH Q1",
      sqlCtx.sql("""select l_returnflag, l_linestatus, count(*), sum(l_extendedprice) as s,
              max(ps_supplycost) as m,
       avg(ps_availqty) as a,count(distinct o_orderkey)
       from orderLineItemPartSupplier
       group by l_returnflag, l_linestatus""")
      )

    val orderDtPredicate = dateTime('o_orderdate) < dateTime("1995-03-15")
    val shipDtPredicateQ3 = dateTime('l_shipdate) > dateTime("1995-03-15")

    val q3 = ("TPCH Q3 - extendePrice instead of revenue; order, limit removed", sqlCtx.sql(date"""
      select
      o_orderkey,
      sum(l_extendedprice) as price, o_orderdate,
      o_shippriority
      from
      orderLineItemPartSupplier where
      c_mktsegment = 'BUILDING' and $orderDtPredicate and $shipDtPredicateQ3
      group by o_orderkey,
      o_orderdate,
      o_shippriority
      """)
      )

    val orderDtPredicateQ51 = dateTime('o_orderdate) >= dateTime("1994-01-01")
    val orderDtPredicateQ52 = dateTime('o_orderdate) < (dateTime("1994-01-01") + 1.year)

    val q5 =  ("TPCH Q5 - extendePrice instead of revenue", sqlCtx.sql(date"""
      select s_nation,
      sum(l_extendedprice) as extendedPrice
      from orderLineItemPartSupplier
      where s_region = 'ASIA'
      and $orderDtPredicateQ51
      and $orderDtPredicateQ52
      group by s_nation
      """)
      )

    val shipDtQ7Year = dateTime('l_shipdate) year

    val q7 = ("TPCH Q7 - price instead of revenue", sqlCtx.sql(date"""
    select s_nation, c_nation, $shipDtQ7Year as l_year,
    sum(l_extendedprice) as extendedPrice
    from orderLineItemPartSupplier
    where ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
           (c_nation = 'FRANCE' and s_nation = 'GERMANY')
           )
    group by s_nation, c_nation, $shipDtQ7Year
    """)
      )

    val orderDtQ8Year = dateTime('o_orderdate) year

    val dtP1 = dateTime('o_orderdate) >= dateTime("1995-01-01")
    val dtP2 = dateTime('o_orderdate) <= dateTime("1996-12-31")

    val q8 = ("TPCH Q8 - extendePrice instead of market share",  sqlCtx.sql(date"""
      select $orderDtQ8Year as o_year,
      sum(l_extendedprice) as price
      from orderLineItemPartSupplier
      where c_region = 'AMERICA' and p_type = 'ECONOMY ANODIZED STEEL' and $dtP1 and $dtP2
      group by $orderDtQ8Year
      """)
      )

    val dtP1Q10 = dateTime('o_orderdate) >= dateTime("1993-10-01")
    val dtP2Q10 = dateTime('o_orderdate) < (dateTime("1993-10-01") + 3.month)

    val q10 = ("TPCH Q10 - extendePrice instead of revenue, no group by on acctBal",
      sqlCtx.sql(date"""
    select c_name, c_nation, c_address, c_phone, c_comment,
           sum(l_extendedprice) as price
    from orderLineItemPartSupplier
    where
      $dtP1Q10 and
      $dtP2Q10 and
      l_returnflag = 'R'
    group by c_name, c_nation, c_address, c_phone, c_comment
    """)
      )


    Seq(basicAgg, shipDteRange, projFiltRange, q1, q3, q5, q7, q8 /* , q10 */)
  }

  def run(sqlCtx : SQLContext, c : Config) : Unit = {
    val qs = queries(sqlCtx)

    val results : Seq[(String, DataFrame, ArrayBuffer[Long])] =
      qs.map(q => (q._1, q._2, ArrayBuffer[Long]()))

    (0 to 5).foreach {i =>
      results.foreach { q =>
        println("running " + q._1)
        val df: DataFrame = q._2
        val times: ArrayBuffer[Long] = q._3
        val sTime = System.currentTimeMillis()
        df.collect()
        val eTime = System.currentTimeMillis()
        times += (eTime - sTime)
      }
    }

    val headings = Seq("Query", "Avg. Time", "Min. Time", "Max. Time")
    println(f"${headings(0)}%50s${headings(1)}%10s${headings(2)}%10s${headings(3)}%10s")
    results.foreach { r =>
      val qName = r._1
      val times: ArrayBuffer[Long] = r._3
      val minTime = times.min
      val maxTime = times.max
      val avgTime = times.sum / times.size

      println(f"$qName%50s $avgTime%10.3f $minTime%10d $maxTime%10d")
    }
  }

  def runShowResults(sqlCtx : SQLContext, c : Config) : Unit = {

    val qs = queries(sqlCtx)

    val results = ArrayBuffer[(String, String)]()

    qs.foreach { q =>
      val df: DataFrame = q._2
      val rows = df.collect()
      val t = (q._1, rows.mkString("\n"))
      results += t
    }


    results.foreach { r =>
      println(r._1)
      println(r._2)
    }
  }

  def main(args: Array[String]) {

    val parser = new scopt.OptionParser[Config]("tpchBenchmark") {
      head("tpchBenchmark", "0.1")
      opt[String]('n', "nameNode") required() valueName("<hostName/ip>") action { (x, c) =>
        c.copy(nameNode = x) } text("the namenode hostName/ip")
      opt[String]('t', "tpchFlatDir") required() valueName("<tpchDir>") action { (x, c) =>
        c.copy(tpchFlatDir = x) } text("the folder containing tpch flattened data")
      opt[String]('d', "druidBroker") required() valueName("<hostname/ip>") action { (x, c) =>
        c.copy(druidBroker = x) } text("the druid broker hostName/ip")
      opt[String]('d', "druidDataSource") required() valueName("<dataSourceName>") action { (x, c) =>
        c.copy(druidDataSource = x) } text("the druid dataSource")
      opt[Boolean]('r', "showResults") action { (x, c) =>
        c.copy(showResults = x)
      } text ("only show results")
      help("help") text("prints this usage text")
    }

    val c = parser.parse(args, Config()) match {
      case Some(config) => config
      case None => sys.exit(-1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("TPCHBenchmark").setMaster("local"))

    val sqlCtx = new SQLContext(sc)

    init(sqlCtx, c)

    if (c.showResults) runShowResults(sqlCtx, c) else run(sqlCtx, c)

  }
}

