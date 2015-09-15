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

package org.apache.spark.sql.execution.tools

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.execution.{PhysicalRDD, SparkPlan}
import org.apache.spark.sql.sources.druid.DruidPlanner
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.sparklinedata.druid.DruidRDD
import org.sparklinedata.spark.dateTime.Functions._
import org.sparklinedata.spark.dateTime.dsl.expressions._

import scala.language.postfixOps

case class Config(nameNode : String = "",
                  tpchFlatDir : String = "",
                  druidBroker : String = "",
                  druidDataSource : String = "",
                  showResults : Boolean = false)


class BenchMark private(val sqlCtx: SQLContext,
                        val nameNode : String,
                        val tpchFlatDir : String,
                        val druidBroker : String,
                        val druidDataSource : String) {

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

  register(sqlCtx)
  DruidPlanner(sqlCtx)
  registerBaseDF
  registerDruidDS

  // scalastyle:off object.name
  object queries {

    val basicAggDruid =
      sqlCtx.sql(s"""select l_returnflag, l_linestatus, count(*),
        sum(l_extendedprice) as s, max(ps_supplycost) as m,
        avg(ps_availqty) as a,count(distinct o_orderkey)
          from $druidBackedTable
          group by l_returnflag, l_linestatus""")

    val shipDtPredicateA = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val intervalAndDimFiltersDruid = sqlCtx.sql(
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

    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicate2 = dateTime('l_shipdate) > (dateTime("1995-12-01"))

    val shipDteRangeDruid = sqlCtx.sql(
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

    val shipDtPredicateL = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicateH = dateTime('l_shipdate) > (dateTime("1995-12-01"))

    val projFiltRangeDruid = sqlCtx.sql(
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
""")

    val q1Druid = sqlCtx.sql("""select l_returnflag, l_linestatus,
                            count(*), sum(l_extendedprice) as s,
              max(ps_supplycost) as m,
       avg(ps_availqty) as a,count(distinct o_orderkey)
       from orderLineItemPartSupplier
       group by l_returnflag, l_linestatus""")

    val orderDtPredicate = dateTime('o_orderdate) < dateTime("1995-03-15")
    val shipDtPredicateQ3 = dateTime('l_shipdate) > dateTime("1995-03-15")

    val q3Druid = sqlCtx.sql(date"""
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

    val orderDtPredicateQ51 = dateTime('o_orderdate) >= dateTime("1994-01-01")
    val orderDtPredicateQ52 = dateTime('o_orderdate) < (dateTime("1994-01-01") + 1.year)

    val q5Druid =  sqlCtx.sql(date"""
      select s_nation,
      sum(l_extendedprice) as extendedPrice
      from orderLineItemPartSupplier
      where s_region = 'ASIA'
      and $orderDtPredicateQ51
      and $orderDtPredicateQ52
      group by s_nation
      """)

    val shipDtQ7Year = dateTime('l_shipdate) year

    val q7Druid = sqlCtx.sql(date"""
    select s_nation, c_nation, $shipDtQ7Year as l_year,
    sum(l_extendedprice) as extendedPrice
    from orderLineItemPartSupplier
    where ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
           (c_nation = 'FRANCE' and s_nation = 'GERMANY')
           )
    group by s_nation, c_nation, $shipDtQ7Year
    """)

    val orderDtQ8Year = dateTime('o_orderdate) year

    val dtP1 = dateTime('o_orderdate) >= dateTime("1995-01-01")
    val dtP2 = dateTime('o_orderdate) <= dateTime("1996-12-31")

    val q8Druid = sqlCtx.sql(date"""
      select $orderDtQ8Year as o_year,
      sum(l_extendedprice) as price
      from orderLineItemPartSupplier
      where c_region = 'AMERICA' and p_type = 'ECONOMY ANODIZED STEEL' and $dtP1 and $dtP2
      group by $orderDtQ8Year
      """)

    val dtP1Q10 = dateTime('o_orderdate) >= dateTime("1993-10-01")
    val dtP2Q10 = dateTime('o_orderdate) < (dateTime("1993-10-01") + 3.month)

    val q10Druid = sqlCtx.sql(date"""
    select c_name, c_nation, c_address, c_phone, c_comment,
           sum(l_extendedprice) as price
    from orderLineItemPartSupplier
    where
      $dtP1Q10 and
      $dtP2Q10 and
      l_returnflag = 'R'
    group by c_name, c_nation, c_address, c_phone, c_comment
    """)



    val basicAggBase = sqlCtx.sql( s"""select l_returnflag, l_linestatus, count(*),
        sum(l_extendedprice) as s, max(ps_supplycost) as m,
        avg(ps_availqty) as a,count(distinct o_orderkey)
          from orderLineItemPartSupplierBase
          group by l_returnflag, l_linestatus""")

    val intervalAndDimFiltersBase = sqlCtx.sql(
      date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation,
          shipYear, shipMonth
         from orderLineItemPartSupplierBase
      ) t
      where
        (shipYear <= 1997)
       and $shipDtPredicateA
       and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
      group by f,s
""")

    val shipDteRangeBase = sqlCtx.sql(
      date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation,
          shipYear, shipMonth
         from orderLineItemPartSupplierBase
      ) t
      where
       shipYear <= 1997
       and shipYear >= 1995
       and $shipDtPredicate and $shipDtPredicate2
      group by f,s
      """
    )

    val projFiltRangeBase = sqlCtx.sql(
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
         l_extendedprice, ps_supplycost, ps_availqty, o_orderkey,
         shipYear, shipMonth
         from orderLineItemPartSupplierBase
         where p_type = 'ECONOMY ANODIZED STEEL'
      ) t
      where
       shipYear <= 1997
       and shipYear >= 1995
       and
          $shipDtPredicateL and
            $shipDtPredicateH and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
      group by s_nation

""")

    val q1Base = sqlCtx.sql( """select l_returnflag, l_linestatus,
                            count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m,
       avg(ps_availqty) as a,count(distinct o_orderkey)
       from orderLineItemPartSupplierBase
       group by l_returnflag, l_linestatus""")

    val q3Base = sqlCtx.sql(date"""
      select
      o_orderkey,
      sum(l_extendedprice) as price, o_orderdate,
      o_shippriority
      from
      orderLineItemPartSupplierBase where
      shipYear >= 1995 and
      c_mktsegment = 'BUILDING' and $orderDtPredicate and $shipDtPredicateQ3
      group by o_orderkey,
      o_orderdate,
      o_shippriority
      """)

    val q5Base =  sqlCtx.sql(date"""
      select s_nation,
      sum(l_extendedprice) as extendedPrice
      from orderLineItemPartSupplierBase
      where s_region = 'ASIA'
      and $orderDtPredicateQ51
      and $orderDtPredicateQ52
      group by s_nation
      """)

    val q7Base = sqlCtx.sql(date"""
    select s_nation, c_nation, $shipDtQ7Year as l_year,
    sum(l_extendedprice) as extendedPrice
    from orderLineItemPartSupplierBase
    where ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
           (c_nation = 'FRANCE' and s_nation = 'GERMANY')
           )
    group by s_nation, c_nation, $shipDtQ7Year
    """)

    val q8Base = sqlCtx.sql(date"""
      select $orderDtQ8Year as o_year,
      sum(l_extendedprice) as price
      from orderLineItemPartSupplierBase
      where c_region = 'AMERICA' and p_type = 'ECONOMY ANODIZED STEEL' and $dtP1 and $dtP2
      group by $orderDtQ8Year
      """)

    val q10Base = sqlCtx.sql(date"""
    select c_name, c_nation, c_address, c_phone, c_comment,
           sum(l_extendedprice) as price
    from orderLineItemPartSupplierBase
    where
      $dtP1Q10 and
      $dtP2Q10 and
      l_returnflag = 'R'
    group by c_name, c_nation, c_address, c_phone, c_comment
    """)

    private def getDruidRDD(p : SparkPlan) : Option[DruidRDD] = p match {
      case PhysicalRDD(_, r) if r.isInstanceOf[DruidRDD] => Some(r.asInstanceOf[DruidRDD])
      case _ if p.children.size == 1 => getDruidRDD(p.children(0))
    }

    def printDetails(df : DataFrame) : Unit = {
      print("Logical Plan:")
      print(df.queryExecution.logical.toString())
      print("Physical Plan:")
      print(df.queryExecution.sparkPlan.toString())

      val pPlan = df.queryExecution.sparkPlan
      val dR = getDruidRDD(pPlan)
      if (dR.isDefined) {
        println("Druid Query:")
        println(dR.get.dQuery)
      }

    }

  }



  private def registerBaseDF: Unit = {
    val fN = s"hdfs://${nameNode}/user/hive/${tpchFlatDir}"
    val df1 = sqlCtx.read.parquet(fN)

    val df = df1.select("o_orderkey", "l_suppkey", "l_linenumber",
      "l_quantity", "l_extendedprice",
      "l_discount", "l_tax",
      "l_returnflag","l_linestatus",
      "l_shipdate", "l_commitdate",
      "l_receiptdate", "l_shipinstruct",
      "l_shipmode",
      "o_orderdate",
      "o_shippriority",
      "order_year", "ps_partkey",
      "ps_suppkey" ,"ps_availqty",
      "ps_supplycost",
      "s_name", "s_address",
      "s_phone", "s_acctbal",
      "s_nation",
      "s_region", "p_name",
      "p_mfgr", "p_brand",
      "p_type", "p_size",
      "p_container", "p_retailprice",
      "c_name",
      "c_address", "c_phone",
      "c_acctbal","c_mktsegment",
      "c_nation", "c_comment",
      "c_region", "shipYear", "shipMonth"
    )

    df.registerTempTable(baseFlatTableName)
  }

  // scalastyle:off line.size.limit
  private def registerDruidDS : Unit = {

    sqlCtx.sql(s"""CREATE TEMPORARY TABLE $druidBackedTable
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "$baseFlatTableName",
      timeDimensionColumn "l_shipdate",
      druidDatasource "${druidDataSource}",
      druidHost "${druidBroker}",
      druidPort "8082",
      functionalDependencies '[   {"col1" : "c_name", "col2" : "c_address", "type" : "1-1"},   {"col1" : "c_phone", "col2" : "c_address", "type" : "1-1"},   {"col1" : "c_name", "col2" : "c_mktsegment", "type" : "n-1"},   {"col1" : "c_name", "col2" : "c_comment", "type" : "1-1"},   {"col1" : "c_name", "col2" : "c_nation", "type" : "n-1"},   {"col1" : "c_nation", "col2" : "c_region", "type" : "n-1"} ]     ')
""")

  }

}

object BenchMark {

  def apply(sqlCtx: SQLContext, nameNode : String,  tpchFlatDir : String,
            druidBroker : String,
            druidDataSource : String) : BenchMark = {

    val b = new BenchMark(sqlCtx, nameNode, tpchFlatDir, druidBroker, druidDataSource)
    b
  }
}

object Main {

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

    val b = BenchMark(sqlCtx, c.nameNode, c.tpchFlatDir, c.druidBroker, c.druidDataSource)

    import b.queries._

    printDetails(q1Druid)

  }
}
