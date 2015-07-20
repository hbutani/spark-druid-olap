package org.sparklinedata.druid.client

import org.apache.spark.sql.sources.druid.{DruidPlanner, DruidStrategy}
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._
import org.json4s.Extraction
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.sparklinedata.druid.{DruidQuery, Utils}
import org.sparklinedata.druid.metadata.{FunctionalDependency, FunctionalDependencyType}

class DataSourceTest extends FunSuite with BeforeAndAfterAll {

  import Utils._

  val colMapping =
    """{
      | "l_quantity" : "sum_l_quantity"
      |}
    """.stripMargin.replace('\n', ' ')

  val functionalDependencies =
    """[
      |  {"col1" : "c_name", "col2" : "c_address", "type" : "1-1"},
      |  {"col1" : "c_phone", "col2" : "c_address", "type" : "1-1"},
      |  {"col1" : "c_name", "col2" : "c_mktsegment", "type" : "n-1"},
      |  {"col1" : "c_name", "col2" : "c_comment", "type" : "1-1"},
      |  {"col1" : "c_name", "col2" : "c_nation", "type" : "n-1"},
      |  {"col1" : "c_nation", "col2" : "c_region", "type" : "n-1"}
      |]
    """.stripMargin.replace('\n', ' ')

  override def beforeAll() = {

    DruidPlanner(TestSQLContext)

    val cT = s"""CREATE TEMPORARY TABLE orderLineItemPartSupplierBase(o_orderkey integer, o_custkey integer,
      o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string,
      o_shippriority integer, o_comment string, l_partkey integer, l_suppkey integer, l_linenumber integer,
      l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,
      l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string,
      l_shipmode string, l_comment string, order_year string, ps_partkey integer, ps_suppkey integer,
      ps_availqty integer, ps_supplycost double, ps_comment string, s_name string, s_address string,
      s_phone string, s_acctbal double, s_comment string, s_nation string, s_region string, p_name string,
      p_mfgr string, p_brand string, p_type string, p_size integer, p_container string, p_retailprice double,
      p_comment string, c_name string , c_address string , c_phone string , c_acctbal double ,
      c_mktsegment string , c_comment string , c_nation string , c_region string)
      USING com.databricks.spark.csv
      OPTIONS (path "/Users/hbutani/tpch/datascale1/orderLineItemPartSupplierCustomer/",
      header "false", delimiter "|")""".stripMargin

    println(cT)
    sql(cT)

    //sql("select * from orderLineItemPartSupplierBase limit 10").show(10)

    val cTOlap = s"""CREATE TEMPORARY TABLE orderLineItemPartSupplier
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "orderLineItemPartSupplierBase",
      timeDimensionColumn "l_shipdate",
      druidDatasource "tpch",
      druidHost "localhost",
      druidPort "8082",
      columnMapping '$colMapping',
      functionalDependencies '$functionalDependencies')""".stripMargin

    println(cTOlap)
    sql(cTOlap)
  }

  test("baseTable") {
    sql("select * from orderLineItemPartSupplierBase").show(10)
  }

  test("noQuery") {
    sql("select * from orderLineItemPartSupplier").show(10)
  }

  test("basicAgg") {
    val df = sql("select l_returnflag, l_linestatus, count(*) " +
      "from orderLineItemPartSupplier group by l_returnflag, l_linestatus")
    println(df.queryExecution.analyzed)
    println(df.queryExecution.sparkPlan)
  }

  test("tpchQ1") {

    val dq =
      compact(render(Extraction.decompose(new DruidQuery(TPCHQueries.q1)))).replace('\n', ' ')

    val q = s"""CREATE TEMPORARY TABLE orderLineItemPartSupplier2
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "orderLineItemPartSupplierBase",
      timeDimensionColumn "l_shipdate",
      druidDatasource "tpch",
      druidHost "localhost",
      druidPort "8082",
      columnMapping '$colMapping',
      functionalDependencies '$functionalDependencies',
      druidQuery '$dq')""".stripMargin

    println(q)

    sql(q)

    sql("select * from orderLineItemPartSupplier2").show(10)
  }

  test("tpchQ1MonthGrain") {

    val dq =
      compact(render(Extraction.decompose(new DruidQuery(TPCHQueries.q1MonthGrain)))
      ).replace('\n', ' ')

    sql(

      s"""CREATE TEMPORARY TABLE orderLineItemPartSupplier2
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "orderLineItemPartSupplierBase",
      timeDimensionColumn "l_shipdate",
      druidDatasource "tpch",
      druidHost "localhost",
      druidPort "8082",
      columnMapping '$colMapping',
      functionalDependencies '$functionalDependencies',
      druidQuery '$dq')""".stripMargin
    )

    sql("select * from orderLineItemPartSupplier2").show(10)
  }

  test("t2") {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._

    val fd = FunctionalDependency("a", "b", FunctionalDependencyType.OneToOne)
    println(pretty(render(Extraction.decompose(fd))))
  }

}
