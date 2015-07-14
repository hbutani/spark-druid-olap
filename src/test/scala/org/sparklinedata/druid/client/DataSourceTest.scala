package org.sparklinedata.druid.client

import org.json4s.ext.{EnumNameSerializer}
import org.scalatest.FunSuite

import org.apache.spark.sql.test.TestSQLContext._
import org.sparklinedata.druid.metadata.{FunctionalDependencyType, FunctionalDependency}

class DataSourceTest extends FunSuite {


  test("t1") {
    sql(
      s"""CREATE TEMPORARY TABLE orderLineItemPartSupplierBase(o_orderkey integer, o_custkey integer,
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
OPTIONS (path "/Users/hbutani/tpch/datascale1/orderLineItemPartSupplierCustomer/", header "false", delimiter "|")"""
    )

    //sql("select * from orderLineItemPartSupplierBase limit 10").show(10)
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

    sql(

    s"""CREATE TEMPORARY TABLE orderLineItemPartSupplier
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "orderLineItemPartSupplierBase",
      timeDimensionColumn "l_shipdate",
      druidDatasource "tpch",
      druidHost "localhost",
      druidPort "8082",
       |columnMapping '$colMapping',
       |functionalDependencies '$functionalDependencies')""".stripMargin
    )
  }

  test("t2") {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    implicit val formats = DefaultFormats + new EnumNameSerializer(FunctionalDependencyType)

    val fd = FunctionalDependency("a", "b", FunctionalDependencyType.OneToOne)
    println(pretty(render(Extraction.decompose(fd))))
  }

}
