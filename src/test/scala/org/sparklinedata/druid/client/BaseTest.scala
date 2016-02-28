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

package org.sparklinedata.druid.client

import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.sources.druid.DruidPlanner
import org.scalatest.{ BeforeAndAfterAll, FunSuite }
import org.sparklinedata.spark.dateTime.Functions._
import scala.io.Source
import java.io.PrintWriter
import java.io.File
import java.math.BigInteger

abstract class BaseTest extends FunSuite with BeforeAndAfterAll with Logging {

  val colMapping =
    """{
      | "l_quantity" : "sum_l_quantity",
      | "ps_availqty" : "sum_ps_availqty",
      | "cn_name" : "c_nation",
      | "cr_name" : "c_region",
      |  "sn_name" : "s_nation",
      | "sr_name" : "s_region"
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

  val flatStarSchema =
    """
      |{
      |  "factTable" : "orderLineItemPartSupplier",
      |  "relations" : []
      | }
    """.stripMargin.replace('\n', ' ')

  val starSchema =
    """
    |{
    |  "factTable" : "lineitem",
    |  "relations" : [ {
    |    "leftTable" : "lineitem",
    |    "rightTable" : "orders",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "l_orderkey",
    |      "rightAttribute" : "o_orderkey"
    |    } ]
    |  }, {
    |    "leftTable" : "lineitem",
    |    "rightTable" : "partsupp",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "l_partkey",
    |      "rightAttribute" : "ps_partkey"
    |    }, {
    |      "leftAttribute" : "l_suppkey",
    |      "rightAttribute" : "ps_suppkey"
    |    } ]
    |  }, {
    |    "leftTable" : "partsupp",
    |    "rightTable" : "part",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "ps_partkey",
    |      "rightAttribute" : "p_partkey"
    |    } ]
    |  }, {
    |    "leftTable" : "partsupp",
    |    "rightTable" : "supplier",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "ps_suppkey",
    |      "rightAttribute" : "s_suppkey"
    |    } ]
    |  }, {
    |    "leftTable" : "orders",
    |    "rightTable" : "customer",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "o_custkey",
    |      "rightAttribute" : "c_custkey"
    |    } ]
    |  }, {
    |    "leftTable" : "customer",
    |    "rightTable" : "custnation",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "c_nationkey",
    |      "rightAttribute" : "cn_nationkey"
    |    } ]
    |  }, {
    |    "leftTable" : "custnation",
    |    "rightTable" : "custregion",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "cn_regionkey",
    |      "rightAttribute" : "cr_regionkey"
    |    } ]
    |  }, {
    |    "leftTable" : "supplier",
    |    "rightTable" : "suppnation",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "s_nationkey",
    |      "rightAttribute" : "sn_nationkey"
    |    } ]
    |  }, {
    |    "leftTable" : "suppnation",
    |    "rightTable" : "suppregion",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "sn_regionkey",
    |      "rightAttribute" : "sr_regionkey"
    |    } ]
    |  } ]
    |}
  """.stripMargin.replace('\n', ' ')

  override def beforeAll() = {

    register(TestHive)
    DruidPlanner(TestHive)

    val cT = s"""CREATE TEMPORARY TABLE orderLineItemPartSupplierBase(o_orderkey integer,
             o_custkey integer,
      o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string,
      o_clerk string,
      o_shippriority integer, o_comment string, l_partkey integer, l_suppkey integer,
      l_linenumber integer,
      l_quantity double, l_extendedprice double, l_discount double, l_tax double,
      l_returnflag string,
      l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string,
      l_shipinstruct string,
      l_shipmode string, l_comment string, order_year string, ps_partkey integer,
      ps_suppkey integer,
      ps_availqty integer, ps_supplycost double, ps_comment string, s_name string, s_address string,
      s_phone string, s_acctbal double, s_comment string, s_nation string,
      s_region string, p_name string,
      p_mfgr string, p_brand string, p_type string, p_size integer, p_container string,
      p_retailprice double,
      p_comment string, c_name string , c_address string , c_phone string , c_acctbal double ,
      c_mktsegment string , c_comment string , c_nation string , c_region string)
      USING com.databricks.spark.csv
      OPTIONS (path "/Users/hbutani/tpch/datascale1/orderLineItemPartSupplierCustomer.small/",
      header "false", delimiter "|")""".stripMargin

    println(cT)
    sql(cT)

    // sql("select * from orderLineItemPartSupplierBase limit 10").show(10)

    val cTOlap = s"""CREATE TEMPORARY TABLE orderLineItemPartSupplier
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "orderLineItemPartSupplierBase",
      timeDimensionColumn "l_shipdate",
      druidDatasource "tpch",
      druidHost "localhost",
      druidPort "8082",
      columnMapping '$colMapping',
      functionalDependencies '$functionalDependencies',
      starSchema '$flatStarSchema')""".stripMargin

    println(cTOlap)
    sql(cTOlap)
  }

  def sqlAndLog(nm: String, sqlStr: String): DataFrame = {
    logInfo(s"\n$nm SQL:\n" + sqlStr)
    sql(sqlStr)
  }

  def logPlan(nm: String, df: DataFrame): Unit = {
    logInfo(s"\n$nm Plan:")
    logInfo(s"\nLogical Plan:\n" + df.queryExecution.optimizedPlan.toString)
    logInfo(s"\nPhysical Plan:\n" + df.queryExecution.sparkPlan.toString)
  }

  def removeTags(plan: String): String = {
    val cleaned = plan.replaceAll("#\\d+|@\\w+", "")
    cleaned.trim
  }

  def compareLogicalPlan(df: DataFrame, goldenFilePath: String): Boolean = {
    val goldenFileContents = Source.fromURL(getClass.getResource("/" + goldenFilePath)).getLines().mkString("\n")
    val logicalPlan = df.queryExecution.optimizedPlan.toString()

    val cleanedGolden = removeTags(goldenFileContents)
    val cleanedLogical = removeTags(logicalPlan)
    
    cleanedGolden == cleanedLogical
  }

  def comparePhysicalPlan(df: DataFrame, goldenFilePath: String): Boolean = {
    val goldenFileContents = Source.fromURL(getClass.getResource("/" + goldenFilePath)).getLines().mkString("\n")
    val physicalPlan = df.queryExecution.sparkPlan.toString()

    val cleanedGolden = removeTags(goldenFileContents)
    val cleanedPhysical = removeTags(physicalPlan)
    
    cleanedGolden == cleanedPhysical
  }
  
}

