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

import java.util.TimeZone
import java.io.File
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.test.sparklinedata.TestHive
import org.apache.spark.sql.hive.test.sparklinedata.TestHive._
import org.apache.spark.sql.sources.druid.DruidPlanner
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.util.ExprUtil
import org.joda.time.DateTimeZone
import org.scalatest.{BeforeAndAfterAll, fixture}
import org.sparklinedata.druid._
import org.sparklinedata.druid.testenv.{DruidTestCluster}
import org.sparklinedata.spark.dateTime.Functions._
import org.sparklinedata.druid.RetryUtils._

import scala.language.reflectiveCalls

trait DruidQueryChecks {

  def isTopN(dq : DruidQuery) = dq.q.isInstanceOf[TopNQuerySpec]

  def isGBy(dq : DruidQuery) = dq.q.isInstanceOf[GroupByQuerySpec]

  case class TopNThresholdCheck(limit : Int) extends (DruidQuery => Boolean) {

    def apply(dq : DruidQuery) : Boolean = {
      isTopN(dq) &&
        dq.q.asInstanceOf[TopNQuerySpec].threshold == limit
    }

  }

}

abstract class BaseTest extends fixture.FunSuite with DruidQueryChecks with
  fixture.TestDataFixture with BeforeAndAfterAll with Logging {

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

  def starSchema(factDB : String = "default",
                 dimDB : String = "default") =
  s"""
    |{
    |  "factTable" : "$factDB.lineitem",
    |  "relations" : [ {
    |    "leftTable" : "$factDB.lineitem",
    |    "rightTable" : "$dimDB.orders",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "l_orderkey",
    |      "rightAttribute" : "o_orderkey"
    |    } ]
    |  }, {
    |    "leftTable" : "$factDB.lineitem",
    |    "rightTable" : "$dimDB.partsupp",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "l_partkey",
    |      "rightAttribute" : "ps_partkey"
    |    }, {
    |      "leftAttribute" : "l_suppkey",
    |      "rightAttribute" : "ps_suppkey"
    |    } ]
    |  }, {
    |    "leftTable" : "$dimDB.partsupp",
    |    "rightTable" : "$dimDB.part",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "ps_partkey",
    |      "rightAttribute" : "p_partkey"
    |    } ]
    |  }, {
    |    "leftTable" : "$dimDB.partsupp",
    |    "rightTable" : "$dimDB.supplier",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "ps_suppkey",
    |      "rightAttribute" : "s_suppkey"
    |    } ]
    |  }, {
    |    "leftTable" : "$dimDB.orders",
    |    "rightTable" : "$dimDB.customer",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "o_custkey",
    |      "rightAttribute" : "c_custkey"
    |    } ]
    |  }, {
    |    "leftTable" : "$dimDB.customer",
    |    "rightTable" : "$dimDB.custnation",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "c_nationkey",
    |      "rightAttribute" : "cn_nationkey"
    |    } ]
    |  }, {
    |    "leftTable" : "$dimDB.custnation",
    |    "rightTable" : "$dimDB.custregion",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "cn_regionkey",
    |      "rightAttribute" : "cr_regionkey"
    |    } ]
    |  }, {
    |    "leftTable" : "$dimDB.supplier",
    |    "rightTable" : "$dimDB.suppnation",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "s_nationkey",
    |      "rightAttribute" : "sn_nationkey"
    |    } ]
    |  }, {
    |    "leftTable" : "$dimDB.suppnation",
    |    "rightTable" : "$dimDB.suppregion",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "sn_regionkey",
    |      "rightAttribute" : "sr_regionkey"
    |    } ]
    |  } ]
    |}
  """.stripMargin.replace('\n', ' ')

  def zkConnectString : String = DruidTestCluster.zkConnectString

  val olFlatCreateTable =
    s"""CREATE TABLE if not exists orderLineItemPartSupplierBase(o_orderkey integer,
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
      OPTIONS (path "src/test/resources/tpch/datascale1/orderLineItemPartSupplierCustomer.small",
      header "false", delimiter "|")""".stripMargin

  def olDruidDS(db : String = "default",
                table : String = "orderLineItemPartSupplierBase",
                dsName : String = "orderLineItemPartSupplier"
               ) =
    s"""CREATE TABLE if not exists $dsName
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "$db.$table",
      timeDimensionColumn "l_shipdate",
      druidDatasource "tpch",
      druidHost '$zkConnectString',
      zkQualifyDiscoveryNames "true",
      columnMapping '$colMapping',
      numProcessingThreadsPerHistorical '1',
      allowTopNRewrite "true",
      functionalDependencies '$functionalDependencies',
      starSchema '$flatStarSchema')""".stripMargin

  def ensureDruidIndex(dataSource: String,
                       indexTask: File)(
                        numTries: Int = Int.MaxValue, start: Int = 200, cap: Int = 5000
                      ): Unit = {
    if (!DruidTestCluster.indexExists(dataSource)) {
      val overlordClient = new DruidOverlordClient("localhost", DruidTestCluster.overlordPort)
      val taskId: String = retryOnError(ifException[DruidDataSourceException])(
        "submit indexing task", {
          overlordClient.submitTask(indexTask)
        }
      )(numTries, start, cap)
      overlordClient.waitUntilTaskCompletes(taskId)
    }

    val coordClient = new DruidCoordinatorClient("localhost", DruidTestCluster.coordinatorPort)
    retryOnError(ifException[DruidDataSourceException])(
      "index metadata", {
        coordClient.dataSourceInfo(dataSource)
      }
    )(numTries, start, cap)
  }

  override def beforeAll() = {

    val tpchIndexTask = Utils.createTempFileFromTemplate(
      "tpch_index_task.json.template",
      Map(
        ":DATA_DIR:" ->
          "src/test/resources/tpch/datascale1/orderLineItemPartSupplierCustomer.small"
      ),
      "tpch_index_task",
      "json"
    )

    System.setProperty("user.timezone", "UTC")
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    DateTimeZone.setDefault(DateTimeZone.forID("UTC"))
    TestHive.setConf(DruidPlanner.TZ_ID.key, "UTC")

    TestHive.sparkContext.setLogLevel("INFO")

    register(TestHive)
    // DruidPlanner(TestHive)

    ensureDruidIndex("tpch", tpchIndexTask)(10, 1000 * 1, 1000 * 10)

    val cT = olFlatCreateTable

    println(cT)
    sql(cT)

    TestHive.table("orderLineItemPartSupplierBase").cache()

    TestHive.setConf(DruidPlanner.SPARKLINEDATA_CACHE_TABLES_TOCHECK.key,
      "orderLineItemPartSupplierBase")

    // sql("select * from orderLineItemPartSupplierBase limit 10").show(10)

    val cTOlap = olDruidDS()

    println(cTOlap)
    sql(cTOlap)
  }

  def result(df : DataFrame) : Array[Row] = {
    df.collect()
  }

  def assertResultEquals(
                     df1 : DataFrame,
                    df2 : DataFrame
                   ) : Unit = {
    assert(result(df1) ==  result(df2))
  }

  def test(nm : String, sql : String,
           numDruidQueries : Int = 1,
           showPlan : Boolean = false,
           showResults : Boolean = false,
           debugTransforms : Boolean = false,
           dqValidators: Seq[DruidQuery => Boolean] = Seq()) : Unit = {
    test(nm) { td =>
      try {
        if (debugTransforms) turnOnTransformDebugging
        val df = sqlAndLog(nm, sql)
        assertDruidQueries(td.name, df, numDruidQueries, dqValidators)
        if (showPlan) logPlan(nm, df)
        logDruidQueries(td.name, df)
        if (showResults) {
          df.show()
        }
      } finally {
        turnOffTransformDebugging
      }
    }
  }

  def cTest(nm : String,
            dsql: String,
            bsql : String,
            debugTransforms : Boolean = false) : Unit = {
    test(nm) {td =>
      try {
        println("modifiefff")
        if (debugTransforms) turnOnTransformDebugging
        val df1 = sqlAndLog(nm, dsql)
        val df2 = sqlAndLog(nm, bsql)
        assert(isTwoDataFrameEqual(df1, df2))
      } finally {
        turnOffTransformDebugging

      }
    }
  }

  def sqlAndLog(nm : String, sqlStr : String) : DataFrame = {
    logInfo(s"\n$nm SQL:\n" + sqlStr)
    sql(sqlStr)
  }

  def logPlan(nm : String, df : DataFrame) : Unit = {
    logInfo(s"\n$nm Plan:")
    logInfo(s"\nLogical Plan:\n" + df.queryExecution.optimizedPlan.toString)
    logInfo(s"\nPhysical Plan:\n" + df.queryExecution.sparkPlan.toString)
  }

  def logDruidQueries(nm : String, df : DataFrame) : Unit = {
    logInfo(s"\n$nm Druid Queries:")
    DruidPlanner.getDruidQuerySpecs(df.queryExecution.sparkPlan).foreach {dq =>
      logInfo(s"${Utils.queryToString(dq)}")
    }
  }

  def assertDruidQueries(nm : String,
                         df : DataFrame,
                         numQueries : Int,
                         dqValidators : Seq[DruidQuery => Boolean] = Seq()) : Unit = {
    val dqbs = DruidPlanner.getDruidQuerySpecs(df.queryExecution.sparkPlan)
    assert(dqbs.length == numQueries)
    dqbs.zip(dqValidators).foreach { t =>
      val dqb = t._1
      val v = t._2
      assert(v(dqb))
    }
  }

  def turnOnTransformDebugging : Unit = {
    TestHive.setConf(DruidPlanner.DEBUG_TRANSFORMATIONS.key, "true")
  }

  def turnOffTransformDebugging : Unit = {
    TestHive.setConf(DruidPlanner.DEBUG_TRANSFORMATIONS.key, "false")
  }

  def roundValue(chooseRounding : Boolean, v : Any, dt: DataType) : Any = {
    if ( chooseRounding && v != null && ExprUtil.isNumeric(dt)) {
      BigDecimal(v.toString).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
    } else {
      v
    }
  }

  def isTwoDataFrameEqual(df1 : DataFrame,
                       df2 : DataFrame,
                       Sorted : Boolean = false,
                       chooseRounding : Boolean = true): Boolean = {


    if (df1.schema != df2.schema){
      println(
        s"""
           |different schemas issue:
           | df1 schema = ${df1.schema}
           | df2.schema = ${df2.schema}
         """.stripMargin)
      // return false
    }

    import collection.JavaConversions._

    var df11 = df1
    var df22 = df2

    if (!Sorted) {
      df11 = df11.sort(df1.columns(0), df1.columns.toSeq.slice(1, df1.columns.size):_*)
      df22 = df22.sort(df2.columns(0), df2.columns.toSeq.slice(1, df2.columns.size):_*)
    }

    var df1_list = df11.collectAsList()
    var df2_list = df22.collectAsList()

    val df1_count = df1_list.size()
    val df2_count = df2_list.size()
    if(df1_count != df2_count){
      println(df1_count + "\t" + df2_count)
      println("The row count is not equal")
      println(s"""df1=\n${df1_list.mkString("\n")}\ndf2=\n ${df2_list.mkString("\n")}""")
      return false
    }

    for (i <- 0 to df1_count.toInt - 1){
      for (j <- 0 to df11.columns.size - 1){
        val res1 = roundValue(chooseRounding, df1_list.get(i).get(j), df1.schema(j).dataType)
        val res2 = roundValue(chooseRounding, df2_list.get(i).get(j), df1.schema(j).dataType)
        if(res1 != res2){
          println(s"values in row $i, column $j don't match: ${res1} != ${res2}")
          println(s"""df1=\n${df1_list.mkString("\n")}\ndf2=\n ${df2_list.mkString("\n")}""")
          return false
        }
      }
    }
    println("The two dataframe is equal " + df1_count)
    return true
  }

}

