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

package org.sparklinedata.druid.client.test

import java.io.File
import java.util.TimeZone

import org.apache.spark.sql.hive.test.sparklinedata.TestHive
import org.apache.spark.sql.hive.test.sparklinedata.TestHive._
import org.apache.spark.sql.sources.druid.DruidPlanner
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.util.ExprUtil
import org.apache.spark.sql.{DataFrame, Row, SPLLogging}
import org.joda.time.DateTimeZone
import org.scalatest.{BeforeAndAfterAll, fixture}
import org.sparklinedata.druid.RetryUtils._
import org.sparklinedata.druid._
import org.sparklinedata.druid.client.{DruidCoordinatorClient, DruidOverlordClient}
import org.sparklinedata.druid.testenv.DruidCluster

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

class AbstractTest extends fixture.FunSuite with DruidQueryChecks with
  fixture.TestDataFixture with BeforeAndAfterAll with SPLLogging {

  def zkConnectString : String = DruidCluster.instance.zkConnectString

  def ensureDruidIndex(dataSource: String,
                       indexTask: File)(
                        numTries: Int = Int.MaxValue, start: Int = 200, cap: Int = 5000
                      ): Unit = {
    if (!DruidCluster.instance.indexExists(dataSource)) {
      val overlordClient = new DruidOverlordClient("localhost", DruidCluster.instance.overlordPort)
      val taskId: String = retryOnError(ifException[DruidDataSourceException])(
        "submit indexing task", {
          overlordClient.submitTask(indexTask)
        }
      )(numTries, start, cap)
      overlordClient.waitUntilTaskCompletes(taskId)
    }

    val coordClient = new DruidCoordinatorClient("localhost",
      DruidCluster.instance.coordinatorPort)
    retryOnError(ifException[DruidDataSourceException])(
      "index metadata", {
        coordClient.dataSourceInfo(dataSource)
      }
    )(numTries, start, cap)
  }

  override def beforeAll() = {

    System.setProperty("user.timezone", "UTC")
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    DateTimeZone.setDefault(DateTimeZone.forID("UTC"))
    TestHive.setConf(DruidPlanner.TZ_ID.key, "UTC")

    TestHive.sparkContext.setLogLevel("INFO")
    
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
