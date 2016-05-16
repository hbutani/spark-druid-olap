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

import com.github.nscala_time.time.Imports._
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.hive.test.sparklinedata.TestHive
import org.apache.spark.sql.hive.test.sparklinedata.TestHive._
import org.apache.spark.sql.sources.druid.DruidPlanner
import org.scalatest.BeforeAndAfterAll
import org.sparklinedata.spark.dateTime.dsl.expressions._

import scala.language.postfixOps

class HistoricalServerTest extends StarSchemaBaseTest with BeforeAndAfterAll with Logging {

  val flatStarSchemaHistorical =
    """
      |{
      |  "factTable" : "orderLineItemPartSupplier_historical",
      |  "relations" : []
      | }
    """.stripMargin.replace('\n', ' ')

  val starSchemaHistorical =
    """
      |{
      |  "factTable" : "lineitem_historical",
      |  "relations" : [ {
      |    "leftTable" : "lineitem_historical",
      |    "rightTable" : "orders",
      |    "relationType" : "n-1",
      |    "joinCondition" : [ {
      |      "leftAttribute" : "l_orderkey",
      |      "rightAttribute" : "o_orderkey"
      |    } ]
      |  }, {
      |    "leftTable" : "lineitem_historical",
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
    super.beforeAll()

    sql(
      s"""CREATE TABLE if not exists orderLineItemPartSupplier_historical
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "orderLineItemPartSupplierBase",
      timeDimensionColumn "l_shipdate",
      druidDatasource "tpch",
      druidHost "localhost",
      queryHistoricalServers "true",
      columnMapping '$colMapping',
      functionalDependencies '$functionalDependencies',
      starSchema '$flatStarSchemaHistorical')""".stripMargin
    )

    sql(
      s"""CREATE TABLE if not exists lineitem_historical
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "lineitembase",
      timeDimensionColumn "l_shipdate",
      druidDatasource "tpch",
      druidHost "localhost",
      queryHistoricalServers "true",
      columnMapping '$colMapping',
      functionalDependencies '$functionalDependencies',
      starSchema '$starSchemaHistorical')""".stripMargin
    )
  }

  def checkEqualResultStrings(s1 : String, s2 : String) : Unit = {
    val l1 = s1.split("\n").sorted
    val l2 = s2.split("\n").sorted
    assert(l1.length == l2.length)
    l1.zip(l2).forall {
      case (s1, s2) => s1 == s2
    }
  }

  def checkHistoricalQueries(df : DataFrame,
                             runInHistorical : Boolean) : Unit = {
    assert(
      DruidPlanner.getDruidQuerySpecs(df.queryExecution.sparkPlan).forall { dq =>
        runInHistorical == dq.queryHistoricalServer
    }
    )
  }

  def testCompare(nm: String,
           tableName: String,
           sqlTemplate: String,
           numDruidQueries: Int = 1,
                  runInHistorical : Boolean = true,
           showPlan: Boolean = true,
           showResults: Boolean = true,
           debugTransforms: Boolean = false): Unit = {
    import org.apache.spark.sql.DataFrameUtils._
    test(nm) { td =>
      try {
        if (debugTransforms) turnOnTransformDebugging
        val df1 = sqlAndLog(nm, sqlTemplate.format(tableName))
        assertDruidQueries(td.name, df1, numDruidQueries)

        if (showPlan) logPlan(nm, df1)
        logDruidQueries(td.name, df1)
        logPlan(nm, df1)
        logDruidQueries(td.name, df1)
        val r1 = if (showResults) df1.dumpResult else null

        val df2 = sqlAndLog(nm, sqlTemplate.format(tableName + "_historical"))
        assertDruidQueries(td.name, df2, numDruidQueries)
        logPlan(nm, df2)
        logDruidQueries(td.name, df2)
        checkHistoricalQueries(df2, runInHistorical)
        val r2 = if (showResults) df2.dumpResult else null

        if (showResults) {
          println("Query Result run against Broker:")
          println(r1)
          println("Query Result run against Historicals:")
          println(r2)
        }

        checkEqualResultStrings(r1, r2)

      } finally {
        turnOffTransformDebugging
      }
    }

  }

  testCompare("projFilterAgg",
    "orderLineItemPartSupplier",
    {
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicate2 = dateTime('l_shipdate) > (dateTime("1995-12-01"))

    date"""
      select s_nation,
      round(count(*),2) as count_order,
      round(sum(l_extendedprice),2) as s,
      round(max(ps_supplycost),2) as m,
      round(avg(ps_availqty),2) as a,
      count(distinct o_orderkey)
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate,
         s_region, s_nation, c_nation, p_type,
         l_extendedprice, ps_supplycost, ps_availqty, o_orderkey
         from %s
         where p_type = 'ECONOMY ANODIZED STEEL'
      ) t
      where $shipDtPredicate and
            $shipDtPredicate2 and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
      group by s_nation
      order by s_nation
"""
  },
    2,
    true
  )

  testCompare("basicCube",
    "orderLineItemPartSupplier",
    "select l_returnflag, l_linestatus, " +
      "count(*), " +
      "round(sum(l_extendedprice),2) as s " +
      "from %s " +
      "group by l_returnflag, l_linestatus with cube",
    4,
    true
  )

  // join test
  val q10DtP1 = dateTime('o_orderdate) >= dateTime("1993-10-01")
  val q10DtP2 = dateTime('o_orderdate) < (dateTime("1993-10-01") + 3.month)
  val q10Template = date"""
    select c_name, cn_name, c_address, c_phone, c_comment,
           round(sum(l_extendedprice),2) as price
    from customer,
    orders, %s, custnation
    where c_custkey = o_custkey
               |and l_orderkey = o_orderkey
               |and c_nationkey = cn_nationkey and
      $q10DtP1 and
      $q10DtP2 and
      l_returnflag = 'R'
    group by c_name, cn_name, c_address, c_phone, c_comment
    """.stripMargin

  testCompare("tpchQ10",
    "lineitem",
    q10Template,
    1,
    true
  )

  // javascript test
  testCompare("gbexprtest1",
    "orderLineItemPartSupplier",
    "select sum(c_acctbal) as bal from %s group by " +
      "(substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate), 'T00:00:00.000')" +
      " AS TIMESTAMP)), 5) AS TIMESTAMP), 0, 10)) order by bal",
    1,
    false)


}
