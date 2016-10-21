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

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.SPLLogging
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.hive.test.sparklinedata.TestHive._
import org.scalatest.BeforeAndAfterAll
import org.sparklinedata.spark.dateTime.dsl.expressions._

import scala.language.postfixOps

class HistoricalServerCTest extends StarSchemaBaseTest with BeforeAndAfterAll with SPLLogging {
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
      druidHost '$zkConnectString',
      zkQualifyDiscoveryNames "true",
      queryHistoricalServers "true",
      columnMapping '$colMapping',
      numProcessingThreadsPerHistorical '1',
      functionalDependencies '$functionalDependencies',
      starSchema '$flatStarSchemaHistorical')""".stripMargin
    )

    sql(
      s"""CREATE TABLE if not exists lineitem_historical
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "lineitembase",
      timeDimensionColumn "l_shipdate",
      druidDatasource "tpch",
      druidHost '$zkConnectString',
      zkQualifyDiscoveryNames "true",
      queryHistoricalServers "true",
      columnMapping '$colMapping',
      numProcessingThreadsPerHistorical '1',
      functionalDependencies '$functionalDependencies',
      starSchema '$starSchemaHistorical')""".stripMargin
    )
  }


  cTest("hscT1",
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
         from orderLineItemPartSupplier
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
         from orderLineItemPartSupplierBase
         where p_type = 'ECONOMY ANODIZED STEEL'
      ) t
      where $shipDtPredicate and
            $shipDtPredicate2 and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
      group by s_nation
      order by s_nation
"""
    }
  )

  cTest("hscT2",
    "select l_returnflag, l_linestatus, " +
      "count(*), " +
      "round(sum(l_extendedprice),2) as s " +
      "from orderLineItemPartSupplier" +
      " where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-07' " +
      "group by l_returnflag, l_linestatus with cube",
    "select l_returnflag, l_linestatus, " +
      "count(*), " +
      "round(sum(l_extendedprice),2) as s " +
      "from orderLineItemPartSupplierBase " +
      "where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-07' " +
      "group by l_returnflag, l_linestatus with cube"
  )

  // javascript test
  cTest("hscT4",
    "select sum(c_acctbal) as bal from orderLineItemPartSupplier " +
      "where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-07' group by " +
      "(substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate), 'T00:00:00.000')" +
      " AS TIMESTAMP)), 5) AS TIMESTAMP), 0, 10)) order by bal",
    "select sum(c_acctbal) as bal from orderLineItemPartSupplierBase " +
      "where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-07' group by " +
      "(substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate), 'T00:00:00.000')" +
      " AS TIMESTAMP)), 5) AS TIMESTAMP), 0, 10)) order by bal"
  )
}