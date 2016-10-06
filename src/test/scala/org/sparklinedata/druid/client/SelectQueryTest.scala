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
import org.apache.spark.sql.hive.test.sparklinedata.TestHive._
import org.scalatest.BeforeAndAfterAll

class SelectQueryTest extends StarSchemaBaseTest with BeforeAndAfterAll with Logging {

  override def beforeAll() = {
    super.beforeAll()

    val flatStarSchema =
      """
        |{
        |  "factTable" : "orderLineItemPartSupplier_select",
        |  "relations" : []
        | }
      """.stripMargin.replace('\n', ' ')

    val cTOlap = s"""CREATE TABLE if not exists orderLineItemPartSupplier_select
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "orderLineItemPartSupplierBase",
      timeDimensionColumn "l_shipdate",
      druidDatasource "tpch",
      druidHost '$zkConnectString',
      zkQualifyDiscoveryNames "true",
      queryHistoricalServers "true",
      nonAggregateQueryHandling "push_project_and_filters",
      columnMapping '$colMapping',
      functionalDependencies '$functionalDependencies',
      starSchema '$flatStarSchema')""".stripMargin
    sql(cTOlap)

    sql(
      s"""CREATE TABLE if not exists lineitem_select
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "lineItemBase",
      timeDimensionColumn "l_shipdate",
      druidDatasource "tpch",
      druidHost '$zkConnectString',
      zkQualifyDiscoveryNames "true",
      columnMapping '$colMapping',
      numProcessingThreadsPerHistorical '1',
      nonAggregateQueryHandling "push_project_and_filters",
      functionalDependencies '$functionalDependencies',
      starSchema '${starSchema().replaceAll("lineitem", "lineitem_select")}')""".stripMargin
    )
  }

  test("basicSelect",
    "select l_returnflag, l_linestatus, " +
      "l_extendedprice as s, ps_supplycost as m, ps_availqty as a," +
      "o_orderkey  " +
      "from orderLineItemPartSupplier_select",
    1,
    true,
    true
  )

  test("projectExpressions",
    "select concat(l_returnflag, ' Flag'), l_linestatus, " +
      "l_extendedprice as s, ps_supplycost as m, ps_availqty as a," +
      "o_orderkey/100, o_orderkey  " +
      "from orderLineItemPartSupplier_select",
    1,
    true,
    true
  )

  test("projectFilterExpressions",
    "select concat(l_returnflag, ' Flag'), l_linestatus, " +
      "l_extendedprice as s, ps_supplycost as m, ps_availqty as a," +
      "o_orderkey/100, o_orderkey," +
      " sin(o_orderkey/100) > sin(10000) " +
      "from orderLineItemPartSupplier_select " +
      "where sin(o_orderkey/100) > sin(10000)",
    1,
    true,
    true
  )

  test("withAliases",
    """
       select concat(f, ' Flag'), s,
      ep as s, ps_supplycost as m, ps_availqty as a,
      sin(okey/100) > sin(10000), okey
      from (
           select l_returnflag as f, l_linestatus as s,
                  l_extendedprice as ep, ps_supplycost,
                  ps_availqty,
                  o_orderkey as okey
           from orderLineItemPartSupplier_select
           where o_orderkey > 12000
           ) as t
      where sin(okey/100) > sin(10000)
    """.stripMargin,
    1,
    true,
    true
  )

  test("columnInFilterOnly",
    """
      |select concat(l_returnflag , ' Flag'), l_linestatus
      |from orderLineItemPartSupplier_select
      |where sin(o_orderkey/100) > sin(10000) and
      |      l_shipdate <= date '1993-02-01'
    """.stripMargin,
    1,
    true,
    true
  )

  test("aggOnTop",
    """
      |select concat(l_returnflag , ' Flag'), l_linestatus, avg(ps_availqty)
      |from orderLineItemPartSupplier_select
      |where sin(o_orderkey/100) > sin(10000) and
      |      l_shipdate <= date '1993-02-01'
      |group by concat(l_returnflag , ' Flag'), l_linestatus
    """.stripMargin,
    1,
    true,
    true
  )

  test("aggOfLShipDateOnTop",
    """
      |select lyear, l_linestatus, avg(ps_availqty)
      |from (
      |select year(cast(l_shipdate as TIMESTAMP)) as lyear, l_linestatus, ps_availqty
      |from orderLineItemPartSupplier_select
      |where sin(o_orderkey/100) > sin(10000) and
      |      l_shipdate <= date '1993-02-01'
      |) q1
      |group by lyear, l_linestatus
    """.stripMargin,
    1,
    true,
    true
  )

  test("joinDruidSelectAndDruidGBy",
    """
      |select lyear, q1.l_linestatus, q2.l_linestatus
      |from (
      |select distinct year(cast(l_shipdate as TIMESTAMP)) as lyear, l_linestatus
      |from orderLineItemPartSupplier_select
      |where sin(o_orderkey/100) > sin(10000) and
      |      l_shipdate <= date '1993-02-01'
      |) q1 right outer join
      |(select distinct l_linestatus
      |from orderLineItemPartSupplier_select) q2 on q1.l_linestatus = q2.l_linestatus
    """.stripMargin,
    2,
    true,
    true
  )

  test("2tableJoin",
    "select  l_linestatus, ps_availqty " +
      "from lineitem_select li join partsupp ps on  li.l_suppkey = ps.ps_suppkey " +
      "and li.l_partkey = ps.ps_partkey ",
    1,
    true,
    true
  )

  test("3tableJoin",
    """
      |select s_name, ps_availqty
      |from partsupp ps join supplier s on ps.ps_suppkey = s.s_suppkey
      |     join lineitem_select li on li.l_suppkey = ps.ps_suppkey and
      |        li.l_partkey = ps.ps_partkey
    """.stripMargin,
    1,
    true,
    true
  )

  test("3tableJoinFilter",
    """
      |select s_name, ps_availqty
      |from partsupp ps join supplier s on ps.ps_suppkey = s.s_suppkey
      |     join lineitem_select li on li.l_suppkey = ps.ps_suppkey and
      |        li.l_partkey = ps.ps_partkey
      |where sin(ps_partkey/100) > sin(10000) and
      |      l_shipdate <= date '1993-02-01'
    """.stripMargin,
    1,
    true,
    true
  )

  test("3tableJoinPushedFilterAggOnTop",
    """
      |select s_name, avg(ps_availqty)
      |from partsupp ps join supplier s on ps.ps_suppkey = s.s_suppkey
      |     join lineitem_select li on li.l_suppkey = ps.ps_suppkey and
      |        li.l_partkey = ps.ps_partkey
      |where sin(ps_partkey/100) > sin(10000) and
      |      ps_partkey > 10 and
      |      l_shipdate <= date '1993-02-01'
      |group by s_name
    """.stripMargin,
    1,
    true,
    true
  )

  test("3tableJoinFilterAggOnTop",
    """
      |select s_name, avg(ps_availqty)
      |from partsupp ps join supplier s on ps.ps_suppkey = s.s_suppkey
      |     join lineitem_select li on li.l_suppkey = ps.ps_suppkey and
      |        li.l_partkey = ps.ps_partkey
      |where sin(ps_partkey/100) > sin(10000) and
      |      l_shipdate <= date '1993-02-01'
      |group by s_name
    """.stripMargin,
    1,
    true,
    true
  )

}
