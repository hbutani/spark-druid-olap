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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.sparklinedata.spark.dateTime.dsl.expressions._

import scala.language.postfixOps

class DruidRewriteCubeTest extends BaseTest {


  test("basicCube",
    "select l_returnflag, l_linestatus, " +
        "count(*), sum(l_extendedprice) as s " +
        "from orderLineItemPartSupplier " +
        "group by l_returnflag, l_linestatus with cube",
    4,
    true,
    true
  )

  test("testUnion",
    "select l_returnflag, l_linestatus, " +
        "count(*), sum(l_extendedprice) as s " +
        "from orderLineItemPartSupplier " +
        "group by l_returnflag, l_linestatus " +
        "union all " +
        "select l_returnflag, null, " +
        "count(*), sum(l_extendedprice) as s " +
        "from orderLineItemPartSupplier " +
        "group by l_returnflag ",
    2,
    true
  )

  /*
   * TODO: followup on the following Spark-SQL issue:
   * If there is an expression of the form fnX(fnY(..),..). So the top-level function has a fun.
   * invocation as its child expression then:
   * - ResolveFunctions rule skips the top-level invocation. The expression is left as an
   * UnResolvedFunction expression.
   * - In ResolveGroupingAnalytics rule, the UnResolvedFunction is handled as an Alias
   * - On the Expand operator, when the projections are computed when masking this expression
   * as a null, a call is made to get its dataType(basicOperators:291). This causes a
   * UnresolvedException to be thrown.
   *
   * Can be reproduced by replacing the shipDtYrGroup groupByExpr by
   * "concat(concat(l_linestatus, 'a'), 'b')". More than one level of function invocation is
   * needed, so "concat(l_linestatus, 'a')" works fine.
   */
  ignore("ShipDateYearAggCube") { td =>

    val shipDtYrGroup = dateTime('l_shipdate) year

    val df = sqlAndLog("basicAgg",
      date"""select l_returnflag, l_linestatus, $shipDtYrGroup, count(*),
      sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a,
      count(distinct o_orderkey)
      from orderLineItemPartSupplier group by l_returnflag, l_linestatus, $shipDtYrGroup
      with Cube""")
    logPlan("basicAgg", df)

    // df.show()
  }

  test("basicFilterCube",
    "select s_nation, l_returnflag, l_linestatus, " +
        "count(*), sum(l_extendedprice) as s " +
        "from orderLineItemPartSupplier " +
        "where s_nation = 'FRANCE' " +
        "group by s_nation, l_returnflag, l_linestatus with cube",
    8,
    true
  )

  test("basicFilterRollup",
    "select l_returnflag, l_linestatus, grouping__id, " +
        "count(*), sum(l_extendedprice) as s " +
        "from orderLineItemPartSupplier " +
        "where s_nation = 'FRANCE' " +
        "group by l_returnflag, l_linestatus with rollup",
    3,
    true
  )

  test("basicFilterGroupingSet",
    "select l_returnflag, l_linestatus, grouping__id, " +
        "count(*), sum(l_extendedprice) as s " +
        "from orderLineItemPartSupplier " +
        "where s_nation = 'FRANCE' " +
        "group by l_returnflag, l_linestatus grouping sets(l_returnflag, l_linestatus, ())",
    3,
    true
  )

  test("basicCubeWithExpr",
    "select lower(l_returnflag), l_linestatus, " +
        "count(*), sum(l_extendedprice) as s " +
        "from orderLineItemPartSupplier " +
        "group by lower(l_returnflag), l_linestatus with cube",
    0,
    true
  )

}
