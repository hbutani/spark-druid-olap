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

import org.apache.spark.sql.hive.test.TestHive

class JoinTest extends StarSchemaBaseTest {

  test("2tableJoin",
    "select  l_linestatus, sum(ps_availqty) " +
        "from lineitem li join partsupp ps on  li.l_suppkey = ps.ps_suppkey " +
        "and li.l_partkey = ps.ps_partkey " +
        "group by l_linestatus",
    1,
    true
  )

  test("2tableJoinFactTableIsRight",
    "select  l_linestatus, sum(ps_availqty) " +
        "from partsupp ps join lineitem li  on  li.l_suppkey = ps.ps_suppkey " +
        "and li.l_partkey = ps.ps_partkey " +
        "group by l_linestatus",
    1,
    true
  )

  test("3tableJoin",
    "select  s_name, sum(ps_availqty) " +
        "from lineitem li join partsupp ps on  li.l_suppkey = ps.ps_suppkey " +
        "and li.l_partkey = ps.ps_partkey " +
        " join supplier s on ps.ps_suppkey = s.s_suppkey " +
        "group by s_name",
    1,
    true
  )

  test("tpchQ3",StarSchemaTpchQueries.q3,
    1,
    true,
    true
  )

  test("tpchQ5", StarSchemaTpchQueries.q5,
    1,
    true,
    true
  )

  test("tpchQ7", StarSchemaTpchQueries.q7,
    1,
    true,
    true
  )

  test("tpchQ8", StarSchemaTpchQueries.q8,
    1,
    true,
    true
  )

  test("tpchQ10", StarSchemaTpchQueries.q10,
    1,
    true,
    true
  )

  test("basicJoinAgg",
    "select s_name, l_linestatus, " +
        "count(*), sum(l_extendedprice) as s " +
        "from lineitembase li join supplier s on  li.l_suppkey = s.s_suppkey " +
        "group by s_name, l_linestatus",
    0,
    true
  )

  test("dfPlan1") { td =>
    val df = TestHive.table("lineitem").groupBy("l_linestatus").count()
    logPlan("basicAggOrderByDimension", df)
    df.show()
  }

  test("dfPlan2") { td =>
    val df = TestHive.table("lineitem").
      join(TestHive.table("partsupp")).
      join(TestHive.table("supplier")).
      where("l_suppkey = ps_suppkey and l_partkey = ps_partkey and ps_suppkey = s_suppkey").
      groupBy("s_name", "l_linestatus").sum("l_extendedprice")
    logPlan("basicAggOrderByDimension", df)
    df.show()
  }

  test("dimensionOnlyQuery",
    """SELECT customer.c_mktsegment AS c_mktsegment
      |FROM   (SELECT *
      |        FROM   lineitem) lineitem
      |       JOIN (SELECT *
      |             FROM   orders) orders
      |         ON ( lineitem.l_orderkey = orders.o_orderkey )
      |       JOIN (SELECT *
      |             FROM   customer) customer
      |         ON ( orders.o_custkey = customer.c_custkey )
      |       JOIN (SELECT *
      |             FROM   custnation) custnation
      |         ON ( customer.c_nationkey = custnation.cn_nationkey )
      |       JOIN (SELECT *
      |             FROM   custregion) custregion
      |         ON ( custnation.cn_regionkey = custregion.cr_regionkey )
      |GROUP  BY customer.c_mktsegment """.stripMargin,
    1,
    true
  )

}
