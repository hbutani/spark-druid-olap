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

class DruidRewriteOrderTest extends BaseTest {


  test("basicAggOrderByDimension") {
    val df = sqlAndLog("basicAggOrderByDimension",
      "select l_returnflag, l_linestatus, " +
        "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
        "from orderLineItemPartSupplier " +
        "group by l_returnflag, l_linestatus " +
        "order by l_linestatus")
    logPlan("basicAggOrderByDimension", df)

    //df.show()
  }

  test("basicAggOrderByDimensionLimit") {
    val df = sqlAndLog("basicAggOrderByDimensionLimit",
      "select l_returnflag, l_linestatus, " +
        "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a," +
        "count(distinct o_orderkey)  " +
        "from orderLineItemPartSupplier " +
        "group by l_returnflag, l_linestatus " +
        "order by l_returnflag " +
        "limit 2")
    logPlan("basicAggOrderByDimensionLimit", df)

    //df.show()
  }

  /*
   * SPARK-10437 is only fixed in Spark-1.6, but the issue here was translation of
   * SortOrder clause.
   */
  test("basicAggOrderByMetric") {
    val df = sqlAndLog("basicAggOrderByMetric",
      "select l_returnflag, l_linestatus, " +
        "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
        "from orderLineItemPartSupplier " +
        "group by l_returnflag, l_linestatus " +
        "order by count(*)")
    logPlan("basicAggOrderByMetric", df)

    df.show()
  }

  test("basicAggOrderByMetric2") {
    val df = sqlAndLog("basicAggOrderByMetric2",
      "select l_returnflag, l_linestatus, " +
        "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
        "from orderLineItemPartSupplier " +
        "group by l_returnflag, l_linestatus " +
        "order by s")
    logPlan("basicAggOrderByMetric2", df)

    //df.show()
  }

  test("basicAggOrderByLimitFull") {
    val df = sqlAndLog("basicAggOrderByLimitFull",
      "select l_returnflag as r, l_linestatus as ls, " +
        "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
        "from orderLineItemPartSupplier " +
        "group by l_returnflag, l_linestatus " +
        "order by s, ls, r " +
        "limit 3")
    logPlan("basicAggOrderByLimitFull", df)

    //df.show()
  }

  test("basicAggOrderByLimitFull2") {
    val df = sqlAndLog("basicAggOrderByLimitFull2",
      "select l_returnflag as r, l_linestatus as ls, " +
        "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
        "from orderLineItemPartSupplier " +
        "group by l_returnflag, l_linestatus " +
        "order by m desc, s, r " +
        "limit 3")
    logPlan("basicAggOrderByLimitFull2", df)


    //df.show()
  }

  test("sortNotPushed") {
    val df = sqlAndLog("sortNotPushed",
      "select l_returnflag as r, l_linestatus as ls, " +
        "count(*) + 1 as c, sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
        "from orderLineItemPartSupplier " +
        "group by l_returnflag, l_linestatus " +
        "order by c " +
        "limit 3")
    logPlan("sortNotPushed", df)

    //df.show()
  }

}
