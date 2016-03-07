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

class SparkDateTimeTest extends StarSchemaBaseTest {


  test("dateBasic") {
    //turnOnTransformDebugging

    val df = sqlAndLog("dateBasic2",
      """SELECT
        |sn_name,
        |sum(l_extendedprice) as revenue
        |FROM
        |customer,
        |orders,
        |lineitem,
        |partsupp,
        |supplier,
        |suppnation,
        |suppregion
        |WHERE
        |c_custkey = o_custkey
        |AND l_orderkey = o_orderkey
        |and l_suppkey = ps_suppkey
        |and l_partkey = ps_partkey
        |and ps_suppkey = s_suppkey
        |AND s_nationkey = sn_nationkey
        |AND sn_regionkey = sr_regionkey
        |AND sr_name = 'ASIA'
        |AND o_orderdate >= date '1994-01-01'
        |AND o_orderdate < date '1994-01-01' + interval '1' year
        |GROUP BY
        |sn_name
        |ORDER BY
        |revenue desc""".stripMargin
    )
    logDruidQuery("basicJoin", df)
  }

  test("castDate") {
    val df=sqlAndLog("castDate",
      "select cast(o_orderdate as date) " +
        "from orderLineItemPartSupplier " +
        "group by cast(o_orderdate as date)"
    )
    logDruidQuery("castDate", df)
    //logPlan("castDate", df)

    df.show()
  }

  test("showByYear") {
    val df = sqlAndLog("showByYear",
      """SELECT Year(Cast(Concat(To_date(lineitem.l_shipdate), ' 00:00:00') AS TIMESTAMP)
        |       ) AS
        |       yr_l_shipdate_ok
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
        |GROUP  BY Year(Cast(Concat(To_date(lineitem.l_shipdate), ' 00:00:00') AS
        |                    TIMESTAMP)) """.stripMargin
    )
    logDruidQuery("castDate", df)
    logPlan("showByYear", df)

    //df.show()
  }

  test("showByQuarter") {
    val df = sqlAndLog("showByQuarter",
    """
      |SELECT Cast(Concat(Year(Cast(Concat(To_date(lineitem.l_shipdate), ' 00:00:00')
      |                             AS
      |                                    TIMESTAMP)
      |                               ), ( CASE
      |            WHEN Month(Cast(Concat(To_date(lineitem.l_shipdate), ' 00:00:00') AS
      |                            TIMESTAMP
      |                       )) < 4 THEN '-01'
      |            WHEN Month(Cast(Concat(To_date(lineitem.l_shipdate), ' 00:00:00') AS
      |                            TIMESTAMP
      |                       )) < 7 THEN '-04'
      |            WHEN Month(Cast(Concat(To_date(lineitem.l_shipdate), ' 00:00:00') AS
      |                            TIMESTAMP
      |                       )) < 10 THEN '-07'
      |            ELSE '-10'
      |            END ), '-01 00:00:00') AS TIMESTAMP) AS tqr_l_shipdate_ok
      |FROM   (SELECT *
      |        FROM   lineitembase) lineitem
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
      |GROUP  BY Cast(Concat(Year(Cast(
      |                      Concat(To_date(lineitem.l_shipdate), ' 00:00:00') AS
      |                                TIMESTAMP)
      |                           ), ( CASE
      |               WHEN Month(Cast(Concat(To_date(lineitem.l_shipdate), ' 00:00:00')
      |                               AS
      |                               TIMESTAMP
      |                          )) < 4 THEN '-01'
      |               WHEN Month(Cast(Concat(To_date(lineitem.l_shipdate), ' 00:00:00')
      |                               AS
      |                               TIMESTAMP
      |                          )) < 7 THEN '-04'
      |               WHEN Month(Cast(Concat(To_date(lineitem.l_shipdate), ' 00:00:00')
      |                               AS
      |                               TIMESTAMP
      |                          )) < 10 THEN '-07'
      |               ELSE '-10'
      |               END ), '-01 00:00:00') AS TIMESTAMP)
    """.stripMargin)
    logPlan("showByQuarter", df)
  }

  test("fromUnixTimestamp") {
    val df = sqlAndLog("fromUnixTimestamp",
    """
      |SELECT Cast(From_unixtime(Unix_timestamp(Cast(
      |                          Concat(To_date(lineitem.l_shipdate), ' 00:00:00')
      |                          AS
      |                                        TIMESTAMP)), 'yyyy-MM-01 00:00:00')
      |                   AS TIMESTAMP) AS tmn_l_shipdate_ok
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
      |GROUP  BY Cast(From_unixtime(Unix_timestamp(Cast(
      |                         Concat(To_date(lineitem.l_shipdate), ' 00:00:00')
      |                         AS
      |                                       TIMESTAMP)), 'yyyy-MM-01 00:00:00') AS
      |               TIMESTAMP)
    """.stripMargin)
    logPlan("fromUnixTimestamp", df)
    logDruidQuery("fromUnixTimestamp", df)
  }

  test("toDate") {
    val df = sqlAndLog("toDate",
      """
        |SELECT Sum(lineitem.l_extendedprice)                                         AS
        |       sum_l_extendedprice_ok,
        |       Cast(Concat(To_date(Cast(
        |                   Concat(To_date(lineitem.l_shipdate), ' 00:00:00') AS
        |                        TIMESTAMP)), ' 00:00:00') AS TIMESTAMP) AS
        |       tdy_l_shipdate_ok
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
        |GROUP  BY Cast(Concat(To_date(Cast(
        |                      Concat(To_date(lineitem.l_shipdate), ' 00:00:00') AS
        |                                     TIMESTAMP)), ' 00:00:00') AS TIMESTAMP )
      """.stripMargin)
    logPlan("toDate", df)
    logDruidQuery("toDate", df)
  }

  test("dateFilter") {
    val df = sqlAndLog("dateFilter",
      """
        |SELECT Sum(lineitem.l_extendedprice)                                         AS
        |       sum_l_extendedprice_ok,
        |       Cast(Concat(To_date(Cast(
        |                   Concat(To_date(lineitem.l_shipdate), ' 00:00:00') AS
        |                        TIMESTAMP)), ' 00:00:00') AS TIMESTAMP) AS
        |       tdy_l_shipdate_ok
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
        |WHERE  ( ( Cast(Concat(To_date(orders.o_orderdate), ' 00:00:00') AS TIMESTAMP)
        |           >= Cast(
        |                    '1993-05-19 00:00:00' AS TIMESTAMP) )
        |         AND ( Cast(Concat(To_date(orders.o_orderdate), ' 00:00:00') AS
        |                    TIMESTAMP) <=
        |               Cast(
        |                   '1998-08-02 00:00:00' AS TIMESTAMP) ) )
        |GROUP  BY Cast(Concat(To_date(Cast(
        |                      Concat(To_date(lineitem.l_shipdate), ' 00:00:00') AS
        |                                     TIMESTAMP)), ' 00:00:00') AS TIMESTAMP)
      """.stripMargin)
    logPlan("dateFilter", df)
  }

  test("intervalFilter") {
    //turnOnTransformDebugging

    val df = sqlAndLog("dateFilter",
      """
        |SELECT Sum(lineitem.l_extendedprice)                                         AS
        |       sum_l_extendedprice_ok,
        |       Cast(Concat(To_date(Cast(
        |                   Concat(To_date(lineitem.l_shipdate), ' 00:00:00') AS
        |                        TIMESTAMP)), ' 00:00:00') AS TIMESTAMP) AS
        |       tdy_l_shipdate_ok
        |FROM   (SELECT *
        |        FROM   lineitem) lineitem
        |
        |WHERE  ( ( Cast(Concat(To_date(lineitem.l_shipdate), ' 00:00:00') AS TIMESTAMP)
        |           >= Cast(
        |                    '1993-05-19 00:00:00' AS TIMESTAMP) )
        |         AND ( Cast(Concat(To_date(lineitem.l_shipdate), ' 00:00:00') AS
        |                    TIMESTAMP) <=
        |               Cast(
        |                   '1998-08-02 00:00:00' AS TIMESTAMP) ) )
        |GROUP  BY Cast(Concat(To_date(Cast(
        |                      Concat(To_date(lineitem.l_shipdate), ' 00:00:00') AS
        |                                     TIMESTAMP)), ' 00:00:00') AS TIMESTAMP)
      """.stripMargin)
    logPlan("intervalFilter", df)
    logDruidQuery("intervalFIlter", df)
  }

}
