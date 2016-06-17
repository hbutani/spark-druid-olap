package org.sparklinedata.druid.client

import org.apache.spark.Logging
import org.scalatest.BeforeAndAfterAll


class SparkDateTimeCTest extends StarSchemaBaseTest with BeforeAndAfterAll with Logging{

  cTest("sdtCT1",

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
      |and l_shipdate  >= '1994-01-01'
      |and l_shipdate <= '1997-01-01'
      |and ps_suppkey = s_suppkey
      |AND s_nationkey = sn_nationkey
      |AND sn_regionkey = sr_regionkey
      |AND sr_name = 'ASIA'
      |AND o_orderdate >= date '1994-01-01'
      |AND o_orderdate < date '1994-01-01' + interval '1' year
      |GROUP BY
      |sn_name
      |ORDER BY
      |revenue desc""".stripMargin,

    """SELECT
      |sn_name,
      |sum(l_extendedprice) as revenue
      |FROM
      |customer,
      |orders,
      |lineitembase,
      |partsupp,
      |supplier,
      |suppnation,
      |suppregion
      |WHERE
      |c_custkey = o_custkey
      |AND l_orderkey = o_orderkey
      |and l_suppkey = ps_suppkey
      |and l_partkey = ps_partkey
      |and l_shipdate  >= '1994-01-01'
      |and l_shipdate <= '1997-01-01'
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

  cTest("sdtCT2",
    "select cast(o_orderdate as date) " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  " +
      "and l_shipdate <= '1997-01-01' " +
      "group by cast(o_orderdate as date)",
    "select cast(o_orderdate as date) " +
      "from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  " +
      "and l_shipdate <= '1997-01-01' " +
      "group by cast(o_orderdate as date)"
  )

  cTest("sdtCT3",
    """SELECT Year(Cast(Concat(To_date(lineitem.l_shipdate), ' 00:00:00') AS TIMESTAMP)
      |       ) AS
      |       yr_l_shipdate_ok
      |FROM   (SELECT *
      |        FROM   lineitem where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' ) lineitem
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
      |                    TIMESTAMP)) """.stripMargin,
    """SELECT Year(Cast(Concat(To_date(lineitem.l_shipdate), ' 00:00:00') AS TIMESTAMP)
      |       ) AS
      |       yr_l_shipdate_ok
      |FROM   (SELECT *
      |        FROM   lineitembase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' ) lineitem
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

  cTest("sdtCT4",
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
      |        FROM   lineitem where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' ) lineitem
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
    """.stripMargin,
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
      |        FROM   lineitembase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' ) lineitem
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
    """.stripMargin
  )

  cTest("sdtCT5",
    """
      |SELECT Cast(From_unixtime(Unix_timestamp(Cast(
      |                          Concat(To_date(lineitem.l_shipdate), ' 00:00:00')
      |                          AS
      |                                        TIMESTAMP)), 'yyyy-MM-01 00:00:00')
      |                   AS TIMESTAMP) AS tmn_l_shipdate_ok
      |FROM   (SELECT *
      |        FROM   lineitem where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' ) lineitem
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
    """.stripMargin,
    """
      |SELECT Cast(From_unixtime(Unix_timestamp(Cast(
      |                          Concat(To_date(lineitem.l_shipdate), ' 00:00:00')
      |                          AS
      |                                        TIMESTAMP)), 'yyyy-MM-01 00:00:00')
      |                   AS TIMESTAMP) AS tmn_l_shipdate_ok
      |FROM   (SELECT *
      |        FROM   lineitembase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' ) lineitem
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
    """.stripMargin
  )

  cTest("sdtCT6",
    """
      |SELECT Sum(lineitem.l_extendedprice)                                         AS
      |       sum_l_extendedprice_ok,
      |       Cast(Concat(To_date(Cast(
      |                   Concat(To_date(lineitem.l_shipdate), ' 00:00:00') AS
      |                        TIMESTAMP)), ' 00:00:00') AS TIMESTAMP) AS
      |       tdy_l_shipdate_ok
      |FROM   (SELECT *
      |        FROM   lineitem where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' ) lineitem
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
    """.stripMargin,
    """
      |SELECT Sum(lineitem.l_extendedprice)                                         AS
      |       sum_l_extendedprice_ok,
      |       Cast(Concat(To_date(Cast(
      |                   Concat(To_date(lineitem.l_shipdate), ' 00:00:00') AS
      |                        TIMESTAMP)), ' 00:00:00') AS TIMESTAMP) AS
      |       tdy_l_shipdate_ok
      |FROM   (SELECT *
      |        FROM   lineitembase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' ) lineitem
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
    """.stripMargin
  )

  cTest("sdtCT7",
    """
      |SELECT Sum(lineitem.l_extendedprice)                                         AS
      |       sum_l_extendedprice_ok,
      |       Cast(Concat(To_date(Cast(
      |                   Concat(To_date(lineitem.l_shipdate), ' 00:00:00') AS
      |                        TIMESTAMP)), ' 00:00:00') AS TIMESTAMP) AS
      |       tdy_l_shipdate_ok
      |FROM   (SELECT *
      |        FROM   lineitem where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' ) lineitem
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
    """.stripMargin,
    """
      |SELECT Sum(lineitem.l_extendedprice)                                         AS
      |       sum_l_extendedprice_ok,
      |       Cast(Concat(To_date(Cast(
      |                   Concat(To_date(lineitem.l_shipdate), ' 00:00:00') AS
      |                        TIMESTAMP)), ' 00:00:00') AS TIMESTAMP) AS
      |       tdy_l_shipdate_ok
      |FROM   (SELECT *
      |        FROM   lineitembase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01') lineitem
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
    """.stripMargin
  )
  cTest("sdtCT8",
    """
      |SELECT Sum(lineitem.l_extendedprice)                                         AS
      |       sum_l_extendedprice_ok,
      |       Cast(Concat(To_date(Cast(
      |                   Concat(To_date(lineitem.l_shipdate), ' 00:00:00') AS
      |                        TIMESTAMP)), ' 00:00:00') AS TIMESTAMP) AS
      |       tdy_l_shipdate_ok
      |FROM   (SELECT *
      |        FROM   lineitem where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' ) lineitem
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
    """.stripMargin,
    """
      |SELECT Sum(lineitem.l_extendedprice)                                         AS
      |       sum_l_extendedprice_ok,
      |       Cast(Concat(To_date(Cast(
      |                   Concat(To_date(lineitem.l_shipdate), ' 00:00:00') AS
      |                        TIMESTAMP)), ' 00:00:00') AS TIMESTAMP) AS
      |       tdy_l_shipdate_ok
      |FROM   (SELECT *
      |        FROM   lineitembase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' ) lineitem
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
    """.stripMargin
  )

  cTest("sdtCT9",
    """
      |SELECT Count(DISTINCT( rae.l_linenumber ))
      |FROM   (SELECT *
      |        FROM   orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01') rae
      | where  rae.l_commitdate != '' and
      |       Month(Cast(Concat(To_date(l_shipdate), ' 00:00:00') AS TIMESTAMP)) < 4
    """.stripMargin,
    """
      |SELECT Count(DISTINCT( rae.l_linenumber ))
      |FROM   (SELECT *
      |        FROM   orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01') rae
      | where  rae.l_commitdate != '' and
      |       Month(Cast(Concat(To_date(l_shipdate), ' 00:00:00') AS TIMESTAMP)) < 4
    """.stripMargin
  )

}
