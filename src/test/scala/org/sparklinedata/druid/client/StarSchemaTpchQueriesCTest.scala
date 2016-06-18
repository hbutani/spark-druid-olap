package org.sparklinedata.druid.client

import org.sparklinedata.spark.dateTime.dsl.expressions._
import scala.language.postfixOps
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

class StarSchemaTpchQueriesCTest extends StarSchemaBaseTest with BeforeAndAfterAll with Logging {

  val q1Predicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 3.day)

  cTest("sstqcT1",
    date"""select l_returnflag, l_linestatus,count(*), sum(l_extendedprice) as s,
       max(ps_supplycost) as m,
       avg(ps_availqty) as a,count(distinct o_orderkey)
       from lineitem, partsupp, orders
       where $q1Predicate
       and l_shipdate  >= '1994-01-01'
       and l_shipdate <= '1997-01-01'
       and l_orderkey = o_orderkey and l_suppkey = ps_suppkey and l_partkey = ps_partkey
       group by l_returnflag, l_linestatus""".stripMargin,
    date"""select l_returnflag, l_linestatus,
       count(*), sum(l_extendedprice) as s,
       max(ps_supplycost) as m,
       avg(ps_availqty) as a,count(distinct o_orderkey)
       from lineitembase, partsupp, orders
       where $q1Predicate
       and l_shipdate  >= '1994-01-01'
       and l_shipdate <= '1997-01-01'
       and l_orderkey = o_orderkey and l_suppkey = ps_suppkey and l_partkey = ps_partkey
       group by l_returnflag, l_linestatus""".stripMargin
  )


 val q3OrderDtPredicate = dateTime('o_orderdate) < dateTime("1995-03-15")
  val q3ShipDtPredicate = dateTime('l_shipdate) > dateTime("1995-03-15")
  cTest("sstqcT2",
    date"""
      select
      o_orderkey,
      sum(l_extendedprice) as price, o_orderdate,
      o_shippriority
      from customer,
       |orders,
       |lineitem
      where c_mktsegment = 'BUILDING' and $q3OrderDtPredicate and $q3ShipDtPredicate
       |and c_custkey = o_custkey
       |and l_orderkey = o_orderkey
       |and l_shipdate  >= '1994-01-01'
       |and l_shipdate <= '1997-01-01'
      group by o_orderkey,
      o_orderdate,
      o_shippriority
      """.stripMargin,
    date"""
      select
      o_orderkey,
      sum(l_extendedprice) as price, o_orderdate,
      o_shippriority
      from customer,
       |orders,
       |lineitembase
      where c_mktsegment = 'BUILDING' and $q3OrderDtPredicate and $q3ShipDtPredicate
       |and c_custkey = o_custkey
       |and l_orderkey = o_orderkey
       |and l_shipdate  >= '1994-01-01'
       |and l_shipdate <= '1997-01-01'
      group by o_orderkey,
      o_orderdate,
      o_shippriority
      """.stripMargin
  )

  val q5orderDtPredicateLower = dateTime('o_orderdate) >= dateTime("1994-01-01")
  val q5OrderDtPredicateUpper= dateTime('o_orderdate) < (dateTime("1994-01-01") + 1.year)

  /**
    * Changes from original query:
    * - join in ''partsupp''. Because we don't support multiple join paths to a table(supplier
    * in this case), the StarSchema doesn't know about the join between ''lineitem'' and
    * ''supplier''
    * - use suppnation and suppregion instead of nation and region.
    */


  cTest("sstqcT3",
    date"""
      select sn_name,
      sum(l_extendedprice) as extendedPrice
      from customer, orders, lineitem, partsupp, supplier, suppnation, suppregion
      where c_custkey = o_custkey
       |and l_orderkey = o_orderkey
       |and l_suppkey = ps_suppkey
       |and l_partkey = ps_partkey
       |and l_shipdate  >= '1994-01-01'
       |and l_shipdate <= '1997-01-01'
       |and ps_suppkey = s_suppkey
       |and s_nationkey = sn_nationkey
       |and sn_regionkey = sr_regionkey
       |and sr_name = 'ASIA'
      and $q5orderDtPredicateLower
      and $q5OrderDtPredicateUpper
      group by sn_name
      """.stripMargin
    ,
    date"""
      select sn_name,
      sum(l_extendedprice) as extendedPrice
      from customer, orders, lineitembase, partsupp, supplier, suppnation, suppregion
      where c_custkey = o_custkey
       |and l_orderkey = o_orderkey
       |and l_suppkey = ps_suppkey
       |and l_partkey = ps_partkey
       |and l_shipdate  >= '1994-01-01'
       |and l_shipdate <= '1997-01-01'
       |and ps_suppkey = s_suppkey
       |and s_nationkey = sn_nationkey
       |and sn_regionkey = sr_regionkey
       |and sr_name = 'ASIA'
      and $q5orderDtPredicateLower
      and $q5OrderDtPredicateUpper
      group by sn_name
      """.stripMargin
  )


  val q7ShipDtYear = dateTime('l_shipdate) year

  /**
    * Changes from original query:
    * - join in ''partsupp''. Because we don't support multiple join paths to a table(supplier
    * in this case), the StarSchema doesn't know about the join between ''lineitem'' and
    * ''supplier''
    * - use suppnation and custnation instead of 'nation n1' and 'nation n2'
    */

  cTest("sstqcT4",
    date"""
    select sn_name, cn_name, $q7ShipDtYear as l_year,
    sum(l_extendedprice) as extendedPrice
    from partsupp, supplier,
       |lineitem, orders, customer, suppnation n1, custnation n2
    where ps_suppkey = s_suppkey
       |and l_suppkey = ps_suppkey
       |and l_partkey = ps_partkey
       |and l_shipdate  >= '1994-01-01'
       |and l_shipdate <= '1997-01-01'
       |and o_orderkey = l_orderkey
       |and c_custkey = o_custkey
       |and s_nationkey = n1.sn_nationkey and c_nationkey = n2.cn_nationkey and
    ((sn_name = 'FRANCE' and cn_name = 'GERMANY') or
           (cn_name = 'FRANCE' and sn_name = 'GERMANY')
           )
    group by sn_name, cn_name, $q7ShipDtYear
    """.stripMargin
    ,
    date"""
    select sn_name, cn_name, $q7ShipDtYear as l_year,
    sum(l_extendedprice) as extendedPrice
    from partsupp, supplier,
       |lineitembase, orders, customer, suppnation n1, custnation n2
    where ps_suppkey = s_suppkey
       |and l_suppkey = ps_suppkey
       |and l_partkey = ps_partkey
       |and l_shipdate  >= '1994-01-01'
       |and l_shipdate <= '1997-01-01'
       |and o_orderkey = l_orderkey
       |and c_custkey = o_custkey
       |and s_nationkey = n1.sn_nationkey and c_nationkey = n2.cn_nationkey and
    ((sn_name = 'FRANCE' and cn_name = 'GERMANY') or
           (cn_name = 'FRANCE' and sn_name = 'GERMANY')
           )
    group by sn_name, cn_name, $q7ShipDtYear
    """.stripMargin
  )


  val q8OrderDtYear = dateTime('o_orderdate) year

  val q8DtP1 = dateTime('o_orderdate) >= dateTime("1995-01-01")
  val q8DtP2 = dateTime('o_orderdate) <= dateTime("1996-12-31")

  /**
    * Changes from original query:
    * - join in ''partsupp''. Because we don't support multiple join paths to a table(supplier
    * in this case), the StarSchema doesn't know about the join between ''lineitem'' and
    * ''supplier''
    * - use suppnation and custnation instead of 'nation n1' and 'nation n2'
    */
  cTest("sstqcT5",
    date"""
      select $q8OrderDtYear as o_year,
      sum(l_extendedprice) as price
      from partsupp, part,
      supplier, lineitem, orders, customer, custnation n1, suppnation n2, custregion
      where ps_partkey = l_partkey
       |and ps_suppkey = l_suppkey
       |and ps_partkey = p_partkey
       |and ps_suppkey = s_suppkey
       |and l_orderkey = o_orderkey
       |and l_shipdate  >= '1994-01-01'
       |and l_shipdate <= '1997-01-01'
       |and o_custkey = c_custkey
       |and c_nationkey = n1.cn_nationkey and
       |n1.cn_regionkey = cr_regionkey and
       |s_nationkey = n2.sn_nationkey and
      cr_name = 'AMERICA' and p_type = 'ECONOMY ANODIZED STEEL' and $q8DtP1 and $q8DtP2
      group by $q8OrderDtYear
      """.stripMargin
    ,
    date"""
      select $q8OrderDtYear as o_year,
      sum(l_extendedprice) as price
      from partsupp, part,
      supplier, lineitembase, orders, customer, custnation n1, suppnation n2, custregion
      where ps_partkey = l_partkey
       |and ps_suppkey = l_suppkey
       |and ps_partkey = p_partkey
       |and ps_suppkey = s_suppkey
       |and l_orderkey = o_orderkey
       |and l_shipdate  >= '1994-01-01'
       |and l_shipdate <= '1997-01-01'
       |and o_custkey = c_custkey
       |and c_nationkey = n1.cn_nationkey and
       |n1.cn_regionkey = cr_regionkey and
       |s_nationkey = n2.sn_nationkey and
      cr_name = 'AMERICA' and p_type = 'ECONOMY ANODIZED STEEL' and $q8DtP1 and $q8DtP2
      group by $q8OrderDtYear
      """.stripMargin
  )

  val q10DtP1 = dateTime('o_orderdate) >= dateTime("1993-10-01")
  val q10DtP2 = dateTime('o_orderdate) < (dateTime("1993-10-01") + 3.month)

  /**
    * Changes from original query:
    * - use custnation instead of 'nation'
    */
  cTest("sstqcT6",
    date"""
    select c_name, cn_name, c_address, c_phone, c_comment,
           sum(l_extendedprice) as price
    from customer,
    orders, lineitem, custnation
    where c_custkey = o_custkey
       |and l_orderkey = o_orderkey
       |and l_shipdate  >= '1994-01-01'
       |and l_shipdate <= '1997-01-01'
       |and c_nationkey = cn_nationkey and
      $q10DtP1 and
      $q10DtP2 and
      l_returnflag = 'R'
    group by c_name, cn_name, c_address, c_phone, c_comment
    """.stripMargin
    ,
    date"""
    select c_name, cn_name, c_address, c_phone, c_comment,
           sum(l_extendedprice) as price
    from customer,
    orders, lineitembase, custnation
    where c_custkey = o_custkey
       |and l_orderkey = o_orderkey
       |and l_shipdate  >= '1994-01-01'
       |and l_shipdate <= '1997-01-01'
       |and c_nationkey = cn_nationkey and
      $q10DtP1 and
      $q10DtP2 and
      l_returnflag = 'R'
    group by c_name, cn_name, c_address, c_phone, c_comment
    """.stripMargin)

}
