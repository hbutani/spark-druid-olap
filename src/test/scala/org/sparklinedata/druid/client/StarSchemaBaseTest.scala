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
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.hive.test.sparklinedata.TestHive
import org.apache.spark.sql.hive.test.sparklinedata.TestHive._
import org.apache.spark.sql.sources.druid.DruidPlanner
import org.scalatest.BeforeAndAfterAll
import org.sparklinedata.spark.dateTime.dsl.expressions._

import scala.language.postfixOps

object StarSchemaTpchQueries {

  val q1Predicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 3.day)

  val q1 = date"""select l_returnflag, l_linestatus,
                            count(*), sum(l_extendedprice) as s,
              max(ps_supplycost) as m,
       avg(ps_availqty) as a,count(distinct o_orderkey)
       from lineitem
       where $q1Predicate
       group by l_returnflag, l_linestatus""".stripMargin

  val q3OrderDtPredicate = dateTime('o_orderdate) < dateTime("1995-03-15")
  val q3ShipDtPredicate = dateTime('l_shipdate) > dateTime("1995-03-15")

  val q3 = date"""
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
      group by o_orderkey,
      o_orderdate,
      o_shippriority
      """.stripMargin

  val q5orderDtPredicateLower = dateTime('o_orderdate) >= dateTime("1994-01-01")
  val q5OrderDtPredicateUpper= dateTime('o_orderdate) < (dateTime("1994-01-01") + 1.year)

  /**
    * Changes from original query:
    * - join in ''partsupp''. Because we don't support multiple join paths to a table(supplier
    * in this case), the StarSchema doesn't know about the join between ''lineitem'' and
    * ''supplier''
    * - use suppnation and suppregion instead of nation and region.
    */
  val q5 =  date"""
      select sn_name,
      sum(l_extendedprice) as extendedPrice
      from customer, orders, lineitem, partsupp, supplier, suppnation, suppregion
      where c_custkey = o_custkey
                    |and l_orderkey = o_orderkey
                    |and l_suppkey = ps_suppkey
                    |and l_partkey = ps_partkey
                    |and ps_suppkey = s_suppkey
                    |and s_nationkey = sn_nationkey
                    |and sn_regionkey = sr_regionkey
                    |and sr_name = 'ASIA'
      and $q5orderDtPredicateLower
      and $q5OrderDtPredicateUpper
      group by sn_name
      """.stripMargin

  val q7ShipDtYear = dateTime('l_shipdate) year

  /**
    * Changes from original query:
    * - join in ''partsupp''. Because we don't support multiple join paths to a table(supplier
    * in this case), the StarSchema doesn't know about the join between ''lineitem'' and
    * ''supplier''
    * - use suppnation and custnation instead of 'nation n1' and 'nation n2'
    */
  val q7 = date"""
    select sn_name, cn_name, $q7ShipDtYear as l_year,
    sum(l_extendedprice) as extendedPrice
    from partsupp, supplier,
              |lineitem, orders, customer, suppnation n1, custnation n2
    where ps_suppkey = s_suppkey
              |and l_suppkey = ps_suppkey
              |and l_partkey = ps_partkey
              |and o_orderkey = l_orderkey
              |and c_custkey = o_custkey
              |and s_nationkey = n1.sn_nationkey and c_nationkey = n2.cn_nationkey and
    ((sn_name = 'FRANCE' and cn_name = 'GERMANY') or
           (cn_name = 'FRANCE' and sn_name = 'GERMANY')
           )
    group by sn_name, cn_name, $q7ShipDtYear
    """.stripMargin

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
  val q8 = date"""
      select $q8OrderDtYear as o_year,
      sum(l_extendedprice) as price
      from partsupp, part,
      supplier, lineitem, orders, customer, custnation n1, suppnation n2, custregion
      where ps_partkey = l_partkey
                   |and ps_suppkey = l_suppkey
                   |and ps_partkey = p_partkey
                   |and ps_suppkey = s_suppkey
                   |and l_orderkey = o_orderkey
                   |and o_custkey = c_custkey
                   |and c_nationkey = n1.cn_nationkey and
                   |n1.cn_regionkey = cr_regionkey and
                   |s_nationkey = n2.sn_nationkey and
      cr_name = 'AMERICA' and p_type = 'ECONOMY ANODIZED STEEL' and $q8DtP1 and $q8DtP2
      group by $q8OrderDtYear
      """.stripMargin

  val q10DtP1 = dateTime('o_orderdate) >= dateTime("1993-10-01")
  val q10DtP2 = dateTime('o_orderdate) < (dateTime("1993-10-01") + 3.month)

  /**
    * Changes from original query:
    * - use custnation instead of 'nation'
    */
  val q10 = date"""
    select c_name, cn_name, c_address, c_phone, c_comment,
           sum(l_extendedprice) as price
    from customer,
    orders, lineitem, custnation
    where c_custkey = o_custkey
                    |and l_orderkey = o_orderkey
                    |and c_nationkey = cn_nationkey and
      $q10DtP1 and
      $q10DtP2 and
      l_returnflag = 'R'
    group by c_name, cn_name, c_address, c_phone, c_comment
    """.stripMargin
}

class StarSchemaBaseTest extends BaseTest with BeforeAndAfterAll with Logging {

  val TPCH_BASE_DIR = sys.env("HOME") + "/tpch/datascale1"

  def tpchDataFolder(tableName : String) = s"$TPCH_BASE_DIR/$tableName/"

  override def beforeAll() = {
    super.beforeAll()

    sql(s"""CREATE TABLE if not exists lineitembase(l_orderkey integer,
        l_partkey integer, l_suppkey integer,
      l_linenumber integer,
      l_quantity double, l_extendedprice double, l_discount double, l_tax double,
      l_returnflag string,
      l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string,
      l_shipinstruct string,
      l_shipmode string, l_comment string)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("lineitem")}",
      header "false", delimiter "|")""".stripMargin)

    TestHive.table("lineitembase").cache

    sql(s"""CREATE TABLE if not exists orders(
           |o_orderkey integer, o_custkey integer,
           |    o_orderstatus VARCHAR(1),
           |    o_totalprice double,
           |    o_orderdate string,
           |    o_orderpriority VARCHAR(15),
           |    o_clerk VARCHAR(15),
           |    o_shippriority integer,
           |    o_comment VARCHAR(79)
    )
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("orders")}",
      header "false", delimiter "|")""".stripMargin)

    TestHive.table("orders").cache

    sql(s"""CREATE TABLE if not exists partsupp(
           | ps_partkey integer, ps_suppkey integer,
           |    ps_availqty integer, ps_supplycost double,
           |    ps_comment VARCHAR(199)
    )
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("partsupp")}",
      header "false", delimiter "|")""".stripMargin)

    TestHive.table("partsupp").cache

    sql(s"""CREATE TABLE if not exists supplier(
             s_suppkey integer, s_name string, s_address string,
             s_nationkey integer,
           |      s_phone string, s_acctbal double, s_comment string)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("supplier")}",
      header "false", delimiter "|")""".stripMargin)

    TestHive.table("supplier").cache

    sql(s"""CREATE TABLE if not exists part(p_partkey integer, p_name string,
           |      p_mfgr string, p_brand string, p_type string, p_size integer, p_container string,
           |      p_retailprice double,
           |      p_comment string)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("part")}",
      header "false", delimiter "|")""".stripMargin)

    TestHive.table("part").cache

    sql(s"""CREATE TABLE if not exists customer(
           | c_custkey INTEGER,
           |    c_name VARCHAR(25),
           |    c_address VARCHAR(40),
           |    c_nationkey INTEGER,
           |    c_phone VARCHAR(15),
           |    c_acctbal double,
           |    c_mktsegment VARCHAR(10),
           |    c_comment VARCHAR(117)
           |)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("customer")}",
      header "false", delimiter "|")""".stripMargin)

    TestHive.table("customer").cache

    sql(s"""CREATE TABLE if not exists custnation(
           | cn_nationkey integer, cn_name VARCHAR(25),
           |    cn_regionkey integer, cn_comment VARCHAR(152)
           |)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("nation")}",
      header "false", delimiter "|")""".stripMargin)

    TestHive.table("custnation").cache

    sql(s"""CREATE TABLE if not exists custregion(
           | cr_regionkey integer, cr_name VARCHAR(25),
           |    cr_comment VARCHAR(152)
           |)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("region")}",
      header "false", delimiter "|")""".stripMargin)

    TestHive.table("custregion").cache

    sql(s"""CREATE TABLE if not exists suppnation(
           | sn_nationkey integer, sn_name VARCHAR(25),
           |    sn_regionkey integer, sn_comment VARCHAR(152)
           |)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("nation")}",
      header "false", delimiter "|")""".stripMargin)

    TestHive.table("suppnation").cache

    sql(s"""CREATE TABLE if not exists suppregion(
           | sr_regionkey integer, sr_name VARCHAR(25),
           |    sr_comment VARCHAR(152)
           |)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("region")}",
      header "false", delimiter "|")""".stripMargin)

    TestHive.table("suppregion").cache

    TestHive.setConf(DruidPlanner.SPARKLINEDATA_CACHE_TABLES_TOCHECK.key,
      "orderLineItemPartSupplierBase,suppregion,suppnation," +
        "custregion,custnation,customer,part,supplier,partsupp,orders,lineitembase")

    /*
     * for -ve testing only
     */
    sql(s"""CREATE TABLE if not exists partsupp2(
           | ps_partkey integer, ps_suppkey integer,
           |    ps_availqty integer, ps_supplycost double,
           |    ps_comment VARCHAR(199)
    )
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("partsupp")}",
      header "false", delimiter "|")""".stripMargin)


    sql(
      s"""CREATE TABLE if not exists lineitem
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "lineItemBase",
      timeDimensionColumn "l_shipdate",
      druidDatasource "tpch",
      druidHost "localhost",
      zkQualifyDiscoveryNames "true",
      columnMapping '$colMapping',
      functionalDependencies '$functionalDependencies',
      starSchema '$starSchema')""".stripMargin
    )


  }

}
