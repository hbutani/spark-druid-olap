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
import org.apache.spark.sql.hive.test.TestHive._
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

  val q5Orig =  date"""
      select s_nation,
      sum(l_extendedprice) as extendedPrice
      from customer, orders, lineitem, supplier, suppnation, suppregion
      where c_custkey = o_custkey
                    |and l_orderkey = o_orderkey
                    |and l_suppkey = s_suppkey
                    |and c_nationkey = s_nationkey
                    |and s_nationkey = sn_nationkey
                    |and sn_regionkey = sr_regionkey
                    |and s_region = 'ASIA'
      and $q5orderDtPredicateLower
      and $q5OrderDtPredicateUpper
      group by s_nation
      """.stripMargin

  val q5Altered =  date"""
      select s_nation,
      sum(l_extendedprice) as extendedPrice
      from customer, orders, lineitem, supplier, suppnation, suppregion
      where c_custkey = o_custkey
                   |and l_orderkey = o_orderkey
                   |and l_suppkey = s_suppkey
                   |and s_nationkey = sn_nationkey
                   |and sn_regionkey = sr_regionkey
                   |and s_region = 'ASIA'
      and $q5orderDtPredicateLower
      and $q5OrderDtPredicateUpper
      group by s_nation
      """.stripMargin

  val q7ShipDtYear = dateTime('l_shipdate) year

  val q7 = date"""
    select s_nation, c_nation, $q7ShipDtYear as l_year,
    sum(l_extendedprice) as extendedPrice
    from supplier,
                   |lineitem, orders, customer, suppnation n1, custnation n2
    where s_suppkey = l_suppkey
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

  val q8 = date"""
      select $q8OrderDtYear as o_year,
      sum(l_extendedprice) as price
      from part,
      supplier, lineitem, orders, customer, custnation n1, suppnation n2, custregion
      where p_partkey = l_partkey
                   |and s_suppkey = l_suppkey
                   |and l_orderkey = o_orderkey
                   |and o_custkey = c_custkey
                   |and c_nationkey = n1.cn_nationkey and
                   |n1.cn_regionkey = cr_regionkey and
                   |s_nationkey = n2.sn_nationkey
      c_region = 'AMERICA' and p_type = 'ECONOMY ANODIZED STEEL' and $q8DtP1 and $q8DtP2
      group by $q8OrderDtYear
      """.stripMargin

  val q10DtP1 = dateTime('o_orderdate) >= dateTime("1993-10-01")
  val q10DtP2 = dateTime('o_orderdate) < (dateTime("1993-10-01") + 3.month)

  val q10 = date"""
    select c_name, c_nation, c_address, c_phone, c_comment,
           sum(l_extendedprice) as price
    from customer,
    orders, lineitem, custnation
    where c_custkey = o_custkey
                    |and l_orderkey = o_orderkey
                    |and c_nationkey = cn_nationkey
      $q10DtP1 and
      $q10DtP2 and
      l_returnflag = 'R'
    group by c_name, c_nation, c_address, c_phone, c_comment
    """.stripMargin
}

class StarSchemaBaseTest extends BaseTest with BeforeAndAfterAll with Logging {

  val TPCH_BASE_DIR = sys.env("HOME") + "tpch/datascale1"

  def tpchDataFolder(tableName : String) = s"$TPCH_BASE_DIR/$tableName/"

  override def beforeAll() = {
    super.beforeAll()

    sql(s"""CREATE TEMPORARY TABLE lineitembase(l_orderkey integer,
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

    sql(s"""CREATE TEMPORARY TABLE orders(
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

    sql(s"""CREATE TEMPORARY TABLE partsupp(
           | ps_partkey integer, ps_suppkey integer,
           |    ps_availqty integer, ps_supplycost double,
           |    ps_comment VARCHAR(199)
    )
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("partsupp")}",
      header "false", delimiter "|")""".stripMargin)

    sql(s"""CREATE TEMPORARY TABLE supplier(
             s_suppkey integer, s_name string, s_address string,
             s_nationkey integer,
           |      s_phone string, s_acctbal double, s_comment string)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("supplier")}",
      header "false", delimiter "|")""".stripMargin)

    sql(s"""CREATE TEMPORARY TABLE part(p_partkey integer, p_name string,
           |      p_mfgr string, p_brand string, p_type string, p_size integer, p_container string,
           |      p_retailprice double,
           |      p_comment string)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("part")}",
      header "false", delimiter "|")""".stripMargin)

    sql(s"""CREATE TEMPORARY TABLE customer(
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

    sql(s"""CREATE TEMPORARY TABLE custnation(
           | cn_nationkey integer, cn_name VARCHAR(25),
           |    cn_regionkey integer, cn_comment VARCHAR(152)
           |)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("nation")}",
      header "false", delimiter "|")""".stripMargin)

    sql(s"""CREATE TEMPORARY TABLE custregion(
           | cr_regionkey integer, cr_name VARCHAR(25),
           |    cr_comment VARCHAR(152)
           |)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("region")}",
      header "false", delimiter "|")""".stripMargin)

    sql(s"""CREATE TEMPORARY TABLE suppnation(
           | sn_nationkey integer, sn_name VARCHAR(25),
           |    sn_regionkey integer, sn_comment VARCHAR(152)
           |)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("nation")}",
      header "false", delimiter "|")""".stripMargin)

    sql(s"""CREATE TEMPORARY TABLE suppregion(
           | sr_regionkey integer, sr_name VARCHAR(25),
           |    sr_comment VARCHAR(152)
           |)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("region")}",
      header "false", delimiter "|")""".stripMargin)

    /*
     * for -ve testing only
     */
    sql(s"""CREATE TEMPORARY TABLE partsupp2(
           | ps_partkey integer, ps_suppkey integer,
           |    ps_availqty integer, ps_supplycost double,
           |    ps_comment VARCHAR(199)
    )
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("partsupp")}",
      header "false", delimiter "|")""".stripMargin)


    sql(
      s"""CREATE TEMPORARY TABLE lineitem
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "lineItemBase",
      timeDimensionColumn "l_shipdate",
      druidDatasource "tpch",
      druidHost "localhost",
      druidPort "8082",
      columnMapping '$colMapping',
      functionalDependencies '$functionalDependencies',
      starSchema '$starSchema')""".stripMargin
    )


  }

}
