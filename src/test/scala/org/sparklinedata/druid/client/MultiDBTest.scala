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
import org.apache.spark.sql.hive.test.sparklinedata.TestHive
import org.apache.spark.sql.hive.test.sparklinedata.TestHive._
import org.apache.spark.sql.sources.druid.DruidPlanner
import org.scalatest.BeforeAndAfterAll
import org.sparklinedata.spark.dateTime.dsl.expressions._


object MultiDBStarSchemaTpchQueries {

  import StarSchemaTpchQueries._

  val q3 = date"""
      select
      o_orderkey,
      sum(l_extendedprice) as price, o_orderdate,
      o_shippriority
      from default.customer,
              |default.orders,
              |lineitem
      where c_mktsegment = 'BUILDING' and $q3OrderDtPredicate and $q3ShipDtPredicate
              |and c_custkey = o_custkey
              |and l_orderkey = o_orderkey
      group by o_orderkey,
      o_orderdate,
      o_shippriority
      """.stripMargin
}

class MultiDBTest extends StarSchemaBaseTest with BeforeAndAfterAll with Logging {

  override def beforeAll() = {
    super.beforeAll()

    /*
     * - orderLineItemPartSupplierBase in DB tpch
     * - orderLineItemPartSupplier Druid DS in DB tpch2
     * - lineItem Druid DS in tpch2, all dimension tables in default DB.
     */

    sql("create database tpch")
    sql("create database tpch2")
    sql("use tpch")

    sql(olFlatCreateTable)

    TestHive.table("orderLineItemPartSupplierBase").cache()

    TestHive.setConf(DruidPlanner.SPARKLINEDATA_CACHE_TABLES_TOCHECK.key,
      "tpch.orderLineItemPartSupplierBase,suppregion,suppnation,custregion," +
        "custnation,customer,part,supplier,partsupp,orders,lineitembase")

    sql("use tpch2")
    val cTOlap = olDruidDS("tpch")
    sql(cTOlap)

    sql(
      s"""CREATE TABLE if not exists lineitem
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "default.lineItemBase",
      timeDimensionColumn "l_shipdate",
      druidDatasource "tpch",
      druidHost "localhost",
      zkQualifyDiscoveryNames "true",
      columnMapping '$colMapping',
      numProcessingThreadsPerHistorical '1',
      functionalDependencies '$functionalDependencies',
      starSchema '${starSchema("tpch2", "default")}')""".stripMargin
    )

    /*
 * View creation on Spark DataSource fails; Spark defers to Hive to create the view
 * Hive thinks the table's schema is (col : String) i.e. 1 String column named 'col'
 */
    //    sql(
    //      """
    //        |create view orderLineItemPartSupplierBase_view as
    //        |select o_orderkey, o_custkey
    //        |from orderLineItemPartSupplierBase
    //      """.stripMargin)

    //    sql(olDruidDS("tpch",
    //      "orderLineItemPartSupplierBase_view",
    //      "orderLineItemPartSupplier_view"
    //    ))


  }

  override def afterAll(): Unit = {
    sql("use default")
    super.afterAll()
  }

  test("basicAgg",
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a," +
      "count(distinct o_orderkey)  " +
      "from orderLineItemPartSupplier group by l_returnflag, l_linestatus",
    2,
    true
  )

  test("tpchQ3",
    MultiDBStarSchemaTpchQueries.q3,
    1,
    true,
    true,
    true
  )

//  test("view",
//    """select count(*)
//      |from orderLineItemPartSupplier_view
//    """.stripMargin,
//    1,
//    true,
//    true
//  )

}
