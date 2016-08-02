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

class SelectQueryTest extends BaseTest with BeforeAndAfterAll with Logging {

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
      druidHost "localhost",
      zkQualifyDiscoveryNames "true",
      queryHistoricalServers "true",
      nonAggregateQueryHandling "push_project_and_filters",
      columnMapping '$colMapping',
      functionalDependencies '$functionalDependencies',
      starSchema '$flatStarSchema')""".stripMargin
    sql(cTOlap)
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

  /*
   * Tests:
   *  - basic project
   *  - project expression1 : attr + literals
   *  - project expression 2: multi attr expr
   *  - filter
   *  - filter column not projected
   *  - single join
   *  - multi dimension join
   *  - complex filters
   */

}
