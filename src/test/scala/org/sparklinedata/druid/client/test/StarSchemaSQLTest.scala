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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.hive.test.sparklinedata.TestHive._

class StarSchemaSQLTest extends StarSchemaBaseTest {

  test("createStarSchema") { td =>

    sql(
      """
        |create star schema on lineitembase
        |as many_to_one join of lineitembase with orders on l_orderkey = o_orderkey
        |   many_to_one join of lineitembase with partsupp on
        |          l_partkey = ps_partkey and l_suppkey = ps_suppkey
        |   many_to_one join of partsupp with part on ps_partkey = p_partkey
        |   many_to_one join of partsupp with supplier on ps_suppkey = s_suppkey
        |   many_to_one join of orders with customer on o_custkey = c_custkey
        |   many_to_one join of customer with custnation on c_nationkey = cn_nationkey
        |   many_to_one join of custnation with custregion on cn_regionkey = cr_regionkey
        |   many_to_one join of supplier with suppnation on s_nationkey = sn_nationkey
        |   many_to_one join of suppnation with suppregion on sn_regionkey = sr_regionkey
      """.stripMargin)

  }

  test("createStarSchemaInvalidTable") { td =>

    val thrown = intercept[NoSuchTableException] {
      sql(
        """
        |create star schema on lineitembase
        |as many_to_one join of lineitembase with orders on l_orderkey = o_orderkey
        |   many_to_one join of lineitembase with partsupp on
        |          l_partkey = ps_partkey and l_suppkey = ps_suppkey
        |   many_to_one join of partsupp with part on ps_partkey = p_partkey
        |   many_to_one join of partsupp with supplier on ps_suppkey = s_suppkey
        |   many_to_one join of orders with customers on o_custkey = c_custkey
        |   many_to_one join of customer with custnation on c_nationkey = cn_nationkey
        |   many_to_one join of custnation with custregion on cn_regionkey = cr_regionkey
        |   many_to_one join of supplier with suppnation on s_nationkey = sn_nationkey
        |   many_to_one join of suppnation with suppregion on sn_regionkey = sr_regionkey
      """.stripMargin)
    }

    assert(thrown.getMessage() ==
    """Table or view 'customers' not found in database 'default';""")

  }

  test("createStarSchemaInvalidColumn") { td =>

    val thrown = intercept[AnalysisException] {
      sql(
        """
          |create star schema on lineitembase
          |as many_to_one join of lineitembase with orders on l_orderkey = o_orderkey
          |   many_to_one join of lineitembase with partsupp on
          |          l_partkey = ps_partkey and l_suppkey = ps_suppke
          |   many_to_one join of partsupp with part on ps_partkey = p_partkey
          |   many_to_one join of partsupp with supplier on ps_suppkey = s_suppkey
          |   many_to_one join of orders with customer on o_custkey = c_custkey
          |   many_to_one join of customer with custnation on c_nationkey = cn_nationkey
          |   many_to_one join of custnation with custregion on cn_regionkey = cr_regionkey
          |   many_to_one join of supplier with suppnation on s_nationkey = sn_nationkey
          |   many_to_one join of suppnation with suppregion on sn_regionkey = sr_regionkey
        """.stripMargin)
    }

    assert(thrown.getMessage() ==
      """Invalid Columns: Field "ps_suppke" does not exist.;""")

  }

  test("alterStarSchema") {td =>

    sql(
      """
        |create star schema on lineitem
        |as many_to_one join of lineitem with orders on l_orderkey = o_orderkey
      """.stripMargin)

    val thrown = intercept[AnalysisException] {
      sql(
        """
        |create star schema on lineitem
        |as many_to_one join of lineitem with orders on l_orderkey = o_orderkey
        |   many_to_one join of orders with customer on o_custkey = c_custkey
      """.
          stripMargin)
      }

    // scalastyle:off line.size.limit
    assert(thrown.getMessage() ==
      """Cannot create starSchema on `default`.`lineitem`, there is already a Star Schema on it, call alter star schema;""")
    // scalastyle:on line.size.limit

    sql(
      """
        |alter star schema on lineitem
        |as many_to_one join of lineitem with orders on l_orderkey = o_orderkey
        |   many_to_one join of orders with customer on o_custkey = c_custkey
      """.
        stripMargin)
  }

}
