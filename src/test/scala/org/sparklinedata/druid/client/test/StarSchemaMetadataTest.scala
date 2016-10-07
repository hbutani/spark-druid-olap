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

import org.apache.spark.sql.hive.test.sparklinedata.TestHive
import org.sparklinedata.druid.Utils
import org.sparklinedata.druid.metadata.{StarRelationInfo, StarSchema, StarSchemaInfo}


class StarSchemaMetadataTest  extends StarSchemaBaseTest {

  test("simpleStar") { td =>

    val starInfo = StarSchemaInfo("lineitem",
      StarRelationInfo.manyToone("lineitem", "orders", ("l_orderkey", "o_orderkey"))
    )

    val ss = StarSchema("lineitembase", starInfo)(TestHive)

    println(ss.right.get.prettyString)

  }

  test("salesStar") { td =>

    val starInfo = StarSchemaInfo("lineitem",
      StarRelationInfo.manyToone("lineitem", "orders", ("l_orderkey", "o_orderkey")),
      StarRelationInfo.manyToone("lineitem", "part", ("l_partkey", "p_partkey")),
      StarRelationInfo.manyToone("lineitem", "supplier", ("l_suppkey", "s_suppkey"))
    )

    val ss = StarSchema("lineitembase", starInfo)(TestHive)

    println(ss.right.get.prettyString)

  }

  test("tpch") { td =>

    val starInfo = StarSchemaInfo("lineitem",
      StarRelationInfo.manyToone("lineitem", "orders", ("l_orderkey", "o_orderkey")),
      StarRelationInfo.manyToone("lineitem", "partsupp",
        ("l_partkey", "ps_partkey"), ("l_suppkey", "ps_suppkey")),
      StarRelationInfo.manyToone("partsupp", "part", ("ps_partkey", "p_partkey")),
      StarRelationInfo.manyToone("partsupp", "supplier", ("ps_suppkey", "s_suppkey")),
      StarRelationInfo.manyToone("orders", "customer", ("o_custkey", "c_custkey")),
      StarRelationInfo.manyToone("customer", "custnation", ("c_nationkey", "cn_nationkey")),
      StarRelationInfo.manyToone("custnation", "custregion", ("cn_regionkey", "cr_regionkey")),
      StarRelationInfo.manyToone("supplier", "suppnation", ("s_nationkey", "sn_nationkey")),
      StarRelationInfo.manyToone("suppnation", "suppregion", ("sn_regionkey", "sr_regionkey"))
    )

    val ss = StarSchema("lineitembase", starInfo)(TestHive)

    println(ss.right.get.prettyString)

    Utils.logStarSchema(starInfo)

  }

  test("multiPathsNotAllowed") { td =>

    val starInfo = StarSchemaInfo("lineitem",
      StarRelationInfo.manyToone("lineitem", "orders", ("l_orderkey", "o_orderkey")),
      StarRelationInfo.manyToone("lineitem", "partsupp",
        ("l_partkey", "ps_partkey"), ("l_suppkey", "ps_suppkey")),
      StarRelationInfo.manyToone("partsupp", "part", ("ps_partkey", "p_partkey")),
      StarRelationInfo.manyToone("partsupp", "supplier", ("ps_suppkey", "s_suppkey")),
      StarRelationInfo.manyToone("lineitem", "supplier", ("li_suppkey", "s_suppkey"))
    )

    val ss = StarSchema("lineitembase", starInfo)(TestHive)

    assert(ss.left.get == "multiple join paths to table 'supplier'")

  }

  test("nonUniqueColumnsNotAllowed") { td =>

    val starInfo = StarSchemaInfo("lineitem",
      StarRelationInfo.manyToone("lineitem", "orders", ("l_orderkey", "o_orderkey")),
      StarRelationInfo.manyToone("lineitem", "partsupp",
        ("l_partkey", "ps_partkey"), ("l_suppkey", "ps_suppkey")),
      StarRelationInfo.manyToone("partsupp", "part", ("ps_partkey", "p_partkey")),
      StarRelationInfo.manyToone("partsupp", "supplier", ("ps_suppkey", "s_suppkey")),
      StarRelationInfo.manyToone("lineitem", "partsupp2",
        ("l_partkey", "ps_partkey"), ("l_suppkey", "ps_suppkey"))
    )

    val ss = StarSchema("lineitembase", starInfo)(TestHive)

    assert(ss.left.get ==
      "Column ps_partkey is not unique across Star Schema; in tables partsupp2, partsupp")

  }

}
