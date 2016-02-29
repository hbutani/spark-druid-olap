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

import org.apache.spark.sql.hive.test.TestHive

class JoinTest extends StarSchemaBaseTest {

  test("2tableJoin") {td =>
    val df = sqlAndLog("li-partsupp",
      "select  l_linestatus, sum(ps_availqty) " +
        "from lineitem li join partsupp ps on  li.l_suppkey = ps.ps_suppkey " +
        "and li.l_partkey = ps.ps_partkey " +
        "group by l_linestatus")
    logPlan("basicJoin", df)
    df.explain(true)
    //df.show()
  }

  test("2tableJoinFactTableIsRight") {td =>
    val df = sqlAndLog("partsupp-li",
      "select  l_linestatus, sum(ps_availqty) " +
        "from partsupp ps join lineitem li  on  li.l_suppkey = ps.ps_suppkey " +
        "and li.l_partkey = ps.ps_partkey " +
        "group by l_linestatus")
    df.explain(true)
    //df.show()
  }

  test("3tableJoin") {td =>
    val df = sqlAndLog("li-partsupp-supp",
      "select  s_name, sum(ps_availqty) " +
        "from lineitem li join partsupp ps on  li.l_suppkey = ps.ps_suppkey " +
        "and li.l_partkey = ps.ps_partkey " +
        " join supplier s on ps.ps_suppkey = s.s_suppkey " +
        "group by s_name")
    df.explain(true)
    //df.show()
  }

  test("tpchQ3") {td =>
    val df = sqlAndLog("tpchQ3", StarSchemaTpchQueries.q3)
    df.explain(true)
    df.show()
  }

  test("tpchQ5") {td =>
    val df = sqlAndLog("tpchQ5", StarSchemaTpchQueries.q5)
    df.explain(true)
    df.show()
  }

  test("tpchQ7") {td =>
    val df = sqlAndLog("tpchQ7", StarSchemaTpchQueries.q7)
    df.explain(true)
    df.show()
  }

  test("tpchQ8") {td =>
    val df = sqlAndLog("tpchQ8", StarSchemaTpchQueries.q8)
    df.explain(true)
    df.show()
  }

  test("tpchQ10") {td =>
    val df = sqlAndLog("tpchQ10", StarSchemaTpchQueries.q10)
    df.explain(true)
    df.show()
  }

  test("basicJoinAgg") {td =>
    val df = sqlAndLog("li-supp-join",
      "select s_name, l_linestatus, " +
        "count(*), sum(l_extendedprice) as s " +
        "from lineitembase li join supplier s on  li.l_suppkey = s.s_suppkey " +
        "group by s_name, l_linestatus")
    logPlan("basicAggOrderByDimension", df)

    val lp = df.queryExecution.optimizedPlan

    val pp = df.queryExecution.sparkPlan

    //df.show()

    df.explain(true)
  }

  test("dfPlan1") {td =>
    val df = TestHive.table("lineitem").groupBy("l_linestatus").count()
    logPlan("basicAggOrderByDimension", df)
    df.show()
  }

  test("dfPlan2") {td =>
    val df = TestHive.table("lineitem").
      join(TestHive.table("partsupp")).
      join(TestHive.table("supplier")).
      where("l_suppkey = ps_suppkey and l_partkey = ps_partkey and ps_suppkey = s_suppkey").
      groupBy("s_name", "l_linestatus").sum("l_extendedprice")
    logPlan("basicAggOrderByDimension", df)
    df.show()
  }

}
