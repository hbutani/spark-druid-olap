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

class JoinTest extends StarSchemaBaseTest {

  test("basicJoin") {
    val df = sqlAndLog("li-supp-join",
      "select s_name, l_linestatus " +
        "from lineitembase li join supplier s on  li.l_suppkey = s.s_suppkey ")
    logPlan("basicAggOrderByDimension", df)

    val lp = df.queryExecution.optimizedPlan

    val pp = df.queryExecution.sparkPlan

    //df.show()

    df.explain(true)
  }

  test("basicJoinAgg") {
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

}
