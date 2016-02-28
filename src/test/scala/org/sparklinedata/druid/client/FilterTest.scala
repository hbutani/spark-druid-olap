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
import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.BeforeAndAfterAll

/**
  * Created by jpullokk on 2/25/16.
  */
class FilterTest extends BaseTest with BeforeAndAfterAll with Logging{
  test("inclauseTest1") {
    val df = sqlAndLog("inclauseTest1", "select c_name, sum(c_acctbal) as bal " +
      "from orderLineItemPartSupplier " +
      "where c_mktsegment in ('MACHINERY', 'HOUSEHOLD') " +
      "group by c_name")
    logPlan("inclauseTest1", df)
    df.explain(true)
  }

  test("inclauseStructTest1") {
    val df = sqlAndLog("inclauseStructTest1", "select c_name, sum(c_acctbal) as bal " +
      "from orderLineItemPartSupplier " +
      "where struct(c_mktsegment, c_nation)  in (struct('MACHINERY', 'JAPAN'), " +
      "struct('HOUSEHOLD', 'BRAZIL')) " +
      "group by c_name")
    logPlan("inclauseStructTest1", df)
    df.explain(true)
  }
}
