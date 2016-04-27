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
import org.scalatest.BeforeAndAfterAll

class FilterTest extends BaseTest with BeforeAndAfterAll with Logging {

  test("inclauseTest1",
    "select c_name, sum(c_acctbal) as bal " +
      "from orderLineItemPartSupplier " +
      "where c_mktsegment in ('MACHINERY', 'HOUSEHOLD') " +
      "group by c_name",
    1,
    true)

  test("inclauseTest2",
    "select c_name, sum(c_acctbal) as bal " +
      "from orderLineItemPartSupplier " +
      "where l_linenumber in (1, 2, 3, 4, 5, 6, 7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22," +
      "23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45) " +
      "group by c_name",
    1,
    true)

  test("notInclauseTest1",
    "select c_name, sum(c_acctbal) as bal " +
      "from orderLineItemPartSupplier " +
      "where c_mktsegment not in ('MACHINERY', 'HOUSEHOLD') " +
      "group by c_name",
    1,
    true
  )

  test("notEqTest1",
    "select c_name, sum(c_acctbal) as bal " +
      "from orderLineItemPartSupplier " +
      "where c_mktsegment !=  'MACHINERY' " +
      "group by c_name",
    1,
    true
  )
}
