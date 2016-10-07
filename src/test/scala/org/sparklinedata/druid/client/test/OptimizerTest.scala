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

import org.apache.spark.Logging
import org.scalatest.BeforeAndAfterAll

class OptimizerTest extends BaseTest with BeforeAndAfterAll with Logging {
  test("gbPush1",
    """select r1.l_linenumber x, sum(r1.l_quantity) y, (mi + 10) as z
       from orderLineItemPartSupplier r1
       join
       (select min(l_linenumber)mi, sum(c_acctbal) ma, count(1) from orderLineItemPartSupplier
       where not(l_shipdate is null) having count(1) > 0) r2
       group by r1.l_linenumber, (mi + 10)
       order by x, y, z""".stripMargin,
    2,
    true, true)

  test("gbPush2",
    """select l_linenumber a, (mi + 10) b, z, mi % 2 , sum(l_quantity) c
       from
       (select r1.l_linenumber,  r1.l_quantity, mi, (mi + 10) as z
        from orderLineItemPartSupplier r1
       join
       (select min(l_linenumber)mi, sum(c_acctbal) ma, count(1)
       from orderLineItemPartSupplier
       where not(l_shipdate is null) having count(1) > 0) r2) r3
       group by l_linenumber, (mi + 10), z, mi % 2
       order by a, b, z, c, mi%2""".stripMargin,
    2,
    true, true)

  test("gbPush3",
    """select l_linenumber a, (mi + 10) b, z, mi % 2 , sum(l_quantity) c
       from
       (select r1.l_linenumber,  r1.l_quantity, mi, (mi + 10) as z, l_shipdate
        from orderLineItemPartSupplier r1
       join
       (select min(l_linenumber)mi, sum(c_acctbal) ma, count(1)
       from orderLineItemPartSupplier
       where not(l_shipdate is null) having count(1) > 0) r2) r3
       where not(cast(l_shipdate as timestamp)is null) and  not (l_linenumber='NA')
       group by l_linenumber, (mi + 10), z, mi % 2
       order by a, b, z, c, mi%2""".stripMargin,
    2,
    true, true)

  test("sumRewrite1",
    """select sum(5) s5
       from orderLineItemPartSupplier
       group by l_linenumber
       having s5 > 10
       order by  s5""".stripMargin,
    1,
    true, true)

  test("sumRewrite1B",
    """select sum(5) s5
       from orderLineItemPartSupplierBase
       group by l_linenumber
       having s5 > 10
       order by  s5""".stripMargin,
    0,
    true, true)

  test("sumRewrite2",
    """select l_linenumber a, (mi + 10) b, z, mi % 2 , sum(l_quantity) c, s5
       from
       (select r1.l_linenumber,  r1.l_quantity, mi, (mi + 10) as z, l_shipdate, s5
        from orderLineItemPartSupplier r1
       join
       (select min(l_linenumber)mi, sum(c_acctbal) ma, sum(5) s5, count(1)
       from orderLineItemPartSupplier
       where not(l_shipdate is null) having count(1) > 0) r2) r3
       where not(cast(l_shipdate as timestamp)is null) and  not (l_linenumber='NA')
       group by l_linenumber, (mi + 10), z, mi % 2, s5
       order by a, b, z, c, mi%2, s5""".stripMargin,
    2,
    true, true)


  test("sumRewrite2B",
    """select l_linenumber a, (mi + 10) b, z, mi % 2 , sum(l_quantity) c, s5
       from
       (select r1.l_linenumber,  r1.l_quantity, mi, (mi + 10) as z, l_shipdate, s5
        from orderLineItemPartSupplierBase r1
       join
       (select min(l_linenumber)mi, sum(c_acctbal) ma, sum(5) s5, count(1)
       from orderLineItemPartSupplierBase
       where not(l_shipdate is null) having count(1) > 0) r2) r3
       where not(cast(l_shipdate as timestamp)is null) and  not (l_linenumber='NA')
       group by l_linenumber, (mi + 10), z, mi % 2, s5
       order by a, b, z, c, mi%2, s5""".stripMargin,
    0,
    true, true)
}
