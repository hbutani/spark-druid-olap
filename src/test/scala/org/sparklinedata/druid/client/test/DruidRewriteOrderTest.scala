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

class DruidRewriteOrderTest extends BaseTest {


  test("basicAggOrderByDimension",
    "select l_returnflag, l_linestatus, " +
        "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
        "from orderLineItemPartSupplier " +
        "group by l_returnflag, l_linestatus " +
        "order by l_linestatus",
    1,
    true
  )

  test("basicAggOrderByDimensionLimit",
    "select l_returnflag, l_linestatus, " +
        "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a," +
        "count(distinct o_orderkey)  " +
        "from orderLineItemPartSupplier " +
        "group by l_returnflag, l_linestatus " +
        "order by l_returnflag " +
        "limit 2",
    2,
    true
  )

  test("basicAggOrderByMetric",
    "select l_returnflag, l_linestatus, " +
        "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
        "from orderLineItemPartSupplier " +
        "group by l_returnflag, l_linestatus " +
        "order by count(*)",
    1,
    true,
    true
  )

  test("basicAggOrderByMetric2",
    "select l_returnflag, l_linestatus, " +
        "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
        "from orderLineItemPartSupplier " +
        "group by l_returnflag, l_linestatus " +
        "order by s",
    1,
    true
  )

  test("basicAggOrderByLimitFull",
    "select l_returnflag as r, l_linestatus as ls, " +
        "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
        "from orderLineItemPartSupplier " +
        "group by l_returnflag, l_linestatus " +
        "order by s, ls, r " +
        "limit 3",
    1,
    true
  )

  test("basicAggOrderByLimitFull2",
    "select l_returnflag as r, l_linestatus as ls, " +
        "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
        "from orderLineItemPartSupplier " +
        "group by l_returnflag, l_linestatus " +
        "order by m desc, s, r " +
        "limit 3",
    1,
    true
  )

  test("sortNotPushed",
    "select l_returnflag as r, l_linestatus as ls, " +
        "count(*) + 1 as c, sum(l_extendedprice) as s, max(ps_supplycost) as m, " +
      "avg(ps_availqty) as a  " +
        "from orderLineItemPartSupplier " +
        "group by l_returnflag, l_linestatus " +
        "order by c " +
        "limit 3",
    1,
    true
  )

}
