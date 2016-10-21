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

import org.apache.spark.sql.SPLLogging
import org.scalatest.BeforeAndAfterAll

// scalastyle:off line.size.limit
class DruidReWriteOrderCTest extends BaseTest with BeforeAndAfterAll with SPLLogging {
  cTest("druidRewriteOrderT1",
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-07'" +
      "group by l_returnflag, l_linestatus " +
      "order by l_linestatus"
    ,
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-07'" +
      "group by l_returnflag, l_linestatus " +
      "order by l_linestatus"
  )
  cTest("druidRewriteOrderT2",
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a," +
      "count(distinct o_orderkey)  " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-07'" +
      "group by l_returnflag, l_linestatus " +
      "order by l_returnflag " +
      "limit 2"
    ,
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a," +
      "count(distinct o_orderkey)  " +
      "from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-07'" +
      "group by l_returnflag, l_linestatus " +
      "order by l_returnflag " +
      "limit 2"
  )
  cTest("druidRewriteOrderT3",
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-07'" +
      "group by l_returnflag, l_linestatus " +
      "order by count(*)"
    ,
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-07'" +
      "group by l_returnflag, l_linestatus " +
      "order by count(*)"
  )
  cTest("druidRewriteOrderT4",
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-07'" +
      "group by l_returnflag, l_linestatus " +
      "order by s"
    ,
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-07'" +
      "group by l_returnflag, l_linestatus " +
      "order by s"
  )
  cTest("druidRewriteOrderT5",
    "select l_returnflag as r, l_linestatus as ls, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-07'" +
      "group by l_returnflag, l_linestatus " +
      "order by s, ls, r " +
      "limit 3"
    ,
    "select l_returnflag as r, l_linestatus as ls, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-07'" +
      "group by l_returnflag, l_linestatus " +
      "order by s, ls, r " +
      "limit 3"
  )
  cTest("druidRewriteOrderT6",
    "select l_returnflag as r, l_linestatus as ls, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-07'" +
      "group by l_returnflag, l_linestatus " +
      "order by m desc, s, r " +
      "limit 3"
    ,
    "select l_returnflag as r, l_linestatus as ls, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-07'" +
      "group by l_returnflag, l_linestatus " +
      "order by m desc, s, r " +
      "limit 3"
  )
  cTest("druidRewriteOrderT7",
    "select l_returnflag as r, l_linestatus as ls, " +
      "count(*) + 1 as c, sum(l_extendedprice) as s, max(ps_supplycost) as m, " +
      "avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-07'" +
      "group by l_returnflag, l_linestatus " +
      "order by c " +
      "limit 3"
    ,
    "select l_returnflag as r, l_linestatus as ls, " +
      "count(*) + 1 as c, sum(l_extendedprice) as s, max(ps_supplycost) as m, " +
      "avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-07'" +
      "group by l_returnflag, l_linestatus " +
      "order by c " +
      "limit 3"
  )
}
