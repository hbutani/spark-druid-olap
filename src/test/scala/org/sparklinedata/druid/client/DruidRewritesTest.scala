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

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.sparklinedata.spark.dateTime.dsl.expressions._

import scala.language.postfixOps

class DruidRewritesTest extends BaseTest {

  test("basicAgg") {td =>
    val df = sqlAndLog("basicAgg",
      "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a," +
      "count(distinct o_orderkey)  " +
      "from orderLineItemPartSupplier group by l_returnflag, l_linestatus")
    logPlan("basicAgg", df)

    // df.show()
  }

  test("basicAggWithProject") {td =>
    val df = sqlAndLog("basicAggWithProject",
      "select f, s, " +
      "count(*)  " +
      "from (select l_returnflag f, l_linestatus s from orderLineItemPartSupplier) t group by f, s")
    logPlan("basicAggWithProject", df)


    // df.show()
  }

  test("dateFilter") {td =>

    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)

    val df = sqlAndLog("dateFilter",
      date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region
         from orderLineItemPartSupplier
      ) t
      where $shipDtPredicate
      group by f,s
      order by f,s
""")

    logPlan("dateFilter", df)

    // df.show()
  }

  test("intervalFilter") {td =>
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)

    val df = sqlAndLog("intervalFilter",
      date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplier
      ) t
      where $shipDtPredicate and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
      group by f,s
      order by f,s
""")

    logPlan("intervalFilter", df)

    // df.show()

  }

  test("intervalFilter2") {td =>
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicate2 = dateTime('l_shipdate) > (dateTime("1995-12-01"))

    val df = sqlAndLog("intervalFilter2",
      date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplier
      ) t
      where $shipDtPredicate and $shipDtPredicate2
      group by f,s
      order by f,s
""")

    logPlan("intervalFilter2", df)

    // df.show()

  }

  test("intervalFilter3") {td =>
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicate2 = dateTime('l_shipdate) < (dateTime("1995-12-01"))

    val df = sqlAndLog("intervalFilter3",
      date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplier
      ) t
      where $shipDtPredicate and $shipDtPredicate2
      group by f,s
      order by f,s
""")

    logPlan("intervalFilter3", df)

    // df.show()

  }

  test("intervalFilter4") {td =>
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicate2 = dateTime('l_shipdate) > (dateTime("1997-12-02"))

    val df = sqlAndLog("intervalFilter4",
      date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplier
      ) t
      where $shipDtPredicate and $shipDtPredicate2
      group by f,s
      order by f,s
""")

    logPlan("intervalFilter4", df)

    // df.show()

  }

  test("dimFilter2") {td =>
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)

    val df = sqlAndLog("dimFilter2",
      date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate,
         s_region, s_nation, c_nation, p_type
         from orderLineItemPartSupplier
      ) t
      where $shipDtPredicate and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 ) and p_type = 'ECONOMY ANODIZED STEEL'
      group by f,s
      order by f,s
""")

    logPlan("dimFilter2", df)

    // df.show()

  }

  test("dimFilter3") {td =>
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)

    val df = sqlAndLog("dimFilter3",
      date"""
      select s_nation, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate,
         s_region, s_nation, c_nation, p_type
         from orderLineItemPartSupplier
      ) t
      where $shipDtPredicate and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
      group by s_nation
      order by s_nation
""")

    logPlan("dimFilter3", df)

    // df.show()

  }

  test("dimFilter4") {td =>
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)

    val df = sqlAndLog("dimFilter4",
      date"""
      select s_nation, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate,
         s_region, s_nation, c_nation, p_type
         from orderLineItemPartSupplier
      ) t
      where $shipDtPredicate and s_nation >= 'FRANCE'
      group by s_nation
      order by s_nation
""")

    logPlan("dimFilter4", df)

    // df.show()

  }

  test("projFilterAgg") {td =>
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicate2 = dateTime('l_shipdate) > (dateTime("1995-12-01"))

    val df = sqlAndLog("projFilterAgg",
      date"""
      select s_nation,
      count(*) as count_order,
      sum(l_extendedprice) as s,
      max(ps_supplycost) as m,
      avg(ps_availqty) as a,
      count(distinct o_orderkey)
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate,
         s_region, s_nation, c_nation, p_type,
         l_extendedprice, ps_supplycost, ps_availqty, o_orderkey
         from orderLineItemPartSupplier
         where p_type = 'ECONOMY ANODIZED STEEL'
      ) t
      where $shipDtPredicate and
            $shipDtPredicate2 and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
      group by s_nation
      order by s_nation
""")
    logPlan("projFilterAgg", df)

    // df.show()
  }

  test("ShipDateYearAgg") {td =>

    val shipDtYrGroup = dateTime('l_shipdate) year

    val df = sqlAndLog("basicAgg",
      date"""select l_returnflag, l_linestatus, $shipDtYrGroup, count(*),
      sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a,
      count(distinct o_orderkey)
      from orderLineItemPartSupplier group by l_returnflag, l_linestatus, $shipDtYrGroup""")
    logPlan("basicAgg", df)

    // df.show()
  }

  test("OrderDateYearAgg") {td =>

    val orderDtYrGroup = dateTime('o_orderdate) year

    val df = sqlAndLog("basicAgg",
      date"""select l_returnflag, l_linestatus, $orderDtYrGroup, count(*),
      sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a,
      count(distinct o_orderkey)
      from orderLineItemPartSupplier group by l_returnflag, l_linestatus, $orderDtYrGroup""")
    logPlan("basicAgg", df)

    // df.show()
  }

}

