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

import com.github.nscala_time.time.Imports._
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.scalatest.BeforeAndAfterAll
import org.sparklinedata.spark.dateTime.dsl.expressions._

import scala.language.postfixOps

// scalastyle:off line.size.limit
class DruidRewritesCTest extends BaseTest with BeforeAndAfterAll with Logging{
  cTest("druidrewriteT1",
    "select count(distinct o_custkey) from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'",
    "select count(distinct o_custkey) from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'"
  )

  cTest("druidrewriteT2",
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a," +
      "count(distinct o_orderkey)  " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-07' group by l_returnflag, l_linestatus",
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a," +
      "count(distinct o_orderkey)  " +
      "from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-07' group by l_returnflag, l_linestatus"
  )

  cTest("druidrewriteT3",
    "select l_returnflag, l_linestatus " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' " +
      "group by l_returnflag, l_linestatus",
    "select l_returnflag, l_linestatus " +
      "from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' " +
      "group by l_returnflag, l_linestatus"
  )

  cTest("druidrewriteT4",
    "select f, s, " +
      "count(*)  " +
      "from (select l_returnflag f, l_linestatus s " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' ) t  group by f, s",
    "select f, s, " +
      "count(*)  " +
      "from (select l_returnflag f, l_linestatus s " +
      "from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' ) t  group by f, s"
  )

  cTest("druidrewriteT5", {
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region
         from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'
      ) t
      where $shipDtPredicate
      group by f,s
      order by f,s
"""},
    {
      val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
      date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region
         from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'
      ) t
      where $shipDtPredicate
      group by f,s
      order by f,s
"""}
  )

  cTest("druidrewriteT6", {
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'
      ) t
      where $shipDtPredicate and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
      group by f,s
      order by f,s
      """
  },
    {val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
      date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'
      ) t
      where $shipDtPredicate and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
      group by f,s
      order by f,s
      """
    }
  )

  cTest("druidrewriteT7", {
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicate2 = dateTime('l_shipdate) > (dateTime("1995-12-01"))

    date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'
      ) t
      where $shipDtPredicate and $shipDtPredicate2
      group by f,s
      order by f,s
"""
  },
    {
      val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
      val shipDtPredicate2 = dateTime('l_shipdate) > (dateTime("1995-12-01"))

      date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'
      ) t
      where $shipDtPredicate and $shipDtPredicate2
      group by f,s
      order by f,s
"""
    }
  )

  cTest("druidrewriteT8", {
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicate2 = dateTime('l_shipdate) < (dateTime("1995-12-01"))

    date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'
      ) t
      where $shipDtPredicate and $shipDtPredicate2
      group by f,s
      order by f,s
"""
  },
    {
      val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
      val shipDtPredicate2 = dateTime('l_shipdate) < (dateTime("1995-12-01"))

      date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'
      ) t
      where $shipDtPredicate and $shipDtPredicate2
      group by f,s
      order by f,s
"""
    })

  cTest("druidrewriteT9", {
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicate2 = dateTime('l_shipdate) > (dateTime("1997-12-02"))

    date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'
      ) t
      where $shipDtPredicate and $shipDtPredicate2
      group by f,s
      order by f,s
"""
  },
    {
      val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
      val shipDtPredicate2 = dateTime('l_shipdate) > (dateTime("1997-12-02"))

      date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'
      ) t
      where $shipDtPredicate and $shipDtPredicate2
      group by f,s
      order by f,s
"""
    }
  )
  cTest("druidrewriteT10", {
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)

    date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate,
         s_region, s_nation, c_nation, p_type
         from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'
      ) t
      where $shipDtPredicate and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 ) and p_type = 'ECONOMY ANODIZED STEEL'
      group by f,s
      order by f,s
"""
  },
    {
      val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)

      date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate,
         s_region, s_nation, c_nation, p_type
         from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'
      ) t
      where $shipDtPredicate and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 ) and p_type = 'ECONOMY ANODIZED STEEL'
      group by f,s
      order by f,s
"""
    }
  )

  cTest("druidrewriteT11", {
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)

    date"""
      select s_nation, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate,
         s_region, s_nation, c_nation, p_type
         from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'
      ) t
      where $shipDtPredicate and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
      group by s_nation
      order by s_nation
"""
  },
    {
      val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)

      date"""
      select s_nation, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate,
         s_region, s_nation, c_nation, p_type
         from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'
      ) t
      where $shipDtPredicate and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
      group by s_nation
      order by s_nation
    """
    }
  )

  cTest("druidrewriteT12", {
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)

    date"""
      select s_nation, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate,
         s_region, s_nation, c_nation, p_type
         from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'
      ) t
      where $shipDtPredicate and s_nation >= 'FRANCE'
      group by s_nation
      order by s_nation
"""
  },
    {
      val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
      date"""
      select s_nation, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate,
         s_region, s_nation, c_nation, p_type
         from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'
      ) t
      where $shipDtPredicate and s_nation >= 'FRANCE'
      group by s_nation
      order by s_nation
    """
    }
  )

  cTest("druidrewriteT13", {
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicate2 = dateTime('l_shipdate) > (dateTime("1995-12-01"))

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
         where p_type = 'ECONOMY ANODIZED STEEL' and  l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'
      ) t
      where $shipDtPredicate and
            $shipDtPredicate2 and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
      group by s_nation
      order by s_nation
"""
  },
    {
      val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
      val shipDtPredicate2 = dateTime('l_shipdate) > (dateTime("1995-12-01"))

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
         from orderLineItemPartSupplierBase
         where p_type = 'ECONOMY ANODIZED STEEL' and l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'
      ) t
      where $shipDtPredicate and
            $shipDtPredicate2 and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
      group by s_nation
      order by s_nation
"""
    }
  )

  cTest("druidrewriteT14", {

    val shipDtYrGroup = dateTime('l_shipdate) year

    date"""select l_returnflag, l_linestatus, $shipDtYrGroup, count(*),
      sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a,
      count(distinct o_orderkey)
      from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-07' group by l_returnflag, l_linestatus, $shipDtYrGroup"""
  },
    {
      val shipDtYrGroup = dateTime('l_shipdate) year

      date"""select l_returnflag, l_linestatus, $shipDtYrGroup, count(*),
      sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a,
      count(distinct o_orderkey)
      from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-07' group by l_returnflag, l_linestatus, $shipDtYrGroup"""
    }
  )

  cTest("druidrewriteT15", {

    val orderDtYrGroup = dateTime('o_orderdate) year

    date"""select l_returnflag, l_linestatus, $orderDtYrGroup, count(*),
      sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a,
      count(distinct o_orderkey)
      from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-02' group by l_returnflag, l_linestatus, $orderDtYrGroup"""
  },
    {
      val orderDtYrGroup = dateTime('o_orderdate) year

      date"""select l_returnflag, l_linestatus, $orderDtYrGroup, count(*),
      sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a,
      count(distinct o_orderkey)
      from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1994-01-02'  group by l_returnflag, l_linestatus, $orderDtYrGroup"""
    }
  )

  cTest("druidrewriteT16",
    """select *
      |from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'
      |limit 3""".stripMargin,
    """select *
      |from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'
      |limit 3""".stripMargin
  )



}
