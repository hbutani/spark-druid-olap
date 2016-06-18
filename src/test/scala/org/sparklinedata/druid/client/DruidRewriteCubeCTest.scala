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

import org.sparklinedata.spark.dateTime.dsl.expressions._
import org.apache.spark.Logging
import org.scalatest.BeforeAndAfterAll

import scala.language.postfixOps
import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.sparklinedata.spark.dateTime.dsl.expressions._


class DruidRewriteCubeCTest extends BaseTest with BeforeAndAfterAll with Logging {
  ignore("ShipDateYearAggCube") { td =>

    val shipDtYrGroup = dateTime('l_shipdate) year

    cTest("duirdrewriteCubeT3",
      date"""select l_returnflag, l_linestatus, $shipDtYrGroup, count(*),
      sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a,
      count(distinct o_orderkey)
      from orderLineItemPartSupplier group by l_returnflag, l_linestatus, $shipDtYrGroup
      with Cube"""
      ,
      date"""select l_returnflag, l_linestatus, $shipDtYrGroup, count(*),
      sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a,
      count(distinct o_orderkey)
      from orderLineItemPartSupplierBase group by l_returnflag, l_linestatus, $shipDtYrGroup
      with Cube"""
    )
  }



  cTest("duirdrewriteCubeT1",
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01' and l_shipdate <= '1997-01-01' " +
      "group by l_returnflag, l_linestatus with cube"
    ,
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s " +
      "from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01' and l_shipdate <= '1997-01-01' " +
      "group by l_returnflag, l_linestatus with cube"
  )

  cTest("duirdrewriteCubeT2",
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01' and l_shipdate <= '1997-01-01' " +
      "group by l_returnflag, l_linestatus " +
      "union all " +
      "select l_returnflag, null, " +
      "count(*), sum(l_extendedprice) as s " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01' and l_shipdate <= '1997-01-01' " +
      "group by l_returnflag "
    ,
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s " +
      "from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01' and l_shipdate <= '1997-01-01' " +
      "group by l_returnflag, l_linestatus " +
      "union all " +
      "select l_returnflag, null, " +
      "count(*), sum(l_extendedprice) as s " +
      "from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' " +
      "group by l_returnflag "
  )

  cTest("duirdrewriteCubeT4",
    "select s_nation, l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s " +
      "from orderLineItemPartSupplier " +
      "where s_nation = 'FRANCE' and l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' " +
      "group by s_nation, l_returnflag, l_linestatus with cube"
    ,
    "select s_nation, l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s " +
      "from orderLineItemPartSupplierBase " +
      "where s_nation = 'FRANCE' and  l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' " +
      "group by s_nation, l_returnflag, l_linestatus with cube"
  )

  cTest("duirdrewriteCubeT5",
    "select l_returnflag, l_linestatus, grouping__id, " +
      "count(*), sum(l_extendedprice) as s " +
      "from orderLineItemPartSupplier " +
      "where s_nation = 'FRANCE' and  l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' " +
      "group by l_returnflag, l_linestatus with rollup"
    ,
    "select l_returnflag, l_linestatus, grouping__id, " +
      "count(*), sum(l_extendedprice) as s " +
      "from orderLineItemPartSupplierBase " +
      "where s_nation = 'FRANCE' and  l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' " +
      "group by l_returnflag, l_linestatus with rollup"
  )

  cTest("duirdrewriteCubeT6",
    "select l_returnflag, l_linestatus, grouping__id, " +
      "count(*), sum(l_extendedprice) as s " +
      "from orderLineItemPartSupplier " +
      "where s_nation = 'FRANCE' and  l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' " +
      "group by l_returnflag, l_linestatus grouping sets(l_returnflag, l_linestatus, ())"
    ,
    "select l_returnflag, l_linestatus, grouping__id, " +
      "count(*), sum(l_extendedprice) as s " +
      "from orderLineItemPartSupplierBase " +
      "where s_nation = 'FRANCE' and  l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' " +
      "group by l_returnflag, l_linestatus grouping sets(l_returnflag, l_linestatus, ())"
  )
  cTest("duirdrewriteCubeT7",
    "select lower(l_returnflag), l_linestatus, " +
      "count(*), sum(l_extendedprice) as s " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      "group by lower(l_returnflag), l_linestatus with cube"
    ,
    "select lower(l_returnflag), l_linestatus, " +
      "count(*), sum(l_extendedprice) as s " +
      "from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      "group by lower(l_returnflag), l_linestatus with cube"
  )

}
