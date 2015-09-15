package org.sparklinedata.druid.client

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.sparklinedata.spark.dateTime.dsl.expressions._

import scala.language.postfixOps

class DruidRewriteCubeTest extends BaseTest {


  test("basicCube") {
    val df = sqlAndLog("basicAggOrderByDimension",
      "select l_returnflag, l_linestatus, " +
        "count(*), sum(l_extendedprice) as s " +
        "from orderLineItemPartSupplier " +
        "group by l_returnflag, l_linestatus with cube")
    logPlan("basicAggOrderByDimension", df)

    //df.show()
  }

  test("testUnion") {
    val df = sqlAndLog("basicAggOrderByDimension",
      "select l_returnflag, l_linestatus, " +
        "count(*), sum(l_extendedprice) as s " +
        "from orderLineItemPartSupplier " +
        "group by l_returnflag, l_linestatus " +
        "union all " +
        "select l_returnflag, null, " +
        "count(*), sum(l_extendedprice) as s " +
        "from orderLineItemPartSupplier " +
        "group by l_returnflag ")
    logPlan("testUnion", df)

    //df.show()
  }

  test("ShipDateYearAggCube") {

    val shipDtYrGroup = dateTime('l_shipdate) year

    val df = sqlAndLog("basicAgg",
      date"""select l_returnflag, l_linestatus, $shipDtYrGroup, count(*),
      sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a,
      count(distinct o_orderkey)
      from orderLineItemPartSupplier group by l_returnflag, l_linestatus, $shipDtYrGroup
      with Cube""")
    logPlan("basicAgg", df)

    //df.show()
  }

  test("basicFilterCube") {
    val df = sqlAndLog("basicAggOrderByDimension",
      "select s_nation, l_returnflag, l_linestatus, " +
        "count(*), sum(l_extendedprice) as s " +
        "from orderLineItemPartSupplier " +
        "where s_nation = 'FRANCE' " +
        "group by s_nation, l_returnflag, l_linestatus with cube")
    logPlan("basicAggOrderByDimension", df)

    //df.show()
  }

  test("basicFilterRollup") {
    val df = sqlAndLog("basicAggOrderByDimension",
      "select l_returnflag, l_linestatus, grouping__id, " +
        "count(*), sum(l_extendedprice) as s " +
        "from orderLineItemPartSupplier " +
        "where s_nation = 'FRANCE' " +
        "group by l_returnflag, l_linestatus with rollup")
    logPlan("basicAggOrderByDimension", df)

    //df.show()
  }

  test("basicFilterGroupingSet") {
    val df = sqlAndLog("basicAggOrderByDimension",
      "select l_returnflag, l_linestatus, grouping__id, " +
        "count(*), sum(l_extendedprice) as s " +
        "from orderLineItemPartSupplier " +
        "where s_nation = 'FRANCE' " +
        "group by l_returnflag, l_linestatus grouping sets(l_returnflag, l_linestatus, ())")
    logPlan("basicAggOrderByDimension", df)

    //df.show()
  }

  test("basicCubeWithExpr") {
    val df = sqlAndLog("basicAggOrderByDimension",
      "select lower(l_returnflag), l_linestatus, " +
        "count(*), sum(l_extendedprice) as s " +
        "from orderLineItemPartSupplier " +
        "group by lower(l_returnflag), l_linestatus with cube")
    logPlan("basicAggOrderByDimension", df)

    //df.show()
  }

}