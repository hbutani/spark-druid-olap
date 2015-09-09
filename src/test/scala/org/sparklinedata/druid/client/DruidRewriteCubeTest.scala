package org.sparklinedata.druid.client

class DruidRewriteCubeTest extends BaseTest {


  test("basicCube") {
    val df = sqlAndLog("basicAggOrderByDimension",
      "select l_returnflag, l_linestatus, " +
        "count(*), sum(l_extendedprice) as s " +
        "from orderLineItemPartSupplier " +
        "group by l_returnflag, l_linestatus with cube")
    logPlan("basicAggOrderByDimension", df)

    df.show()
  }

  test("basicFilterCube") {
    val df = sqlAndLog("basicAggOrderByDimension",
      "select l_returnflag, l_linestatus, " +
        "count(*), sum(l_extendedprice) as s " +
        "from orderLineItemPartSupplier " +
        "where s_nation = 'FRANCE' " +
        "group by l_returnflag, l_linestatus with cube")
    logPlan("basicAggOrderByDimension", df)

    df.show()
  }

  test("basicFilterRollup") {
    val df = sqlAndLog("basicAggOrderByDimension",
      "select l_returnflag, l_linestatus, grouping__id, " +
        "count(*), sum(l_extendedprice) as s " +
        "from orderLineItemPartSupplier " +
        "where s_nation = 'FRANCE' " +
        "group by l_returnflag, l_linestatus with rollup")
    logPlan("basicAggOrderByDimension", df)

    df.show()
  }

  test("basicFilterGroupingSet") {
    val df = sqlAndLog("basicAggOrderByDimension",
      "select l_returnflag, l_linestatus, grouping__id, " +
        "count(*), sum(l_extendedprice) as s " +
        "from orderLineItemPartSupplier " +
        "where s_nation = 'FRANCE' " +
        "group by l_returnflag, l_linestatus grouping sets(l_returnflag, l_linestatus, ())")
    logPlan("basicAggOrderByDimension", df)

    df.show()
  }

  test("basicCubeExpr") {
    val df = sqlAndLog("basicAggOrderByDimension",
      "select lower(l_returnflag), l_linestatus, " +
        "count(*), sum(l_extendedprice) as s " +
        "from orderLineItemPartSupplier " +
        "group by lower(l_returnflag), l_linestatus with cube")
    logPlan("basicAggOrderByDimension", df)

    df.show()
  }

}