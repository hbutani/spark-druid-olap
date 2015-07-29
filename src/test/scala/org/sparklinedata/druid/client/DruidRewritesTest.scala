package org.sparklinedata.druid.client

import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._

class DruidRewritesTest extends BaseTest {

  test("basicAgg") {
    val df = sql("select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a," +
      "count(distinct o_orderkey)  " +
      "from orderLineItemPartSupplier group by l_returnflag, l_linestatus")
    println(df.queryExecution.analyzed)
    println(df.queryExecution.sparkPlan)

    df.show()
  }

  test("basicAggWithProject") {
    val df = sql("select f, s, " +
      "count(*)  " +
      "from (select l_returnflag f, l_linestatus s from orderLineItemPartSupplier) t group by f, s")
    println(df.queryExecution.optimizedPlan)
    println(df.queryExecution.sparkPlan)

    //df.show()
  }

  test("dateFilter") {
    import com.github.nscala_time.time.Imports._
    import org.apache.spark.sql.catalyst.dsl.expressions._
    import org.sparklinedata.spark.dateTime.Functions._
    import org.sparklinedata.spark.dateTime.dsl.expressions._
    register(TestSQLContext)

    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)

    val df = sql( date"""
select
f,
s,
count(*) as count_order
from
( select l_returnflag as f, l_linestatus as s, l_shipdate from orderLineItemPartSupplier) t
where
$shipDtPredicate
group by
f,
s
order by
f,
s
""")

    println(df.queryExecution.optimizedPlan)
    println(df.queryExecution.sparkPlan)

    df.show()
  }

}

