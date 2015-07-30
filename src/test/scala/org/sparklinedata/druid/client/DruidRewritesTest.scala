package org.sparklinedata.druid.client

import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._
import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.sparklinedata.spark.dateTime.Functions._
import org.sparklinedata.spark.dateTime.dsl.expressions._

class DruidRewritesTest extends BaseTest {


  register(TestSQLContext)

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

    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)

    val df = sql( date"""
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

    println(df.queryExecution.optimizedPlan)
    println(df.queryExecution.sparkPlan)

    df.show()
  }

  test("intervalFilter") {
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)

    val df = sql( date"""
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

    println(df.queryExecution.optimizedPlan)
    println(df.queryExecution.sparkPlan)

    df.show()

  }

  test("intervalFilter2") {
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicate2 = dateTime('l_shipdate) > (dateTime("1995-12-01"))

    val df = sql( date"""
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

    println(df.queryExecution.optimizedPlan)
    println(df.queryExecution.sparkPlan)

    df.show()

  }

  test("intervalFilter3") {
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicate2 = dateTime('l_shipdate) < (dateTime("1995-12-01"))

    val df = sql( date"""
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

    println(df.queryExecution.optimizedPlan)
    println(df.queryExecution.sparkPlan)

    df.show()

  }

  test("intervalFilter4") {
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicate2 = dateTime('l_shipdate) > (dateTime("1997-12-02"))

    val df = sql( date"""
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

    println(df.queryExecution.optimizedPlan)
    println(df.queryExecution.sparkPlan)

    df.show()

  }

  test("dimFilter2") {
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)

    val df = sql( date"""
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

    println(df.queryExecution.optimizedPlan)
    println(df.queryExecution.sparkPlan)

    df.show()

  }

  test("dimFilter3") {
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)

    val df = sql( date"""
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

    println(df.queryExecution.optimizedPlan)
    println(df.queryExecution.sparkPlan)

    df.show()

  }

  test("dimFilter4") {
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)

    val df = sql( date"""
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

    println(df.queryExecution.optimizedPlan)
    println(df.queryExecution.sparkPlan)

    df.show()

  }

  test("projFilterAgg") {
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicate2 = dateTime('l_shipdate) > (dateTime("1995-12-01"))

    val df = sql(date"""
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
    println(df.queryExecution.analyzed)
    println(df.queryExecution.sparkPlan)

    df.show()
  }

}

