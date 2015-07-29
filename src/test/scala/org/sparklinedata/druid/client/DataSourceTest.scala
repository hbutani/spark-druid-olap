package org.sparklinedata.druid.client

import org.apache.spark.sql.test.TestSQLContext._
import org.json4s.Extraction
import org.json4s.jackson.JsonMethods._
import org.sparklinedata.druid.DruidQuery
import org.sparklinedata.druid.metadata.{FunctionalDependency, FunctionalDependencyType}

class DataSourceTest extends BaseTest {

  import org.sparklinedata.druid.Utils._

  test("baseTable") {
    sql("select * from orderLineItemPartSupplierBase").show(10)
  }

  test("noQuery") {
    sql("select * from orderLineItemPartSupplier").show(10)
  }

  test("tpchQ1") {

    val dq =
      compact(render(Extraction.decompose(new DruidQuery(TPCHQueries.q1)))).replace('\n', ' ')

    val q = s"""CREATE TEMPORARY TABLE orderLineItemPartSupplier2
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "orderLineItemPartSupplierBase",
      timeDimensionColumn "l_shipdate",
      druidDatasource "tpch",
      druidHost "localhost",
      druidPort "8082",
      columnMapping '$colMapping',
      functionalDependencies '$functionalDependencies',
      druidQuery '$dq')""".stripMargin

    println(q)

    sql(q)

    sql("select * from orderLineItemPartSupplier2").show(10)
  }

  test("tpchQ1MonthGrain") {

    val dq =
      compact(render(Extraction.decompose(new DruidQuery(TPCHQueries.q1MonthGrain)))
      ).replace('\n', ' ')

    sql(

      s"""CREATE TEMPORARY TABLE orderLineItemPartSupplier2
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "orderLineItemPartSupplierBase",
      timeDimensionColumn "l_shipdate",
      druidDatasource "tpch",
      druidHost "localhost",
      druidPort "8082",
      columnMapping '$colMapping',
      functionalDependencies '$functionalDependencies',
      druidQuery '$dq')""".stripMargin
    )

    sql("select * from orderLineItemPartSupplier2").show(10)
  }

  test("t2") {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._

    val fd = FunctionalDependency("a", "b", FunctionalDependencyType.OneToOne)
    println(pretty(render(Extraction.decompose(fd))))
  }


}
