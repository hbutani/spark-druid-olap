package org.sparklinedata.druid.client

import org.joda.time.Interval
import org.json4s.Extraction
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.sparklinedata.druid._

class DruidClientTest extends FunSuite with BeforeAndAfterAll {

  var client : DruidClient = _

  implicit val formats = org.json4s.DefaultFormats +
    new QueryResultRowSerializer ++ org.json4s.ext.JodaTimeSerializers.all

  override def beforeAll() = {
    client = new DruidClient("localhost", 8082)
  }

  test("timeBoundary") {
    println(client.timeBoundary("tpch"))
  }

  test("metaData") {
    println(client.metadata("tpch"))
  }

  test("tpchQ1") {

    val q1 = new GroupByQuerySpec("tpch",
      List(new DefaultDimensionSpec("l_returnflag"), new DefaultDimensionSpec("l_linestatus")),
    Some(new LimitSpec(10,
      List(new OrderByColumnSpec("l_returnflag"), new OrderByColumnSpec("l_linestatus")))
    ),
    None,
      Left("all"),
      None,
    List(
    FunctionAggregationSpec("longSum", "count", "count"),
      FunctionAggregationSpec("longSum", "sum_quantity", "sum_l_quantity"),
      FunctionAggregationSpec("doubleSum", "sum_base_price", "l_extendedprice"),
      FunctionAggregationSpec("doubleSum", "l_discount", "l_discount"),
      FunctionAggregationSpec("doubleSum", "l_tax", "l_tax"),
      new CardinalityAggregationSpec("count_order", List("o_orderkey"))
    ),
    Some(List(
      new ArithmeticPostAggregationSpec("sum_disc_price", "-",
        List(new FieldAccessPostAggregationSpec("sum_base_price"),
          new FieldAccessPostAggregationSpec("l_discount")), None),
      new ArithmeticPostAggregationSpec("sum_charge", "-",
        List(new FieldAccessPostAggregationSpec("sum_base_price"),
          new FieldAccessPostAggregationSpec("l_tax")), None),
      new ArithmeticPostAggregationSpec("avg_qty", "/",
        List(new FieldAccessPostAggregationSpec("sum_quantity"),
          new FieldAccessPostAggregationSpec("count")), None),
      new ArithmeticPostAggregationSpec("avg_price", "/",
        List(new FieldAccessPostAggregationSpec("sum_base_price"),
          new FieldAccessPostAggregationSpec("count")), None),
      new ArithmeticPostAggregationSpec("avg_disc", "/",
        List(new FieldAccessPostAggregationSpec("l_discount"),
          new FieldAccessPostAggregationSpec("count")), None)
    )),
    List("1993-01-01T00:00:00.000/1998-09-01T00:00:00.000")
    )

    println(pretty(render(Extraction.decompose(q1))))

    val r = client.executeQuery(q1)
    r.foreach(println)

  }

}
