package org.sparklinedata.druid.client

import org.json4s.Extraction
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.sparklinedata.druid.Utils

class DruidClientTest extends FunSuite with BeforeAndAfterAll {

  import TPCHQueries._

  var client : DruidClient = _

  import Utils._

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
    println(pretty(render(Extraction.decompose(q1))))

    val r = client.executeQuery(q1)
    r.foreach(println)

  }

  test("tpchQ3") {
    println(pretty(render(Extraction.decompose(q3))))

    val r = client.executeQuery(q3)
    r.foreach(println)

  }

  test("tpchQ1MonthGrain") {
    println(pretty(render(Extraction.decompose(q1))))

    val r = client.executeQuery(q1)
    r.foreach(println)
  }

}
