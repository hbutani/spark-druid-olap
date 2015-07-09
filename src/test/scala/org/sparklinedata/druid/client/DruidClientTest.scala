package org.sparklinedata.druid.client

import org.scalatest.{BeforeAndAfterAll, FunSuite}

class DruidClientTest extends FunSuite with BeforeAndAfterAll {

  var client : DruidClient = _

  override def beforeAll() = {
    client = new DruidClient("localhost", 8082)
  }

  test("timeBoundary") {
    println(client.timeBoundary("tpch"))
  }

  test("metaData") {
    println(client.metadata("tpch"))
  }

}
