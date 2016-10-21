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

import org.apache.http.concurrent.Cancellable
import org.apache.spark.sql.SPLLogging
import org.apache.spark.sql.sources.druid.JsonOperations
import org.json4s._
import org.scalatest.{BeforeAndAfterAll, fixture}
import org.sparklinedata.druid._
import org.sparklinedata.druid.client.{CancellableHolder, DruidQueryServerClient}

object CancelThread extends CancellableHolder with SPLLogging {

  @volatile
  private var cancellable : Cancellable = null

  @volatile
  var wasCancelTriggered : Boolean = false

  val t = new Thread(new Runnable {
    override def run(): Unit = {
      while(true) {
        Thread.sleep(50);
        if (cancellable != null) {
          log.info(s"aborted http request")
          cancellable.cancel()
        }
      }
    }
  })
  t.setDaemon(true)
  t.setName("cancel thread")
  t.start()

  def setCancellable(c : Cancellable) = {
    wasCancelTriggered = true
    cancellable = c
  }
}

class CancelDruidRequestTest extends fixture.FunSuite with
  fixture.TestDataFixture with BeforeAndAfterAll with SPLLogging with JsonOperations {

  val useSmile : Boolean = true

  val queryUrl = "http://192.168.1.5:8082/druid/v2/?pretty"

  val queryJson =
    """
      |{
      |  "jsonClass" : "GroupByQuerySpec",
      |  "queryType" : "groupBy",
      |  "dataSource" : "tpch",
      |  "dimensions" : [ {
      |    "jsonClass" : "ExtractionDimensionSpec",
      |    "type" : "extraction",
      |    "dimension" : "__time",
      |    "outputName" : "alias-1",
      |    "extractionFn" : {
      |      "jsonClass" : "TimeFormatExtractionFunctionSpec",
      |      "type" : "timeFormat",
      |      "format" : "YYYY-MM-dd HH:mm:ss",
      |      "timeZone" : "UTC",
      |      "locale" : "en_US"
      |    }
      |  } ],
      |  "granularity" : "all",
      |  "aggregations" : [ {
      |    "jsonClass" : "FunctionAggregationSpec",
      |    "type" : "count",
      |    "name" : "addCountAggForNoMetricQuery",
      |    "fieldName" : "count"
      |  } ],
      |  "intervals" : [ "1993-01-01T00:00:00.000Z/1997-12-31T00:00:01.000Z" ],
      |  "context" : {
      |    "queryId" : "query-1472571855113427000"
      |  }
      |}
    """.stripMargin


  override def beforeAll() = {
  }

  test("cancel")  { td =>

    import jsonMethods._

    val jV = parse(queryJson)

    val dClient = new DruidQueryServerClient("192.168.1.5:8082", true)
    dClient.setCancellableHolder(CancelThread)

    try {
      dClient.postQuery(queryUrl,
        new DummyQuerySpec(),
        jV)
    } catch {
      case _ if CancelThread.wasCancelTriggered => ()
      case t : Throwable => throw t
     }
  }
}
