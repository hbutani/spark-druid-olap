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

import java.io.FileInputStream

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.sources.druid.DruidQueryResultIterator
import org.json4s.Extraction
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.sparklinedata.druid.Utils

class DruidClientTest extends FunSuite with BeforeAndAfterAll with TestUtils {

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

  test("streamQueryResult") {

    def qRis = getClass.getClassLoader.getResourceAsStream("sampleQueryResult.json")

    for(i <- 0 to 5) {
      recordTime("streamed read") {

        val is = IOUtils.toBufferedInputStream(qRis)
        val it = DruidQueryResultIterator(is)
        while (it.hasNext) {
          it.next
        }
      }
      recordTime("list read") {
        val is = IOUtils.toBufferedInputStream(qRis)
        val it = DruidQueryResultIterator(is, (), true)
        while (it.hasNext) {
          it.next
        }
      }

    }

  }

}

