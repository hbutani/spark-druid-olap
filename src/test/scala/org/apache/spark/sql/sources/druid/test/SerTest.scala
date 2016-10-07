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

package org.apache.spark.sql.sources.druid.test

import java.io.StringBufferInputStream

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}
import org.apache.spark.sql.sources.druid.{JsonOperations, OM}
import org.json4s.jackson.Json4sScalaModule
import org.json4s.{JValue, JsonAST}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.sparklinedata.druid.Utils
import org.sparklinedata.druid.client.SelectResultRow
import org.sparklinedata.druid.client.test.TestUtils

class SerTest extends FunSuite with BeforeAndAfterAll with TestUtils with JsonOperations {

  val useSmile : Boolean = false
  import jsonMethods._

  val json1 =
    """
      |[ {
      |  "timestamp" : "1993-01-01T08:00:00.000Z",
      |  "result" : {
      |    "pagingIdentifiers" : {
      |      "tpch_1993-01-01T00:00:00.000Z_1993-02-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 1,
      |      "tpch_1993-03-01T00:00:00.000Z_1993-04-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 0,
      |      "tpch_1993-04-01T00:00:00.000Z_1993-05-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 1,
      |      "tpch_1993-05-01T00:00:00.000Z_1993-06-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 1,
      |      "tpch_1993-06-01T00:00:00.000Z_1993-07-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 1,
      |      "tpch_1993-07-01T00:00:00.000Z_1993-08-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 1,
      |      "tpch_1993-08-01T00:00:00.000Z_1993-09-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 1,
      |      "tpch_1993-09-01T00:00:00.000Z_1993-10-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 2,
      |      "tpch_1993-10-01T00:00:00.000Z_1993-11-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 0,
      |      "tpch_1993-11-01T00:00:00.000Z_1993-12-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 1,
      |      "tpch_1993-12-01T00:00:00.000Z_1994-01-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 0,
      |      "tpch_1994-02-01T00:00:00.000Z_1994-03-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 7,
      |      "tpch_1994-03-01T00:00:00.000Z_1994-04-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 0,
      |      "tpch_1994-04-01T00:00:00.000Z_1994-05-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 0,
      |      "tpch_1994-06-01T00:00:00.000Z_1994-07-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 0,
      |      "tpch_1994-07-01T00:00:00.000Z_1994-08-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 2,
      |      "tpch_1994-08-01T00:00:00.000Z_1994-09-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 1,
      |      "tpch_1994-10-01T00:00:00.000Z_1994-11-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 1,
      |      "tpch_1994-11-01T00:00:00.000Z_1994-12-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 2,
      |      "tpch_1994-12-01T00:00:00.000Z_1995-01-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 0,
      |      "tpch_1995-02-01T00:00:00.000Z_1995-03-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 0,
      |      "tpch_1995-03-01T00:00:00.000Z_1995-04-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 1,
      |      "tpch_1995-04-01T00:00:00.000Z_1995-05-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 1,
      |      "tpch_1995-05-01T00:00:00.000Z_1995-06-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 1,
      |      "tpch_1995-06-01T00:00:00.000Z_1995-07-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 1,
      |      "tpch_1995-07-01T00:00:00.000Z_1995-08-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 0,
      |      "tpch_1995-08-01T00:00:00.000Z_1995-09-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 0,
      |      "tpch_1995-09-01T00:00:00.000Z_1995-10-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 1,
      |      "tpch_1995-10-01T00:00:00.000Z_1995-11-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 1,
      |      "tpch_1995-12-01T00:00:00.000Z_1996-01-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 0,
      |      "tpch_1996-01-01T00:00:00.000Z_1996-02-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 0,
      |      "tpch_1996-02-01T00:00:00.000Z_1996-03-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 1,
      |      "tpch_1996-03-01T00:00:00.000Z_1996-04-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 0,
      |      "tpch_1996-04-01T00:00:00.000Z_1996-05-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 0,
      |      "tpch_1996-06-01T00:00:00.000Z_1996-07-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 4,
      |      "tpch_1996-07-01T00:00:00.000Z_1996-08-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 2,
      |      "tpch_1996-08-01T00:00:00.000Z_1996-09-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 0,
      |      "tpch_1996-09-01T00:00:00.000Z_1996-10-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 1,
      |      "tpch_1996-10-01T00:00:00.000Z_1996-11-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 3,
      |      "tpch_1996-11-01T00:00:00.000Z_1996-12-01T00:00:00.000Z_2016-01-13T21:03:20.966Z" : 2
      |    },
      |    "events" : [ {
      |      "segmentId" : "tpch_1993-01-01T00:00:00.000Z_1993-02-01T00:00:00.000Z_2016-01-13T21:03:20.966Z",
      |      "offset" : 0,
      |      "event" : {
      |        "timestamp" : "1993-01-11T00:00:00.000Z",
      |        "p_mfgr" : "Manufacturer#5",
      |        "count" : 1
      |      }
      |    }, {
      |      "segmentId" : "tpch_1994-06-01T00:00:00.000Z_1994-07-01T00:00:00.000Z_2016-01-13T21:03:20.966Z",
      |      "offset" : 0,
      |      "event" : {
      |        "timestamp" : "1994-06-17T00:00:00.000Z",
      |        "p_mfgr" : "Manufacturer#5",
      |        "count" : 1
      |      }
      |    }, {
      |      "segmentId" : "tpch_1994-07-01T00:00:00.000Z_1994-08-01T00:00:00.000Z_2016-01-13T21:03:20.966Z",
      |      "offset" : 0,
      |      "event" : {
      |        "timestamp" : "1994-07-05T00:00:00.000Z",
      |        "p_mfgr" : "Manufacturer#3",
      |        "count" : 1
      |      }
      |    }, {
      |      "segmentId" : "tpch_1994-07-01T00:00:00.000Z_1994-08-01T00:00:00.000Z_2016-01-13T21:03:20.966Z",
      |      "offset" : 1,
      |      "event" : {
      |        "timestamp" : "1994-07-08T00:00:00.000Z",
      |        "p_mfgr" : "Manufacturer#3",
      |        "count" : 1
      |      }
      |    }, {
      |      "segmentId" : "tpch_1994-07-01T00:00:00.000Z_1994-08-01T00:00:00.000Z_2016-01-13T21:03:20.966Z",
      |      "offset" : 2,
      |      "event" : {
      |        "timestamp" : "1994-07-15T00:00:00.000Z",
      |        "p_mfgr" : "Manufacturer#5",
      |        "count" : 1
      |      }
      |    }]
      |  }
      |} ]
    """.stripMargin

  val is = new StringBufferInputStream(json1)

  import Utils._

  def selectRow(jp : JsonParser,
                ctxt: DeserializationContext,
                jValDeser : JsonDeserializer[JValue]) : SelectResultRow = {
    val o : JsonAST.JValue = jValDeser.deserialize(jp, ctxt)
    val r = o.extract[SelectResultRow]
    r
  }

  test("1") {

    val m = new OM(mapper)
    m.registerModule(new Json4sScalaModule)
    val jF = m.getFactory
    val jp = jF.createParser(is)
    val ctxt: DeserializationContext = m.createDeserializationContext(jp)
    val jValDeser = m.jValueResultDeserializer(ctxt)
    var t = jp.nextToken() // START_ARRAY

    // 1. SelResContainer
    t = jp.nextToken() // START_OBJECT

    // 1.1 timestamp
    t = jp.nextToken() // FIELD_NAME, jp.getCurrentName == timestamp
    t = jp.nextToken() // VALUE_STRING, set selResContainer.timestamp = jp.getText
    val ts = jp.getText

    t = jp.nextToken() // FIELD_NAME, jp.getCurrentName = "result"
    t= jp.nextToken() // START_OBJECT
    t = jp.nextToken() // FIELD_NAME , jp.getCurrentName == pagingIdentifiers

    t = jp.nextToken() // 1.2.1.v pIds value // START_OBJECT
    // t = jp.nextToken // FIELD_NAME
    val pagingIdentifiersJV : JsonAST.JValue = jValDeser.deserialize(jp, ctxt)
    val pagingIdentifiers = pagingIdentifiersJV.extract[ Map[String, Int]]

    // 1.2.2 events events field
    t = jp.nextToken // FIELD_NAME, jp.getCurrentName == events
    t = jp.nextToken // START_ARRAY
    t = jp.nextToken  // START_OBJECT
    // 1.2.2.v events value list

    while ( t != JsonToken.END_ARRAY ) {
      val o : JsonAST.JValue = jValDeser.deserialize(jp, ctxt)
      val r = o.extract[SelectResultRow]
      println(r)
      t = jp.nextToken()
    }

    t
  }

}
