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

package org.apache.spark.sql.sources.druid

import java.io.{ByteArrayInputStream, InputStream}

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}
import org.apache.commons.io.IOUtils
import org.apache.spark.util.NextIterator
import org.json4s._
import org.json4s.jackson.Json4sScalaModule
import org.json4s.jackson.JsonMethods._
import org.sparklinedata.druid.client._
import org.sparklinedata.druid.{CloseableIterator, SelectSpec, Utils}


private class DruidSelectResultIterator(val useSmile : Boolean,
                                        is: InputStream,
                                        val selectSpec: SelectSpec,
                                        val druidQuerySvrConn: DruidQueryServerClient,
                                        initialOnDone: () => Unit)
  extends NextIterator[SelectResultRow] with CloseableIterator[SelectResultRow]
    with JsonOperations {

  import jsonMethods._
  import Utils._

  var m = new OM(mapper)
  m.registerModule(new Json4sScalaModule)
  private var jF = m.getFactory

  private var onDone = initialOnDone
  private var thisRoundHadData: Boolean = false
  private var jp: JsonParser = _
  private var ctxt: DeserializationContext = _
  private var jValDeser: JsonDeserializer[JValue] = _
  private var t: JsonToken = _
  private var currSelectResultContainerTS: String = _
  private var nextPagingIdentifiers: Map[String, Int] = _

  consumeNextStream(is)

  private def transferState(nextIt : DruidSelectResultIterator) : Unit = {
    m = nextIt.m
    jF = nextIt.jF
    onDone = nextIt.onDone
    thisRoundHadData = nextIt.thisRoundHadData
    jp = nextIt.jp
    ctxt = nextIt.ctxt
    jValDeser = nextIt.jValDeser
    t = nextIt.t
    currSelectResultContainerTS = nextIt.currSelectResultContainerTS
    nextPagingIdentifiers = nextIt.nextPagingIdentifiers
  }

  def consumeNextStream(is: InputStream): Unit = {

    jp = jF.createParser(is)
    ctxt = m.createDeserializationContext(jp)
    jValDeser = m.jValueResultDeserializer(ctxt)

    jp.nextToken() // START_ARRAY

    // 1. SelResContainer
    t = jp.nextToken() // START_OBJECT

    // 1.1 timestamp
    t = jp.nextToken() // FIELD_NAME, jp.getCurrentName == timestamp
    t = jp.nextToken() // VALUE_STRING, set selResContainer.timestamp = jp.getText
    currSelectResultContainerTS = jp.getText

    t = jp.nextToken() // FIELD_NAME, jp.getCurrentName = "result"
    t = jp.nextToken() // START_OBJECT
    t = jp.nextToken() // FIELD_NAME , jp.getCurrentName == pagingIdentifiers

    t = jp.nextToken() // 1.2.1.v pIds value // START_OBJECT
    // t = jp.nextToken // FIELD_NAME
    val pagingIdentifiersJV: JsonAST.JValue = jValDeser.deserialize(jp, ctxt)
    nextPagingIdentifiers = pagingIdentifiersJV.extract[Map[String, Int]]

//    nextPagingIdentifiers = nextPagingIdentifiers.map {
//      case (s,i) if !selectSpec.descending => (s, i + 1)
//      case (s,i) if selectSpec.descending => (s, i - 1)
//    }


    // 1.2.2 events events field
    t = jp.nextToken // FIELD_NAME, jp.getCurrentName == events
    t = jp.nextToken // START_ARRAY
    t = jp.nextToken // START_OBJECT
  }

  override protected def getNext(): SelectResultRow = {
    if (t == JsonToken.END_ARRAY) {
      onDone()
      if (!thisRoundHadData) {
        finished = true
        null
      } else {
        thisRoundHadData = false
        val nextSelectSpec = selectSpec.withPagingIdentifier(nextPagingIdentifiers)
        transferState(
          nextSelectSpec.executeQuery(druidQuerySvrConn).asInstanceOf[DruidSelectResultIterator]
        )
        getNext
      }
    } else {
      val o: JsonAST.JValue = jValDeser.deserialize(jp, ctxt)
      val r = o.extract[SelectResultRow]
      t = jp.nextToken()
      thisRoundHadData = true
      r
    }
  }

  override protected def close(): Unit = {
    onDone()
  }
}

private class DruidSelectResultIterator2(val useSmile : Boolean,
                                         is: InputStream,
                                         val selectSpec: SelectSpec,
                                         val druidQuerySvrConn: DruidQueryServerClient,
                                         initialOnDone: () => Unit)
  extends NextIterator[SelectResultRow] with CloseableIterator[SelectResultRow]
    with JsonOperations {

  import jsonMethods._
  import Utils._

  protected var thisRoundHadData: Boolean = false
  consumeNextStream(is)

  var onDone = initialOnDone
  var currResult: SelectResult = _
  var currIt: Iterator[SelectResultRow] = _

  private def transferState(nextIt : DruidSelectResultIterator2) : Unit = {
    thisRoundHadData = nextIt.thisRoundHadData
    onDone = nextIt.onDone
    currResult = nextIt.currResult
    currIt = nextIt.currIt
  }

  private def consumeNextStream(is: InputStream): Unit = {
    val s : JsonInput = if (useSmile) {
      new ByteArrayInputStream(IOUtils.toByteArray(is))
    } else {
      IOUtils.toString(is)
    }
    val jV = parse(s)
    currResult = jV.extract[SelectResultContainer].result
    currIt = currResult.events.iterator
  }

  override protected def getNext(): SelectResultRow = {
    if (currIt.hasNext) {
      thisRoundHadData = true
      currIt.next
    } else {
      onDone
      if (!thisRoundHadData) {
        finished = true
        null
      } else {
        thisRoundHadData = false
        val nextSelectSpec = selectSpec.withPagingIdentifier(currResult.pagingIdentifiers)
        transferState(
          nextSelectSpec.executeQuery(druidQuerySvrConn).asInstanceOf[DruidSelectResultIterator2]
        )
        getNext
      }
    }
  }

  override protected def close(): Unit = onDone
}

object DruidSelectResultIterator {
  def apply(useSmile : Boolean,
            is: InputStream,
            selectSpec: SelectSpec,
            druidQuerySvrConn: DruidQueryServerClient,
            onDone: => Unit,
            fromList: Boolean = false): CloseableIterator[SelectResultRow] =
    if (!fromList) {
      new DruidSelectResultIterator(useSmile, is, selectSpec, druidQuerySvrConn, {() => onDone})
    }
    else {
      new DruidSelectResultIterator2(useSmile, is, selectSpec, druidQuerySvrConn, {() => onDone})
    }
}

