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
import com.fasterxml.jackson.databind.deser.DefaultDeserializationContext
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, ObjectMapper}
import org.apache.commons.io.IOUtils
import org.apache.spark.util.NextIterator
import org.json4s.{JsonAST, JsonInput}
import org.json4s.jackson.Json4sScalaModule
import org.sparklinedata.druid.Utils
import org.sparklinedata.druid.client.QueryResultRow
import org.sparklinedata.druid.CloseableIterator

private[druid] class OM(mapper : ObjectMapper) extends ObjectMapper(mapper) {

  type JValue   = JsonAST.JValue

  def createDeserializationContext (jp : JsonParser) : DefaultDeserializationContext = {
    return _deserializationContext.createInstance(getDeserializationConfig, jp, _injectableValues)
  }

  def jValueResultDeserializer(ctxt: DeserializationContext ) : JsonDeserializer[JValue] = {
    _findRootDeserializer(ctxt, constructType(classOf[JValue])
    ).asInstanceOf[JsonDeserializer[JValue]]
  }

}

trait JsonOperations {
  def useSmile : Boolean
  val jsonMethods = if (useSmile) {
    org.json4s.jackson.sparklinedata.SmileJsonMethods
  } else {
    org.json4s.jackson.JsonMethods
  }

}

private class DruidQueryResultIterator(val useSmile : Boolean,
                                       val is : InputStream,
                                       onDone : => Unit = ())
  extends NextIterator[QueryResultRow] with CloseableIterator[QueryResultRow] with JsonOperations {

  import jsonMethods._
  import Utils._

  val m = new OM(mapper)
  m.registerModule(new Json4sScalaModule)
  val jF = m.getFactory
  val jp = jF.createParser(is)
  val ctxt: DeserializationContext = m.createDeserializationContext(jp)
  val jValDeser = m.jValueResultDeserializer(ctxt)
  private var t = jp.nextToken()
  t = jp.nextToken()

  override protected def getNext(): QueryResultRow = {
    if ( t == JsonToken.END_ARRAY ) {
      finished = true
      null
    } else {
      val o : JsonAST.JValue = jValDeser.deserialize(jp, ctxt)
      val r = o.extract[QueryResultRow]
      t = jp.nextToken()
      r
    }
  }

  override protected def close(): Unit = {
    onDone
  }
}

private class DruidQueryResultIterator2(val useSmile : Boolean,
                                         val is : InputStream,
                                        onDone : => Unit = ())
  extends NextIterator[QueryResultRow] with CloseableIterator[QueryResultRow] with JsonOperations {

  import jsonMethods._
  import Utils._

  val s : JsonInput = if (useSmile) {
    new ByteArrayInputStream(IOUtils.toByteArray(is))
  } else {
    IOUtils.toString(is)
  }

  onDone

  val jV = parse(s)
  val it = jV.extract[List[QueryResultRow]].iterator

  override protected def getNext(): QueryResultRow = {
    if (it.hasNext) {
      it.next
    } else {
      finished = true
      null
    }
  }

  override protected def close(): Unit = ()
}

case class SearchQueryResult(
                            timestamp : String,
                            result : List[QueryResultRow]
                            )

class SearchQueryResultIterator(val useSmile : Boolean,
                                val is : InputStream,
                                onDone : => Unit = ())
  extends NextIterator[QueryResultRow] with CloseableIterator[QueryResultRow] with JsonOperations {

  import jsonMethods._
  import Utils._

  val s : JsonInput = if (useSmile) {
    new ByteArrayInputStream(IOUtils.toByteArray(is))
  } else {
    IOUtils.toString(is)
  }

  onDone

  val jV = parse(s)
  val searchResult = jV.extract[SearchQueryResult]
  val it = searchResult.result.iterator

  override protected def getNext(): QueryResultRow = {
    if (it.hasNext) {
      it.next.copy(timestamp = searchResult.timestamp)
    } else {
      finished = true
      null
    }
  }

  override protected def close(): Unit = ()
}

object DruidQueryResultIterator {
  def apply(useSmile : Boolean,
            is : InputStream,
            onDone : => Unit = (),
            fromList : Boolean = false) : CloseableIterator[QueryResultRow] =
    if (!fromList) {
      new DruidQueryResultIterator(useSmile, is, onDone)
    }
    else {
      new DruidQueryResultIterator2(useSmile, is, onDone)
    }

}
