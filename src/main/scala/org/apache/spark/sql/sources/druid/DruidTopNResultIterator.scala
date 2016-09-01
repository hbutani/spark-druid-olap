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

import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.DeserializationContext
import org.apache.commons.io.IOUtils
import org.apache.spark.util.NextIterator
import org.json4s._
import org.json4s.jackson.Json4sScalaModule
import org.sparklinedata.druid.client._
import org.sparklinedata.druid.{CloseableIterator, Utils}


private class DruidTopNResultIterator(val useSmile : Boolean,
                                       val is : InputStream,
                                       onDone : => Unit = ())
  extends NextIterator[TopNResultRow] with CloseableIterator[TopNResultRow] with JsonOperations {

  import Utils._
  import jsonMethods._

  val m = new OM(mapper)
  m.registerModule(new Json4sScalaModule)
  val jF = m.getFactory
  val jp = jF.createParser(is)
  val ctxt: DeserializationContext = m.createDeserializationContext(jp)
  val jValDeser = m.jValueResultDeserializer(ctxt)
  private var t : JsonToken = null

  jp.nextToken() // START_ARRAY
  jp.nextToken() // START_OBJ

  jp.nextToken() // timestamp field
  jp.nextToken() // timstamp value
  // jp.getText

  jp.nextToken() // result field
  jp.nextToken() // START_ARRAY for results
  t = jp.nextToken() // START_OBJ

  override protected def getNext(): TopNResultRow = {
    if ( t == JsonToken.END_ARRAY ) {
      finished = true
      null
    } else {
      val o : JsonAST.JValue = jValDeser.deserialize(jp, ctxt)
      val r = o.extract[TopNResultRow]
      t = jp.nextToken()
      r
    }
  }

  override protected def close(): Unit = {
    onDone
  }
}

private class DruidTopNResultIterator2(val useSmile : Boolean,
                                        val is : InputStream,
                                        onDone : => Unit = ())
  extends NextIterator[TopNResultRow] with CloseableIterator[TopNResultRow] with JsonOperations {

  import Utils._
  import jsonMethods._

  val s : JsonInput = if (useSmile) {
    new ByteArrayInputStream(IOUtils.toByteArray(is))
  } else {
    IOUtils.toString(is)
  }

  onDone

  val jV = parse(s)
  val result = jV.extract[TopNResult]
  val it = result.result.iterator

  override protected def getNext(): TopNResultRow = {
    if (it.hasNext) {
      it.next
    } else {
      finished = true
      null
    }
  }

  override protected def close(): Unit = ()
}

object DruidTopNResultIterator {
  def apply(useSmile : Boolean,
            is : InputStream,
            onDone : => Unit = (),
            fromList : Boolean = false) : CloseableIterator[TopNResultRow] =
    if (!fromList) {
      new DruidTopNResultIterator(useSmile, is, onDone)
    }
    else {
      new DruidTopNResultIterator2(useSmile, is, onDone)
    }

}