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

package org.json4s.jackson.sparklinedata

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.Module.SetupContext
import com.fasterxml.jackson.databind.{BeanDescription, JavaType, SerializationConfig}
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.ser.Serializers
import org.json4s.jackson.{JValueDeserializerResolver, Json4sScalaModule}
import org.json4s._

/**
  * Why all this rewiring?
  * - core issue is that [[org.json4s.jackson.JValueSerializer]] always serializes an JInt as
  *   a BigInt. This is fine for the default ObjectMapper, but in the case of Smile the dataType
  *   of a value is added to the serialized form. On Druid's side certain
  *   values are checked for the correct dataType(like queryContext.minTopNThreshold
  *   must be an int).
  * - to fix this we introduce an overridden ''JValueSerializer'' that checks the value of a
  *  JInt is an integer and serializes it as an Int.
  * - to introduce the new ''Serializer'' we introduce a new
  *   [[com.fasterxml.jackson.databind.Module]], and ''SerializerResolver''
  *
 */

class SmileJson4sScalaModule extends Json4sScalaModule {

  override def setupModule(ctxt: SetupContext) {
    ctxt.addSerializers(JValueSerializerResolver)
    ctxt.addDeserializers(JValueDeserializerResolver)
  }
}

object SmileJson4sScalaModule extends SmileJson4sScalaModule {

}

private object JValueSerializerResolver extends Serializers.Base {
  private val JVALUE = classOf[JValue]
  override def findSerializer(config: SerializationConfig,
                              theType: JavaType, beanDesc: BeanDescription) = {
    if (!JVALUE.isAssignableFrom(theType.getRawClass)) null
    else new JValueSerializer
  }

}

class JValueSerializer extends
 org.json4s.jackson.JValueSerializer {
  import JValueSerializer.isInt

  override def serialize(value: JValue, json: JsonGenerator, provider: SerializerProvider) {
    value match {
      case JInt(v) if isInt(v) => json.writeNumber(v.intValue)
      case _ => super.serialize(value, json, provider)
    }
  }
}

object JValueSerializer {
  val intMax : BigInt = Int.MaxValue
  val intMin : BigInt = Int.MinValue

  def isInt(i : BigInt) : Boolean =
    i != null && !( i.compare(intMax) > 0 || i.compare(intMin) < 0)

}
