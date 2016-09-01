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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.smile.{SmileFactory, SmileGenerator}
import org.json4s.jackson.{Json4sScalaModule, JsonMethods}

object SmileJsonMethods extends JsonMethods {

  private[this] lazy val _defaultMapper = {

    val smileFactory = new SmileFactory
    smileFactory.configure(SmileGenerator.Feature.ENCODE_BINARY_AS_7BIT, false)
    smileFactory.delegateToTextual(true)
    val m = new ObjectMapper(smileFactory)
    m.getFactory().setCodec(m)
    m.registerModule(SmileJson4sScalaModule)
    m
  }

  override def mapper = _defaultMapper

}
