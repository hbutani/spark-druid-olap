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

import org.joda.time.Interval
import org.json4s.CustomSerializer
import org.json4s.JsonAST._

case class ColumnDetails(typ : String, size : Long,
                         cardinality : Option[Long], errorMessage : Option[String])
case class MetadataResponse(id : String,
                            intervals : List[String],
                             columns : Map[String, ColumnDetails],
                             size : Long)

case class SegmentInfo(id : String,
                       intervals : Interval,
                       size : Long
                      ) {
  def this(mr : MetadataResponse) =
    this(mr.id,  Interval.parse(mr.intervals(0)), mr.size  )
}

case class QueryResultRow(version : String,
                           timestamp : String,
                           event : Map[String, Any])

class QueryResultRowSerializer extends CustomSerializer[QueryResultRow](format => (
  {
    case JObject(
    JField("version", JString(v)) ::
      JField("timestamp", JString(t)) ::
      JField("event", JObject(obj)) :: Nil
    ) =>
      val m : Map[String, Any] = obj.map(t => (t._1, t._2.values)).toMap
      QueryResultRow(v, t, m)
  },
  {
    case x: QueryResultRow =>
      throw new RuntimeException("QueryRow serialization not supported.")
  }
  ))
