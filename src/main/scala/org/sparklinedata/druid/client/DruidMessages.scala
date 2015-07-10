package org.sparklinedata.druid.client

import org.json4s.CustomSerializer
import org.json4s.JsonAST._

case class ColumnDetails(typ : String, size : Long,
                         cardinality : Option[Long], errorMessage : Option[String])
case class MetadataResponse(id : String,
                            intervals : List[String],
                             columns : Map[String, ColumnDetails],
                             size : Long)

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