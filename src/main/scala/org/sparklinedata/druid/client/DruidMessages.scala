package org.sparklinedata.druid.client

case class ColumnDetails(typ : String, size : Long,
                         cardinality : Option[Long], errorMessage : Option[String])
case class MetadataResponse(id : String,
                            intervals : List[String],
                             columns : Map[String, ColumnDetails],
                             size : Long)
