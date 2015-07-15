package org.sparklinedata.druid.metadata

import org.joda.time.Interval
import org.sparklinedata.druid.client.{ColumnDetails, MetadataResponse}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, DataType}

object DruidDataType extends Enumeration {
  val String = Value("STRING")
  val Long = Value("LONG")
  val Float = Value("FLOAT")
  val HyperUnique = Value("HYPERUNIQUE")

  def sparkDataType(t : String) : DataType = sparkDataType(DruidDataType.withName(t))

  def sparkDataType(t : DruidDataType.Value) : DataType = t match {
    case String => StringType
    case Long => LongType
    case Float => DoubleType
    case HyperUnique => DoubleType
  }
}

sealed trait DruidColumn {
  val name : String
  val dataType : DruidDataType.Value
  val size : Long // in bytes
}

object DruidColumn {

  def apply(nm : String, c : ColumnDetails) : DruidColumn = {
    if (nm == DruidDataSource.TIME_COLUMN_NAME) {
      DruidTimeDimension(nm, DruidDataType.withName(c.typ), c.size)
    } else if ( c.cardinality.isDefined) {
      DruidDimension(nm, DruidDataType.withName(c.typ), c.size, c.cardinality.get)
    } else {
      DruidMetric(nm, DruidDataType.withName(c.typ), c.size)
    }
  }
}

case class DruidDimension(name : String,
                       dataType : DruidDataType.Value,
                       size : Long,
                       cardinality : Long) extends DruidColumn

case class DruidMetric(name : String,
                          dataType : DruidDataType.Value,
                          size : Long) extends DruidColumn

case class DruidTimeDimension(name : String,
                       dataType : DruidDataType.Value,
                       size : Long) extends DruidColumn

case class DruidDataSource(name : String,
                         intervals : List[Interval],
                         columns : Map[String, DruidColumn],
                         size : Long) {

  import DruidDataSource._

  lazy val timeDimension = columns.values.find {
    case c if c.name == TIME_COLUMN_NAME => true
  }

  lazy val dimensions : IndexedSeq[DruidDimension] = columns.values.filter {
    case d : DruidDimension => true
    case _ => false
  }.map(_.asInstanceOf[DruidDimension]).toIndexedSeq

  lazy val metrics = columns.values.filter {
    case m : DruidMetric => true
    case _ => false
  }

  def numDimensions = dimensions.size

  def indexOfDimension(d : String) : Int = {
    dimensions.indexWhere(_.name == d)
  }

}

object DruidDataSource {

  val TIME_COLUMN_NAME = "__time"

  def apply(dataSource : String, mr : MetadataResponse) : DruidDataSource = {
    val is = mr.intervals.map(Interval.parse(_))
    val columns = mr.columns.map {
      case (n, c) => (n -> DruidColumn(n,c))
    }
    new DruidDataSource(dataSource, is, columns, mr.size)
  }
}
