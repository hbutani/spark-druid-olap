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

package org.sparklinedata.druid.metadata

import org.joda.time.Interval
import org.sparklinedata.druid.client.{ColumnDetails, MetadataResponse}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, DataType}

object DruidDataType extends Enumeration {
  val String = Value("STRING")
  val Long = Value("LONG")
  val Float = Value("FLOAT")
  val HyperUnique = Value("hyperUnique")

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

  def isDimension(excludeTime : Boolean = false) : Boolean
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

trait DruidDimensionInfo {
  def name : String
  def dataType : DruidDataType.Value
  def size : Long
  def cardinality : Long
}

case class DruidDimension(name : String,
                       dataType : DruidDataType.Value,
                       size : Long,
                       cardinality : Long) extends DruidColumn with DruidDimensionInfo {
  def isDimension(excludeTime : Boolean = false) = true
}

case class DruidMetric(name : String,
                          dataType : DruidDataType.Value,
                          size : Long) extends DruidColumn {
  def isDimension(excludeTime : Boolean = false) = false
}

case class DruidTimeDimension(name : String,
                       dataType : DruidDataType.Value,
                       size : Long) extends DruidColumn with DruidDimensionInfo {
  def isDimension(excludeTime : Boolean = false) = !excludeTime

  /**
    * assume the worst, this is only used during query costing
    */
  val cardinality = Int.MaxValue.toLong
}

trait DruidDataSourceCapability {
  def druidVersion : String
  def supportsBoundFilter : Boolean = druidVersion.compareTo("0.9.0") >= 0
}

case class DruidDataSource(name : String,
                         intervals : List[Interval],
                         columns : Map[String, DruidColumn],
                         size : Long,
                           druidVersion : String = null) extends DruidDataSourceCapability {

  import DruidDataSource._

  lazy val timeDimension = columns.values.find {
    case c if c.name == TIME_COLUMN_NAME => true
    case _ => false
  }

  lazy val dimensions : IndexedSeq[DruidDimension] = columns.values.filter {
    case d : DruidDimension => true
    case _ => false
  }.map(_.asInstanceOf[DruidDimension]).toIndexedSeq

  lazy val metrics : Map[String, DruidMetric] = columns.values.filter {
    case m : DruidMetric => true
    case _ => false
  }.map(m => (m.name -> m.asInstanceOf[DruidMetric])).toMap

  def numDimensions = dimensions.size

  def indexOfDimension(d : String) : Int = {
    dimensions.indexWhere(_.name == d)
  }

  def metric(name : String) : Option[DruidMetric] = {
    metrics.get(name)
  }

}

object DruidDataSource {

  val TIME_COLUMN_NAME = "__time"

  def apply(dataSource : String, mr : MetadataResponse,
             is : List[Interval]) : DruidDataSource = {
    val columns = mr.columns.map {
      case (n, c) => (n -> DruidColumn(n,c))
    }
    new DruidDataSource(dataSource, is, columns, mr.size)
  }
}
