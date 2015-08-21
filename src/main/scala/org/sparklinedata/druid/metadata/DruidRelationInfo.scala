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

import org.apache.spark.Logging
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.types._
import org.sparklinedata.druid.client.DruidClient

import scala.collection.mutable.{Map => MMap}

case class DruidClientInfo(host : String, port : Int)

case class DruidRelationInfo(val druidClientInfo : DruidClientInfo,
                         val sourceDFName : String,
                            val timeDimensionCol : String,
                         val druidDS : DruidDataSource,
                         val sourceToDruidMapping : Map[String, DruidColumn],
                         val fd : FunctionalDependencies,
                         val maxCardinality : Long,
                         val cardinalityPerDruidQuery : Long,
                              val allowCountDistinct : Boolean) {

  def sourceDF(sqlContext : SQLContext) = sqlContext.table(sourceDFName)

}

object DruidRelationInfo {

  // scalastyle:off parameter.number
  def apply(sourceDFName : String,
             sourceDF : DataFrame,
           dsName : String,
            timeDimensionCol : String,
             druidHost : String,
             druidPort : Int,
             columnMapping : Map[String, String],
             functionalDeps : List[FunctionalDependency],
            maxCardinality : Long,
            cardinalityPerDruidQuery : Long,
             allowCountDistinct : Boolean = true) : DruidRelationInfo = {

    val client = new DruidClient(druidHost, druidPort)
    val druidDS = client.metadata(dsName)
    val sourceToDruidMapping =
      MappingBuilder.buildMapping(columnMapping, sourceDF, timeDimensionCol, druidDS)
    val fd = new FunctionalDependencies(druidDS, functionalDeps,
      DependencyGraph(druidDS, functionalDeps))

    DruidRelationInfo(DruidClientInfo(druidHost, druidPort),
    sourceDFName,
    timeDimensionCol,
    druidDS,
    sourceToDruidMapping,
    fd,
    maxCardinality,
    cardinalityPerDruidQuery,
    allowCountDistinct)
  }

}

private object MappingBuilder extends Logging {

  /**
   * Only top level Numeric and String Types are mapped.
   * @param dT
   * @return
   */
  def supportedDataType(dT : DataType) : Boolean = dT match {
    case t if t.isInstanceOf[NumericType] => true
    case StringType => true
    case _ => false
  }

  def buildMapping( nameMapping : Map[String, String],
                            sourceDF : DataFrame,
                    timeDimensionCol : String,
                            druidDS : DruidDataSource) : Map[String, DruidColumn] = {

    val m = MMap[String, DruidColumn]()

    sourceDF.schema.iterator.foreach { f =>
      if ( supportedDataType(f.dataType)) {
        val dCol = druidDS.columns.get(nameMapping.getOrElse(f.name, f.name))
        if ( dCol.isDefined) {
          m += (f.name -> dCol.get)
        } else if (f.name == timeDimensionCol) {
          m += (f.name -> druidDS.timeDimension.get)
        }

      } else {
        logDebug(s"${f.name} not mapped to Druid dataSource, unsupported dataType")
      }
    }

    m.toMap
  }
}
