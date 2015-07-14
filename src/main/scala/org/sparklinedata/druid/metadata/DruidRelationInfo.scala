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
                         val cardinalityPerDruidQuery : Long) {

  def sourceDF(sqlContext : SQLContext) = sqlContext.table(sourceDFName)

}

object DruidRelationInfo {

  def apply(sourceDFName : String,
             sourceDF : DataFrame,
           dsName : String,
            timeDimensionCol : String,
             druidHost : String,
             druidPort : Int,
             columnMapping : Map[String, String],
             functionalDeps : List[FunctionalDependency],
            maxCardinality : Long,
            cardinalityPerDruidQuery : Long) : DruidRelationInfo = {

    val client = new DruidClient(druidHost, druidPort)
    val druidDS = client.metadata(dsName)
    val sourceToDruidMapping = MappingBuilder.buildMapping(columnMapping, sourceDF, druidDS)
    val fd = new FunctionalDependencies(druidDS, functionalDeps)

    DruidRelationInfo(DruidClientInfo(druidHost, druidPort),
    sourceDFName,
    timeDimensionCol,
    druidDS,
    sourceToDruidMapping,
    fd,
    maxCardinality,
    cardinalityPerDruidQuery)
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
                            druidDS : DruidDataSource) : Map[String, DruidColumn] = {

    val m = MMap[String, DruidColumn]()

    sourceDF.schema.iterator.foreach { f =>
      if ( supportedDataType(f.dataType)) {
        val dCol = druidDS.columns.get(nameMapping.getOrElse(f.name, f.name))
        if ( dCol.isDefined) {
          m += (f.name -> dCol.get)
        }

      } else {
        logDebug(s"${f.name} not mapped to Druid dataSource, unsupported dataType")
      }
    }

    m.toMap
  }
}
