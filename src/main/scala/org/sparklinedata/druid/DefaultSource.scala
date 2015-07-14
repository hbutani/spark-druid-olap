package org.sparklinedata.druid

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}
import org.json4s._
import org.json4s.ext.EnumSerializer
import org.json4s.jackson.JsonMethods._
import org.sparklinedata.druid.metadata.{DruidRelationInfo, FunctionalDependencyType, FunctionalDependency}

class DefaultSource extends RelationProvider {

  import DefaultSource._

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {

    implicit val formats = DefaultFormats + new EnumSerializer(FunctionalDependencyType)

    val sourceDFName = parameters.getOrElse(SOURCE_DF_PARAM,
      throw new DruidDataSourceException(
        s"'$SOURCE_DF_PARAM' must be specified for Druid DataSource")
    )

    val sourceDF = sqlContext.table(sourceDFName)

    val dsName : String = parameters.getOrElse(DRUID_DS_PARAM,
      throw new DruidDataSourceException(
        s"'$DRUID_DS_PARAM' must be specified for Druid DataSource")
    )

    val maxCardinality =
      parameters.getOrElse(MAX_CARDINALITY_PARAM, DEFAULT_MAX_CARDINALITY).toLong
    val cardinalityPerDruidQuery =
      parameters.getOrElse(MAX_CARDINALITY_PER_DRUID_QUERY_PARAM,
        DEFAULT_CARDINALITY_PER_DRUID_QUERY).toLong

    val columnMapping : Map[String, String] =
      parameters.get(SOURCE_TO_DRUID_NAME_MAP_PARAM).map { s =>
      val o = parse(s).asInstanceOf[JObject]
      o.obj.map(t => (t._1, t._2.values.toString)).toMap
    }.getOrElse(Map())

    val fds : List[FunctionalDependency] = parameters.get(FUNCTIONAL_DEPENDENCIES_PARAM).map { s=>
      parse(s).extract[List[FunctionalDependency]]
    }.getOrElse(List())

    val druidHost = parameters.get(DRUID_HOST_PARAM).getOrElse(DEFAULT_DRUID_HOST)
    val druidPort : Int = parameters.get(DRUID_PORT_PARAM).getOrElse(DEFAULT_DRUID_PORT).toInt

    val drI = DruidRelationInfo(sourceDFName, sourceDF,
    dsName,
    druidHost,
    druidPort,
    columnMapping,
    fds,
    maxCardinality,
    cardinalityPerDruidQuery)

  ???
  }
}

object DefaultSource {

  val SOURCE_DF_PARAM = "spark.druid.source.dataframe"

  /**
   * DataSource name in Druid.
   */
  val DRUID_DS_PARAM = "spark.druid.datasource"
  
  /**
   * If the result cardinality of a Query exceeeds this value then Query is not
   * converted to a Druid Query.
   */
  val MAX_CARDINALITY_PARAM = "spark.druid.max.result.cardinality"
  val DEFAULT_MAX_CARDINALITY : String = (1 * 1000 * 1000).toString

  /**
   * If the result size estimate exceeds this number, and attempt is made to run 'n'
   * druid queries, each of which spans a sub interval of the total time interval.
   * 'n' is computed as `result.size % thisParam + 1`
   */
  val MAX_CARDINALITY_PER_DRUID_QUERY_PARAM = "spark.druid.max.cardinality.per.query"
  val DEFAULT_CARDINALITY_PER_DRUID_QUERY = (100 * 1000).toString

  /**
   * Map column names to Druid field names.
   * Specified as a json string.
   */
  val SOURCE_TO_DRUID_NAME_MAP_PARAM = "spark.druid.column.mapping"

  /**
   * Specify how columns are related, see
   * [[org.sparklinedata.druid.metadata.FunctionalDependency]]. Specified as a list of
   * functional dependency objects.
   */
  val FUNCTIONAL_DEPENDENCIES_PARAM = "spark.druid.functional.dependencies"

  val DRUID_HOST_PARAM = "spark.druid.host"
  val DEFAULT_DRUID_HOST = "localhost"

  val DRUID_PORT_PARAM = "spark.druid.port"
  val DEFAULT_DRUID_PORT = "8082"  // the broker port

}
