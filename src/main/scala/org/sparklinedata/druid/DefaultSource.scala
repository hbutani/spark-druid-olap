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

package org.sparklinedata.druid

import org.apache.spark.sql.SPLLogging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.sparklinedata.SPLSessionState
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.sparklinedata.druid.metadata._

class DefaultSource extends RelationProvider with SPLLogging {

  import DefaultSource._

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {

    import Utils.jsonFormat

    var sourceDFName = parameters.getOrElse(SOURCE_DF_PARAM,
      throw new DruidDataSourceException(
        s"'$SOURCE_DF_PARAM' must be specified for Druid DataSource")
    )

    sourceDFName = SPLSessionState.qualifiedName(sqlContext, sourceDFName)

    val sourceDF = sqlContext.table(sourceDFName)

    val dsName: String = parameters.getOrElse(DRUID_DS_PARAM,
      throw new DruidDataSourceException(
        s"'$DRUID_DS_PARAM' must be specified for Druid DataSource")
    )

    val timeDimensionCol: String = parameters.getOrElse(TIME_DIMENSION_COLUMN_PARAM,
      throw new DruidDataSourceException(
        s"'$TIME_DIMENSION_COLUMN_PARAM' must be specified for Druid DataSource")
    )

    // validate field is in SourceDF.
    sourceDF.schema(timeDimensionCol)

    val maxCardinality =
      parameters.getOrElse(MAX_CARDINALITY_PARAM, DEFAULT_MAX_CARDINALITY).toLong
    val cardinalityPerDruidQuery =
      parameters.getOrElse(MAX_CARDINALITY_PER_DRUID_QUERY_PARAM,
        DEFAULT_CARDINALITY_PER_DRUID_QUERY).toLong

    val columnMapping: Map[String, String] =
      parameters.get(SOURCE_TO_DRUID_NAME_MAP_PARAM).map { s =>
        val o = parse(s).asInstanceOf[JObject]
        o.obj.map(t => (t._1, t._2.values.toString)).toMap
      }.getOrElse(Map())

    val columnInfos: List[DruidRelationColumnInfo] =
      parameters.get(SOURCE_TO_DRUID_INFO).map { s =>
        parse(s).extract[List[DruidRelationColumnInfo]]
      }.getOrElse(List())

    val fds: List[FunctionalDependency] = parameters.get(FUNCTIONAL_DEPENDENCIES_PARAM).map { s =>
      parse(s).extract[List[FunctionalDependency]]
    }.getOrElse(List())

    val druidHost = parameters.get(DRUID_HOST_PARAM).getOrElse(DEFAULT_DRUID_HOST)

    var starSchemaInfo =
      parameters.get(STAR_SCHEMA_INFO_PARAM).map(parse(_).extract[StarSchemaInfo]).
        getOrElse(StarSchemaInfo(sourceDFName))

    starSchemaInfo = StarSchemaInfo.qualifyTableNames(sqlContext, starSchemaInfo)

    val ss = StarSchema(sourceDFName, starSchemaInfo)(sqlContext)
    if (ss.isLeft) {
      throw new DruidDataSourceException(
        s"Failed to parse StarSchemaInfo: ${ss.left.get}")
    }

    val pushHLLTODruid =
      parameters.get(PUSH_HYPERLOGLOG_TODRUID).
        getOrElse(DEFAULT_PUSH_HYPERLOGLOG_TODRUID).toBoolean

    val streamDruidQueryResults =
      parameters.get(STREAM_DRUID_QUERY_RESULTS).
        getOrElse(DEFAULT_STREAM_DRUID_QUERY_RESULTS).toBoolean

    val loadMetadataFromAllSegments =
      parameters.get(LOAD_METADATA_FROM_ALL_SEGMENTS).
        getOrElse(DEFAULT_LOAD_METADATA_FROM_ALL_SEGMENTS).toBoolean

    val zkSessionTimeoutMs : Int =
      parameters.get(ZK_SESSION_TIMEOUT).
        getOrElse(DEFAULT_ZK_SESSION_TIMEOUT).toInt

    val zkEnableCompression : Boolean =
      parameters.get(ZK_ENABLE_COMPRESSION).
        getOrElse(DEFAULT_ZK_ENABLE_COMPRESSION).toBoolean

    val zkDruidPath : String =
      parameters.get(ZK_DRUID_PATH).
        getOrElse(DEFAULT_ZK_DRUID_PATH)

    val queryHistorical : Boolean =
      parameters.get(QUERY_HISTORICAL).
        getOrElse(DEFAULT_QUERY_HISTORICAL).toBoolean

    val zkQualifyDiscoveryNames : Boolean =
      parameters.get(ZK_QUALIFY_DISCOVERY_NAMES).
        getOrElse(DEFAULT_ZK_QUALIFY_DISCOVERY_NAMES).toBoolean

    val numSegmentsPerHistoricalQuery : Int =
      parameters.get(NUM_SEGMENTS_PER_HISTORICAL_QUERY).
        getOrElse(DEFAULT_NUM_SEGMENTS_PER_HISTORICAL_QUERY).toInt

    val useSmile : Boolean =
      parameters.get(USE_SMILE).
        getOrElse(DEFAULT_USE_SMILE).toBoolean

    val allowTopN : Boolean =
      parameters.get(ALLOW_TOPN).
        getOrElse(DEFAULT_ALLOW_TOPN.toString).toBoolean

    val topNMaxThreshold : Int =
      parameters.get(TOPN_MAX_THRESHOLD).
        getOrElse(DEFAULT_TOPN_MAX_THRESHOLD.toString).toInt


    val numProcessingThreadsPerHistorical =
      parameters.get(NUM_PROCESSING_THREADS_PER_HISTORICAL).map(_.toInt)

    val nonAggregateQueryHandling:
    NonAggregateQueryHandling.Value = NonAggregateQueryHandling.withName(
      parameters.get(NON_AGG_QUERY_HANDLING).
        getOrElse(DEFAULT_NON_AGG_QUERY_HANDLING)
    )

    val queryGranularity : DruidQueryGranularity =
      DruidQueryGranularity(parameters.get(QUERY_GRANULARITY).getOrElse(DEFAULT_QUERY_GRANULARITY))

    val options = DruidRelationOptions(
      maxCardinality,
      cardinalityPerDruidQuery,
      pushHLLTODruid,
      streamDruidQueryResults,
      loadMetadataFromAllSegments,
      zkSessionTimeoutMs,
      zkEnableCompression,
      zkDruidPath,
      queryHistorical,
      zkQualifyDiscoveryNames,
      numSegmentsPerHistoricalQuery,
      useSmile,
      nonAggregateQueryHandling,
      queryGranularity,
      allowTopN,
      topNMaxThreshold,
      numProcessingThreadsPerHistorical
    )


    val drI = DruidMetadataCache.druidRelation(sqlContext,
      sourceDFName, sourceDF,
      dsName,
      timeDimensionCol,
      druidHost,
      columnMapping,
      columnInfos,
      fds,
      ss.right.get,
      options)

    logInfo(drI.fd.depGraph.debugString(drI.druidDS))

    val dQuery = parameters.get(DRUID_QUERY).map { s =>
      parse(s).extract[DruidQuery]
    }

    new DruidRelation(drI, dQuery)(sqlContext)
  }
}

object DefaultSource {

  val SOURCE_DF_PARAM = "sourceDataframe"

  /**
    * DataSource name in Druid.
    */
  val DRUID_DS_PARAM = "druidDatasource"

  val TIME_DIMENSION_COLUMN_PARAM = "timeDimensionColumn"

  /**
    * If the result cardinality of a Query exceeeds this value then Query is not
    * converted to a Druid Query.
    */
  val MAX_CARDINALITY_PARAM = "maxResultCardinality"
  val DEFAULT_MAX_CARDINALITY: String = (1 * 1000 * 1000).toString

  /**
    * If the result size estimate exceeds this number, and attempt is made to run 'n'
    * druid queries, each of which spans a sub interval of the total time interval.
    * 'n' is computed as `result.size % thisParam + 1`
    */
  val MAX_CARDINALITY_PER_DRUID_QUERY_PARAM = "maxCardinalityPerQuery"
  val DEFAULT_CARDINALITY_PER_DRUID_QUERY = (100 * 1000).toString

  /**
    * Map column names to Druid field names.
    * Specified as a json string.
    */
  val SOURCE_TO_DRUID_NAME_MAP_PARAM = "columnMapping"

  /**
    * List of [[DruidRelationColumnInfo]] that provide details about the source column
    * to Druid linkages.
    */
  val SOURCE_TO_DRUID_INFO = "columnInfos"

  /**
    * Specify how columns are related, see
    * [[org.sparklinedata.druid.metadata.FunctionalDependency]]. Specified as a list of
    * functional dependency objects.
    */
  val FUNCTIONAL_DEPENDENCIES_PARAM = "functionalDependencies"

  val DRUID_HOST_PARAM = "druidHost"
  val DEFAULT_DRUID_HOST = "localhost"

  // this is only for test purposes
  val DRUID_QUERY = "druidQuery"

  val STAR_SCHEMA_INFO_PARAM = "starSchema"

  /**
    * Controls whether Query results from Druid are streamed into
    * Spark Operator pipeline. Default is true.
    */
  val PUSH_HYPERLOGLOG_TODRUID = "pushHLLTODruid"
  val DEFAULT_PUSH_HYPERLOGLOG_TODRUID = "true"

  /**
    * Controls whether Query results from Druid are streamed into
    * Spark Operator pipeline. Default is true.
    */
  val STREAM_DRUID_QUERY_RESULTS = "streamDruidQueryResults"
  val DEFAULT_STREAM_DRUID_QUERY_RESULTS = "true"


  /**
    * When loading Druid DataSource metadata should the query interval be
    * the entire dataSource interval, or only the latest segment is enough.
    * Default is to load from all segments; since our query has
    * ("analysisTypes" -> []) the query is cheap.
    */
  val LOAD_METADATA_FROM_ALL_SEGMENTS = "loadMetadataFromAllSegments"
  val DEFAULT_LOAD_METADATA_FROM_ALL_SEGMENTS = "true"

  val ZK_SESSION_TIMEOUT = "zkSessionTimeoutMilliSecs"
  val DEFAULT_ZK_SESSION_TIMEOUT = "30000"

  val ZK_ENABLE_COMPRESSION = "zkEnableCompression"
  val DEFAULT_ZK_ENABLE_COMPRESSION = "true"

  val ZK_DRUID_PATH = "zkDruidPath"
  val DEFAULT_ZK_DRUID_PATH = "/druid"

  val QUERY_HISTORICAL = "queryHistoricalServers"
  val DEFAULT_QUERY_HISTORICAL = "false"

  val ZK_QUALIFY_DISCOVERY_NAMES = "zkQualifyDiscoveryNames"
  val DEFAULT_ZK_QUALIFY_DISCOVERY_NAMES = "false"

  val NUM_SEGMENTS_PER_HISTORICAL_QUERY = "numSegmentsPerHistoricalQuery"
  val DEFAULT_NUM_SEGMENTS_PER_HISTORICAL_QUERY = Int.MaxValue.toString

  val USE_SMILE = "useSmile"
  val DEFAULT_USE_SMILE = "true"

  val NUM_PROCESSING_THREADS_PER_HISTORICAL = "numProcessingThreadsPerHistorical"

  val NON_AGG_QUERY_HANDLING = "nonAggregateQueryHandling"
  val DEFAULT_NON_AGG_QUERY_HANDLING = NonAggregateQueryHandling.PUSH_NONE.toString

  val QUERY_GRANULARITY = "queryGranularity"
  val DEFAULT_QUERY_GRANULARITY = "none"

  val ALLOW_TOPN = "allowTopNRewrite"
  val DEFAULT_ALLOW_TOPN = false

  val TOPN_MAX_THRESHOLD = "topNMaxThreshold"
  val DEFAULT_TOPN_MAX_THRESHOLD : Int = 100000

}
