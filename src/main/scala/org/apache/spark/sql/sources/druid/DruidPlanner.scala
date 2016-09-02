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

package org.apache.spark.sql.sources.druid

import java.util.TimeZone

import org.apache.spark.sql.SQLConf.SQLConfEntry
import org.apache.spark.sql.SQLConf.SQLConfEntry._
import org.apache.spark.sql.execution.{PhysicalRDD, SparkPlan}
import org.apache.spark.sql.{CachedTablePattern, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.sparklinedata.druid._
import org.sparklinedata.druid.client.ConnectionManager
import org.sparklinedata.druid.metadata.DruidRelationInfo

class DruidPlanner private[druid](val sqlContext : SQLContext) extends DruidTransforms {

  val cacheTablePatternMatch = new CachedTablePattern(sqlContext)

  sqlContext.experimental.extraStrategies =
    (new DruidStrategy(this) +: sqlContext.experimental.extraStrategies)

  val joinGraphTransform =
    druidRelationTransformForJoin.debug("druidRelationTransformForJoin") or
      joinTransform.debug("join")

  val joinTransformWithDebug = joinTransform.debug("join")

  val transforms : Seq[DruidTransform] = Seq(
    druidRelationTransform.debug("druidRelationTransform") or
      joinTransformWithDebug,
    aggregateTransform.debug("aggregate"),
    limitTransform.debug("limit")
  )

  def plan(db : Seq[DruidQueryBuilder], plan: LogicalPlan): Seq[DruidQueryBuilder] = {
    transforms.view.flatMap(_(db, plan))
  }

}

object DruidPlanner {

  def apply(sqlContext : SQLContext) : Unit = {
    new DruidPlanner(sqlContext)
    ConnectionManager.init(sqlContext)
  }

  val SPARKLINEDATA_CACHE_TABLES_TOCHECK = stringSeqConf(
    "spark.sparklinedata.druid.cache.tables.tocheck",
    defaultValue = Some(List()),
    doc = "A comma separated list of tableNames that should be checked if they are cached." +
      "For Star-Schemas with associated Druid Indexes, even if tables are cached, we " +
      "attempt to rewrite the Query to Druid. In order to do this we need to convert an" +
      "[[InMemoryRelation]] operator with its underlying Logical Plan. This value, will" +
      "tell us to restrict our check to certain tables. Otherwise by default we will check" +
      "all tables.  ")

  val DEBUG_TRANSFORMATIONS = booleanConf("spark.sparklinedata.druid.debug.transformations",
    defaultValue = Some(false),
    doc = "When set to true each transformation is logged.")

  val  TZ_ID = stringConf("spark.sparklinedata.tz.id",
    defaultValue = Some(org.joda.time.DateTimeZone.getDefault.getID),
    doc = "Specifes the TimeZone ID of the spark; " +
      "used by Druid for Date/TimeStamp transformations.")

  val DRUID_SELECT_QUERY_PAGESIZE = intConf("spark.sparklinedata.druid.selectquery.pagesize",
    defaultValue = Some(10000),
    doc = "Num. of rows fetched on each invocation of Druid Select Query"
  )

  val DRUID_CONN_POOL_MAX_CONNECTIONS = intConf("spark.sparklinedata.druid.max.connections",
    defaultValue = Some(100),
    doc = "Max. number of Http Connections to Druid Cluster"
  )

  val DRUID_CONN_POOL_MAX_CONNECTIONS_PER_ROUTE = intConf(
    "spark.sparklinedata.druid.max.connections.per.route",
    defaultValue = Some(20),
    doc = "Max. number of Http Connections to each server in Druid Cluster"
  )

  val DRUID_QUERY_COST_MODEL_ENABLED = booleanConf(
    "spark.sparklinedata.druid.querycostmodel.enabled",
    defaultValue = Some(true),
    doc = "flag that controls if decision to execute druid query on broker or" +
      " historicals is based on the cost model; default is true. If false " +
      "the decision is based on the 'queryHistoricalServers' and " +
      "'numSegmentsPerHistoricalQuery' datasource options."
  )

  val DRUID_QUERY_COST_MODEL_HIST_MERGE_COST = doubleConf(
    "spark.sparklinedata.druid.querycostmodel.histMergeCostFactor",
    defaultValue = Some(0.07),
    doc = "cost of performing a segment agg. merge in druid " +
      "historicals relative to spark shuffle cost"
  )

  val DRUID_QUERY_COST_MODEL_HIST_SEGMENTS_PERQUERY_LIMIT = intConf(
    "spark.sparklinedata.druid.querycostmodel.histSegsPerQueryLimit",
    defaultValue = Some(5),
    doc = "the max. number of segments processed per historical query."
  )

  val DRUID_QUERY_COST_MODEL_INTERVAL_SCALING_NDV = doubleConf(
    "spark.sparklinedata.druid.querycostmodel.queryintervalScalingForDistinctValues",
    defaultValue = Some(3),
    doc = "The ndv estimate for a query interval uses this number. The ratio of" +
      "the (query interval/index interval) is multiplied by this number. " +
      "The ndv for a query is estimated as:" +
      " 'log(this_value * min(10*interval_ratio,10)) * orig_ndv'. The reduction is logarithmic" +
      "and this value applies further dampening factor on the reduction. At a default value" +
      "of '3' any interval ratio >= 0.33 will have no reduction in ndvs. "
  )

  val DRUID_QUERY_COST_MODEL_HISTORICAL_PROCESSING_COST = doubleConf(
    "spark.sparklinedata.druid.querycostmodel.historicalProcessingCost",
    defaultValue = Some(0.1),
    doc = "the cost per row of groupBy processing in historical servers " +
      "relative to spark shuffle cost"
  )

  val DRUID_QUERY_COST_MODEL_HISTORICAL_TIMESERIES_PROCESSING_COST = doubleConf(
    "spark.sparklinedata.druid.querycostmodel.historicalTimeSeriesProcessingCost",
    defaultValue = Some(0.07),
    doc = "the cost per row of timeseries processing in historical servers " +
      "relative to spark shuffle cost"
  )

  val DRUID_QUERY_COST_MODEL_SPARK_SCHEDULING_COST = doubleConf(
    "spark.sparklinedata.druid.querycostmodel.sparkSchedulingCost",
    defaultValue = Some(1.0),
    doc = "the cost of scheduling tasks in spark relative to the shuffle cost of 1 row"
  )

  val DRUID_QUERY_COST_MODEL_SPARK_AGGREGATING_COST = doubleConf(
    "spark.sparklinedata.druid.querycostmodel.sparkAggregatingCost",
    defaultValue = Some(0.15),
    doc = "the cost per row to do aggregation in spark relative to the shuffle cost"
  )

  val DRUID_QUERY_COST_MODEL_OUTPUT_TRANSPORT_COST = doubleConf(
    "spark.sparklinedata.druid.querycostmodel.druidOutputTransportCost",
    defaultValue = Some(0.4),
    doc = "the cost per row to transport druid output relative to the shuffle cost"
  )

  val DRUID_USE_SMILE = booleanConf(
    "spark.sparklinedata.druid.option.useSmile",
    defaultValue = Some(true),
    doc = "for druid queries use the smile binary protocol"
  )

  val DRUID_ALLOW_TOPN_QUERIES = booleanConf(
    "spark.sparklinedata.druid.allowTopN",
    defaultValue = Some(DefaultSource.DEFAULT_ALLOW_TOPN),
    doc = "druid TopN queries are approximate in their aggregation and ranking, this " +
      "flag controls if TopN query rewrites should happen."
  )

  val DRUID_TOPN_QUERIES_MAX_THRESHOLD = intConf(
    "spark.sparklinedata.druid.topNMaxThreshold",
    defaultValue = Some(DefaultSource.DEFAULT_TOPN_MAX_THRESHOLD),
    doc = "if druid TopN queries are enabled, this property controls the maximum " +
      "limit for which such rewrites are done. For limits beyond this value the GroupBy query is" +
      "executed."
  )

  def getDruidQuerySpecs(plan : SparkPlan) : Seq[DruidQuery] = {
    plan.collect {
      case PhysicalRDD(_, r : DruidRDD, _, _, _) => r.dQuery
    }
  }

  def getDruidRDDs(plan : SparkPlan) : Seq[DruidRDD] = {
    plan.collect {
      case PhysicalRDD(_, r : DruidRDD, _, _, _) => r
    }
  }

  def getConfValue[T](sqlContext : SQLContext,
                      entry : SQLConfEntry[T]) : T = {
    sqlContext.getConf(entry)
  }

}
