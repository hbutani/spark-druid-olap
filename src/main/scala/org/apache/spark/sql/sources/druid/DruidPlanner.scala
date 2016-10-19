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

import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.internal.SQLConf.SQLConfigBuilder
import org.apache.spark.sql.internal.SQLConf.SQLConfigBuilder._
import org.apache.spark.sql.execution.{RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.{CachedTablePattern, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.sparklinedata.druid._
import org.sparklinedata.druid.client.ConnectionManager

class DruidPlanner private[druid](val sqlContext : SQLContext) extends DruidTransforms {

  val cacheTablePatternMatch = new CachedTablePattern(sqlContext)

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

  def apply(sqlContext : SQLContext) : DruidPlanner = {
    val dP = new DruidPlanner(sqlContext)
    ConnectionManager.init(sqlContext)
    dP
  }

  val SPARKLINEDATA_CACHE_TABLES_TOCHECK = SQLConfigBuilder(
    "spark.sparklinedata.druid.cache.tables.tocheck").
    doc("A comma separated list of tableNames that should be checked if they are cached." +
      "For Star-Schemas with associated Druid Indexes, even if tables are cached, we " +
      "attempt to rewrite the Query to Druid. In order to do this we need to convert an" +
      "[[InMemoryRelation]] operator with its underlying Logical Plan. This value, will" +
      "tell us to restrict our check to certain tables. Otherwise by default we will check" +
      "all tables.  ").stringConf.toSequence.createWithDefault(List())

  val DEBUG_TRANSFORMATIONS = SQLConfigBuilder("spark.sparklinedata.druid.debug.transformations").
    doc("When set to true each transformation is logged.").
    booleanConf.createWithDefault(false)

  val  TZ_ID = SQLConfigBuilder("spark.sparklinedata.tz.id").
  doc("Specifes the TimeZone ID of the spark; " +
      "used by Druid for Date/TimeStamp transformations.").
    stringConf.createWithDefault(org.joda.time.DateTimeZone.getDefault.getID)

  val DRUID_SELECT_QUERY_PAGESIZE =
    SQLConfigBuilder("spark.sparklinedata.druid.selectquery.pagesize").
  doc("Num. of rows fetched on each invocation of Druid Select Query").
    intConf.createWithDefault(10000)

  val DRUID_CONN_POOL_MAX_CONNECTIONS =
    SQLConfigBuilder("spark.sparklinedata.druid.max.connections").
    doc("Max. number of Http Connections to Druid Cluster").intConf.createWithDefault(100)

  val DRUID_CONN_POOL_MAX_CONNECTIONS_PER_ROUTE = SQLConfigBuilder(
    "spark.sparklinedata.druid.max.connections.per.route").
    doc("Max. number of Http Connections to each server in Druid Cluster").intConf.
    createWithDefault(20)

  val DRUID_QUERY_COST_MODEL_ENABLED = SQLConfigBuilder(
    "spark.sparklinedata.druid.querycostmodel.enabled").
    doc("flag that controls if decision to execute druid query on broker or" +
      " historicals is based on the cost model; default is true. If false " +
      "the decision is based on the 'queryHistoricalServers' and " +
      "'numSegmentsPerHistoricalQuery' datasource options.").booleanConf.createWithDefault(true)

  val DRUID_QUERY_COST_MODEL_HIST_MERGE_COST = SQLConfigBuilder(
    "spark.sparklinedata.druid.querycostmodel.histMergeCostFactor").
    doc("cost of performing a segment agg. merge in druid " +
      "historicals relative to spark shuffle cost").doubleConf.createWithDefault(0.07)

  val DRUID_QUERY_COST_MODEL_HIST_SEGMENTS_PERQUERY_LIMIT = SQLConfigBuilder(
    "spark.sparklinedata.druid.querycostmodel.histSegsPerQueryLimit").
    doc("the max. number of segments processed per historical query.").
    intConf.createWithDefault(5)

  val DRUID_QUERY_COST_MODEL_INTERVAL_SCALING_NDV = SQLConfigBuilder(
    "spark.sparklinedata.druid.querycostmodel.queryintervalScalingForDistinctValues").
    doc("The ndv estimate for a query interval uses this number. The ratio of" +
      "the (query interval/index interval) is multiplied by this number. " +
      "The ndv for a query is estimated as:" +
      " 'log(this_value * min(10*interval_ratio,10)) * orig_ndv'. The reduction is logarithmic" +
      "and this value applies further dampening factor on the reduction. At a default value" +
      "of '3' any interval ratio >= 0.33 will have no reduction in ndvs. ").
    doubleConf.createWithDefault(3)

  val DRUID_QUERY_COST_MODEL_HISTORICAL_PROCESSING_COST = SQLConfigBuilder(
    "spark.sparklinedata.druid.querycostmodel.historicalProcessingCost").
    doc("the cost per row of groupBy processing in historical servers " +
      "relative to spark shuffle cost").doubleConf.createWithDefault(0.1)

  val DRUID_QUERY_COST_MODEL_HISTORICAL_TIMESERIES_PROCESSING_COST = SQLConfigBuilder(
    "spark.sparklinedata.druid.querycostmodel.historicalTimeSeriesProcessingCost").
    doc("the cost per row of timeseries processing in historical servers " +
      "relative to spark shuffle cost").doubleConf.createWithDefault(0.07)

  val DRUID_QUERY_COST_MODEL_SPARK_SCHEDULING_COST = SQLConfigBuilder(
    "spark.sparklinedata.druid.querycostmodel.sparkSchedulingCost").
    doc("the cost of scheduling tasks in spark relative to the shuffle cost of 1 row").
    doubleConf.createWithDefault(1.0)

  val DRUID_QUERY_COST_MODEL_SPARK_AGGREGATING_COST = SQLConfigBuilder(
    "spark.sparklinedata.druid.querycostmodel.sparkAggregatingCost").
    doc("the cost per row to do aggregation in spark relative to the shuffle cost").
    doubleConf.createWithDefault(0.15)

  val DRUID_QUERY_COST_MODEL_OUTPUT_TRANSPORT_COST = SQLConfigBuilder(
    "spark.sparklinedata.druid.querycostmodel.druidOutputTransportCost").
    doc("the cost per row to transport druid output relative to the shuffle cost").
    doubleConf.createWithDefault(0.4)

  val DRUID_USE_SMILE = SQLConfigBuilder(
    "spark.sparklinedata.druid.option.useSmile").
    doc("for druid queries use the smile binary protocol").
    booleanConf.createWithDefault(true)

  val DRUID_ALLOW_TOPN_QUERIES = SQLConfigBuilder(
    "spark.sparklinedata.druid.allowTopN").
    doc("druid TopN queries are approximate in their aggregation and ranking, this " +
      "flag controls if TopN query rewrites should happen.").
    booleanConf.createWithDefault(DefaultSource.DEFAULT_ALLOW_TOPN)

  val DRUID_TOPN_QUERIES_MAX_THRESHOLD = SQLConfigBuilder(
    "spark.sparklinedata.druid.topNMaxThreshold").
    doc("if druid TopN queries are enabled, this property controls the maximum " +
      "limit for which such rewrites are done. For limits beyond this value the GroupBy query is" +
      "executed.").intConf.createWithDefault(DefaultSource.DEFAULT_TOPN_MAX_THRESHOLD)

  val DRUID_USE_V2_GBY_ENGINE = SQLConfigBuilder(
    "spark.sparklinedata.druid.option.use.v2.groupByEngine").
    doc("for druid queries use the smile binary protocol").
    booleanConf.createWithDefault(false)

  val DRUID_RECORD_QUERY_EXECUTION = SQLConfigBuilder(
    "spark.sparklinedata.enable.druid.query.history").
    doc("track each druid query executed from every Spark Executor.").
    booleanConf.createWithDefault(false)

  def getDruidQuerySpecs(plan : SparkPlan) : Seq[DruidQuery] = {
    plan.collect {
      case RowDataSourceScanExec(_, r : DruidRDD, _, _, _, _) => r.dQuery
    }
  }

  def getDruidRDDs(plan : SparkPlan) : Seq[DruidRDD] = {
    plan.collect {
      case RowDataSourceScanExec(_, r : DruidRDD, _, _, _, _) => r
    }
  }

  def getConfValue[T](sqlContext : SQLContext,
                      entry : ConfigEntry[T]) : T = {
    sqlContext.conf.getConf(entry)
  }

}
