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
import org.apache.spark.sql.execution.{SparkPlan, PhysicalRDD}
import org.apache.spark.sql.{CachedTablePattern, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.sparklinedata.druid._
import org.sparklinedata.druid.client.ConnectionManager

class DruidPlanner private[druid](val sqlContext : SQLContext) extends DruidTransforms {

  val cacheTablePatternMatch = new CachedTablePattern(sqlContext)

  sqlContext.experimental.extraStrategies =
    (new DruidStrategy(this) +: sqlContext.experimental.extraStrategies)

  val joinGraphTransforms : Seq[DruidTransform] = Seq(
    druidRelationTransformForJoin.debug("druidRelationTransformForJoin") or
      joinTransform.debug("join")
  )

  val transforms : Seq[DruidTransform] = Seq(
    druidRelationTransform.debug("druidRelationTransform") or
      joinTransform.debug("join"),
    aggregateTransform.debug("aggregate"),
    limitTransform.debug("limit")
  )

  def plan(db : Seq[DruidQueryBuilder], plan: LogicalPlan): Seq[DruidQueryBuilder] = {
    transforms.view.flatMap(_(db, plan))
  }

  private[druid] def joinPlan(db : Seq[DruidQueryBuilder], plan: LogicalPlan):
  Seq[DruidQueryBuilder] = {
    joinGraphTransforms.view.flatMap(_(db, plan))
  }

}

object DruidPlanner {

  def apply(sqlContext : SQLContext) : Unit = {
    new DruidPlanner(sqlContext)
    ConnectionManager.init(sqlContext)
  }

  val SPARKLINEDATA_CACHE_TABLES_TOCHECK = stringSeqConf("spark.sparklinedata.cache.tables.tocheck",
    defaultValue = Some(List()),
    doc = "A comma separated list of tableNames that should be checked if they are cached." +
      "For Star-Schemas with associated Druid Indexes, even if tables are cached, we " +
      "attempt to rewrite the Query to Druid. In order to do this we need to convert an" +
      "[[InMemoryRelation]] operator with its underlying Logical Plan. This value, will" +
      "tell us to restrict our check to certain tables. Otherwise by default we will check" +
      "all tables.  ")

  val DEBUG_TRANSFORMATIONS = booleanConf("spark.sparklinedata.debug.transformations",
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

  def getDruidQuerySpecs(plan : SparkPlan) : Seq[DruidQuery] = {
    plan.collect {
      case PhysicalRDD(_, r : DruidRDD, _, _, _) => r.dQuery
    }
  }

  def getConfValue[T](sqlContext : SQLContext,
                      entry : SQLConfEntry[T]) : T = {
    sqlContext.getConf(entry)
  }

}
