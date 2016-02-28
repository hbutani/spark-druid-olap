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

import org.apache.spark.sql.SQLConf.SQLConfEntry._
import org.apache.spark.sql.{CachedTablePattern, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.sparklinedata.druid.DruidQueryBuilder


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

  def apply(sqlContext : SQLContext) = new DruidPlanner(sqlContext)

  val SPARKLINEDATA_CACHE_TABLES_TOCHECK = stringSeqConf("spark.sparklinedata.cache.tables.tocheck",
    defaultValue = None,
    doc = "A comma separated list of tableNames that should be checked if they are cached." +
      "For Star-Schemas with associated Druid Indexes, even if tables are cached, we " +
      "attempt to rewrite the Query to Druid. In order to do this we need to convert an" +
      "[[InMemoryRelation]] operator with its underlying Logical Plan. This value, will" +
      "tell us to restrict our check to certain tables. Otherwise by default we will check" +
      "all tables.  ")

  val DEBUG_TRANSFORMATIONS = booleanConf("spark.sparklinedata.debug.transformations",
    defaultValue = Some(false),
    doc = "When set to true each transformation is logged.")
}
