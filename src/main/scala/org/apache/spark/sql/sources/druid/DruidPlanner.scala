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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.sparklinedata.druid.DruidQueryBuilder


class DruidPlanner private[druid](val sqlContext : SQLContext) extends DruidTransforms {

  sqlContext.experimental.extraStrategies =
    (new DruidStrategy(this) +: sqlContext.experimental.extraStrategies)

  val joinGraphTransforms : Seq[DruidTransform] = Seq(
    druidRelationTransformForJoin or joinTransform
  )

  val transforms : Seq[DruidTransform] = Seq(
    druidRelationTransform or joinTransform,
    aggregateTransform,
    limitTransform
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
}
