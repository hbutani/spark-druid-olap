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

package org.sparklinedata.druid.query

import org.apache.spark.Logging
import org.sparklinedata.druid.{FunctionAggregationSpec, GroupByQuerySpec, QuerySpec}
import org.sparklinedata.druid.metadata.DruidRelationInfo


abstract class Transform extends Logging {

  val transformName: String = {
    val className = getClass.getName
    if (className endsWith "$") className.dropRight(1) else className
  }

  def apply(drInfo : DruidRelationInfo, qSpec: QuerySpec): QuerySpec
}

/**
  * Based on [[org.apache.spark.sql.catalyst.rules.RuleExecutor]]
  */
abstract class TransformExecutor extends Logging {

  abstract class Strategy { def maxIterations: Int }

  case object Once extends Strategy { val maxIterations = 1 }

  case class FixedPoint(maxIterations: Int) extends Strategy

  protected case class Batch(name: String, strategy: Strategy, transforms: Transform*)

  protected val batches: Seq[Batch]

  def transform(drInfo : DruidRelationInfo, qSpec: QuerySpec): QuerySpec = {
    var curQSpec = qSpec

    batches.foreach { batch =>
      val batchStartQSpec = curQSpec
      var iteration = 1
      var lastQSpec = curQSpec
      var continue = true

      // Run until fix point (or the max number of iterations as specified in the strategy.
      while (continue) {
        curQSpec = batch.transforms.foldLeft(curQSpec) {
          case (qSpec, transform) =>
            transform(drInfo, qSpec)
        }
        iteration += 1
        if (iteration > batch.strategy.maxIterations) {
          // Only log if this is a rule that is supposed to run more than once.
          if (iteration != 2) {
            logInfo(s"Max iterations (${iteration - 1}) reached for batch ${batch.name}")
          }
          continue = false
        }

        if (curQSpec == lastQSpec) {
          logTrace(
            s"Fixed point reached for batch ${batch.name} after ${iteration - 1} iterations.")
          continue = false
        }
        lastQSpec = curQSpec
      }

      if (batchStartQSpec != curQSpec) {
        logDebug(
          s"""
             |=== Result of Batch ${batch.name} ===
             |$curQSpec
        """.stripMargin)
      } else {
        logTrace(s"Batch ${batch.name} has no effect.")
      }
    }

    curQSpec
  }
}

object AddCountAggregateForNoMetricsGroupByQuerySpec extends Transform {

  override def apply(drInfo : DruidRelationInfo, qSpec: QuerySpec): QuerySpec =
    qSpec match {

    case gbSpec : GroupByQuerySpec if gbSpec.aggregations.isEmpty =>
      gbSpec.copy(aggregations =
      List(FunctionAggregationSpec("count", "addCountAggForNoMetricQuery", "count"))
      )
    case _ => qSpec
  }

}

object QuerySpecTransforms extends TransformExecutor {

  override  protected val batches: Seq[Batch] = Seq(Batch(
    "dimensionQueries", Once, AddCountAggregateForNoMetricsGroupByQuerySpec
  ))

}
