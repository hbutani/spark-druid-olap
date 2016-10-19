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

package org.apache.spark.sql.sparklinedata.shim

import org.apache.hadoop.hive.ql.optimizer.spark.SparkJoinOptimizer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.execution.SparkOptimizer


trait ShimStrategyToRuleStrategy {
  self : RuleExecutor[LogicalPlan] =>

  implicit def toStrategy(s : SparkShim.RuleStrategy) : self.Strategy = s match {
    case SparkShim.Once => self.Once
    case SparkShim.FixedPoint(m) => self.FixedPoint(m)
  }

}

case class ExtendedLogicalOptimizer private[shim](conf : SQLConf,
                                 extraRules :
                                 Seq[(String, SparkShim.RuleStrategy, Rule[LogicalPlan])]
                                ) extends Optimizer(null, conf)
  with ShimStrategyToRuleStrategy {

  override val batches =
    new SparkOptimizer(null, conf, null).batches.asInstanceOf[List[Batch]] ++
      extraRules.map(b => Batch(b._1, b._2, b._3)).toList
}

case class
ParsedLogicalPlanTransform private[shim](conf : SQLConf,
                                        rules :
                                        Seq[(String, SparkShim.RuleStrategy, Rule[LogicalPlan])]
                                       )
  extends RuleExecutor[LogicalPlan] with ShimStrategyToRuleStrategy {

  override val batches =
    rules.map(b => Batch(b._1, b._2, b._3)).toList
}

object SparkShim {

  abstract class RuleStrategy { def maxIterations: Int }
  private[shim] case object Once extends RuleStrategy { val maxIterations = 1 }
  private[shim] case class FixedPoint(maxIterations: Int) extends RuleStrategy

  def once : RuleStrategy = Once
  def fixedPoint(maxIterations: Int) = FixedPoint(maxIterations)


  def extendedlogicalOptimizer(conf : SQLConf,
                       extraRules : Seq[(String, RuleStrategy, Rule[LogicalPlan])]
                      ) : Optimizer = {
    ExtendedLogicalOptimizer(conf, extraRules)
  }

  def parsedLogicalPlanTransform(conf : SQLConf,
                                 rules : Seq[(String, RuleStrategy, Rule[LogicalPlan])]
                              ) : RuleExecutor[LogicalPlan] = {
    ParsedLogicalPlanTransform(conf, rules)
  }

}
