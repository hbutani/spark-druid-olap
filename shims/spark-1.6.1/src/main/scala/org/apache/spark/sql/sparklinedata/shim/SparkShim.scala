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

import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.catalyst.optimizer.{DefaultOptimizer, Optimizer}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

case class ExtendedLogicalOptimizer private[shim](conf : SQLConf,
                                 extraRules :
                                 Seq[(String, SparkShim.RuleStrategy, Rule[LogicalPlan])]
                                ) extends Optimizer {

  implicit def toStrategy(s : SparkShim.RuleStrategy) : Strategy = s match {
    case SparkShim.Once => Once
    case SparkShim.FixedPoint(m) => FixedPoint(m)
  }

  override protected val batches: Seq[Batch] =
    DefaultOptimizer.batches.asInstanceOf[Seq[Batch]] ++
      extraRules.map(b => Batch(b._1, b._2, b._3))
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

}
