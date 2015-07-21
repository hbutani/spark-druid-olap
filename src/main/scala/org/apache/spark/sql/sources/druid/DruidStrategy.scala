package org.apache.spark.sql.sources.druid

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan


private[druid] class DruidStrategy(val planner : DruidPlanner) extends Strategy with PredicateHelper {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case l => {
      val qb = planner.plan(null, l)
      Nil
    }
  }
}
