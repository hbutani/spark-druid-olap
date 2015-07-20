package org.apache.spark.sql.sources.druid

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.sparklinedata.druid.DruidQueryBuilder


class DruidPlanner private[druid](val sqlContext : SQLContext) extends DruidTransforms {

  sqlContext.experimental.extraStrategies =
    (new DruidStrategy(this) +: sqlContext.experimental.extraStrategies)

  val transforms : Seq[Function[LogicalPlan, Option[DruidQueryBuilder]]] = Seq(
    druidRelationTransform.lift
  )

  def plan(plan: LogicalPlan): Option[DruidQueryBuilder] = {
    val iter = transforms.view.flatMap(_(plan)).toIterator
    if (iter.isEmpty) None else Some(iter.next())
  }

}

object DruidPlanner {

  def apply(sqlContext : SQLContext) = new DruidPlanner(sqlContext)
}
